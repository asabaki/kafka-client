package kafkaclient

import (
	"context"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel/propagation"
)

type AsyncProducer interface {
	// PublishRawAtMostOnce queues a message; see PublishAtMostOnce.
	PublishRawAtMostOnce(ctx context.Context, topic string, messageKey string, payload []byte, headers map[string]string)
	// PublishAtMostOnce queues msg and returns. It blocks only while sarama's input queue is full, and at most until
	// ctx ends. Every message ends in exactly one delivery report: the success handler, or the error handler (also
	// for messages that could not be encoded or queued before ctx ended).
	PublishAtMostOnce(ctx context.Context, msg Message, opts ...Option)
	// Close flushes buffered messages, waits for all delivery reports to be handled and releases resources.
	Close() error
	Unwrap() sarama.Client
}

type asyncProducer struct {
	producer   sarama.AsyncProducer
	client     sarama.Client
	propagator propagation.TextMapPropagator
	logger     Logger
	onSuccess  func(*sarama.ProducerMessage)
	onError    func(*sarama.ProducerError)
	metrics    *producerMetrics
	wg         sync.WaitGroup
}

// PublishRawAtMostOnce publishes a message with at-most-once guarantee.
// messageKey is used to determine the Kafka partition; an empty key selects a random partition.
//
// If ctx contains a span, its trace context is propagated in the message headers.
func (p *asyncProducer) PublishRawAtMostOnce(ctx context.Context, topic string, messageKey string, payload []byte, headers map[string]string) {
	p.PublishAtMostOnce(ctx, &rawMessage{
		topic:   topic,
		key:     messageKey,
		payload: payload,
		header:  headers,
	})
}

// PublishAtMostOnce publishes a message with at-most-once guarantee.
//
// If ctx contains a span, its trace context is propagated in the message headers.
func (p *asyncProducer) PublishAtMostOnce(ctx context.Context, msg Message, opts ...Option) {
	produceMsg, err := toProducerMessage(ctx, p.propagator, msg, opts...)
	if err != nil {
		p.reportLocal(&sarama.ProducerMessage{Topic: msg.KafkaTopic()}, fmt.Errorf("can not build producer message: %w", err))
		return
	}
	if err := ctx.Err(); err != nil {
		p.reportLocal(produceMsg, err)
		return
	}

	select {
	case p.producer.Input() <- produceMsg:
	case <-ctx.Done():
		p.reportLocal(produceMsg, ctx.Err())
	}
}

// reportLocal reports a message that never reached sarama, through the same path as delivery failures.
func (p *asyncProducer) reportLocal(msg *sarama.ProducerMessage, err error) {
	p.metrics.delivered(msg.Topic, err)
	p.onError(&sarama.ProducerError{Msg: msg, Err: err})
}

func (p *asyncProducer) Close() error {
	// AsyncClose lets the handler goroutines drain the Successes/Errors channels until sarama closes them.
	p.producer.AsyncClose()
	p.wg.Wait()
	return p.client.Close()
}

func (p *asyncProducer) Unwrap() sarama.Client {
	return p.client
}

func (p *asyncProducer) logProducerError(pe *sarama.ProducerError) {
	if keyStr, ok := pe.Msg.Key.(sarama.StringEncoder); ok {
		p.logger.Error(context.Background(), "produce message error: topic=%s key=%s: %s", pe.Msg.Topic, string(keyStr), pe.Err)
		return
	}
	p.logger.Error(context.Background(), "produce message error: topic=%s: %s", pe.Msg.Topic, pe.Err)
}
