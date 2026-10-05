package kafkaclient

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel/propagation"
)

type SyncProducer interface {
	// PublishRawAtLeastOnce publishes a message and waits for the broker's acknowledgement.
	// See PublishAtLeastOnce for the ctx semantics.
	PublishRawAtLeastOnce(ctx context.Context, topic string, messageKey string, payload []byte, headers map[string]string) (partition int32, offset int64, err error)
	// PublishAtLeastOnce publishes msg and waits for the broker's acknowledgement.
	//
	// It returns early with ctx.Err() when ctx is canceled or its deadline passes. The message may still be
	// delivered afterwards: it can't be recalled from sarama's pipeline. Treat that error as "outcome unknown";
	// retrying can create a duplicate.
	PublishAtLeastOnce(ctx context.Context, msg Message, opts ...Option) (partition int32, offset int64, err error)
	// Close stops accepting messages, waits for in-flight ones and releases the connection.
	Close() error
	Unwrap() sarama.Client
}

// syncProducer is built on sarama's AsyncProducer, so a publish can stop waiting when its ctx ends
// (sarama's SyncProducer has no context).
type syncProducer struct {
	producer   sarama.AsyncProducer
	client     sarama.Client
	propagator propagation.TextMapPropagator
	feedback   *feedbackPartitioners
	metrics    *producerMetrics

	// heartbeat is set when ProducerHeartbeatEnabled is true.
	heartbeat *producerHeartbeat

	wg sync.WaitGroup
}

// syncResult is attached to each message (ProducerMessage.Metadata) and receives exactly one result.
type syncResult chan error

func newSyncProducer(producer sarama.AsyncProducer, client sarama.Client, cfg KafkaConfig, feedback *feedbackPartitioners, metrics *producerMetrics) *syncProducer {
	p := &syncProducer{
		producer:   producer,
		client:     client,
		propagator: cfg.tracePropagator(),
		feedback:   feedback,
		metrics:    metrics,
	}

	p.wg.Add(2)
	go func() {
		defer p.wg.Done()
		for msg := range producer.Successes() {
			p.feedback.success(msg)
			deliver(msg, nil)
		}
	}()
	go func() {
		defer p.wg.Done()
		for pe := range producer.Errors() {
			p.feedback.failure(pe.Msg, pe.Err)
			deliver(pe.Msg, pe.Err)
		}
	}()

	return p
}

func deliver(msg *sarama.ProducerMessage, err error) {
	if result, ok := msg.Metadata.(syncResult); ok {
		result <- err // buffered: never blocks, even if the publisher stopped waiting
	}
}

func (p *syncProducer) PublishRawAtLeastOnce(ctx context.Context, topic string, messageKey string, payload []byte, headers map[string]string) (partition int32, offset int64, err error) {
	return p.PublishAtLeastOnce(ctx, &rawMessage{
		topic:   topic,
		key:     messageKey,
		payload: payload,
		header:  headers,
	})
}

func (p *syncProducer) PublishAtLeastOnce(ctx context.Context, msg Message, opts ...Option) (partition int32, offset int64, err error) {
	start := p.metrics.start()
	topic := msg.KafkaTopic()
	defer func() { p.metrics.published(ctx, start, topic, err) }()

	if err := ctx.Err(); err != nil {
		return -1, -1, err
	}

	produceMsg, err := toProducerMessage(ctx, p.propagator, msg, opts...)
	if err != nil {
		return -1, -1, err
	}
	result := make(syncResult, 1)
	produceMsg.Metadata = result

	select {
	case p.producer.Input() <- produceMsg:
	case <-ctx.Done():
		return -1, -1, ctx.Err() // never entered the pipeline: definitely not sent
	}

	select {
	case err := <-result:
		if err != nil {
			return -1, -1, err
		}
		return produceMsg.Partition, produceMsg.Offset, nil
	case <-ctx.Done():
		return -1, -1, ctx.Err()
	}
}

func (p *syncProducer) Close() error {
	if p.heartbeat != nil {
		p.heartbeat.stop()
	}
	// AsyncClose flushes what is in flight; the goroutines above deliver every remaining result until sarama closes
	// the channels.
	p.producer.AsyncClose()
	p.wg.Wait()

	// sarama does not close a client passed to NewAsyncProducerFromClient.
	return p.client.Close()
}

func (p *syncProducer) Unwrap() sarama.Client {
	return p.client
}
