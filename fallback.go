package kafkaclient

import (
	"context"
	"fmt"
	"strconv"

	"github.com/IBM/sarama"
)

// FallbackFunc parks a message that its handler could not process, e.g. by writing it to a dead-letter topic or a
// queue. It returns nil once the message is stored safely; an error means the message must be redelivered.
type FallbackFunc func(ctx context.Context, msg *sarama.ConsumerMessage, handlerErr error) error

// WrapWithFallbackHandler calls fallback when handler fails, then treats the message as consumed, so one failing
// message doesn't block its partition. If fallback also fails, both errors are returned and the message is
// redelivered: it is never marked as consumed without being stored somewhere.
//
// Combine it with retries by wrapping a retry handler that returns its final error:
//
//	kafkaclient.WrapWithFallbackHandler(
//		kafkaclient.WrapWithRetryBackoffHandler(handler, kafkaclient.RetryConfig{MaxRetries: 3, ReturnError: true, ...}),
//		kafkaclient.DeadLetterTopic(producer, "orders.dead-letter"),
//	)
func WrapWithFallbackHandler(handler MessageHandler[*sarama.ConsumerMessage], fallback FallbackFunc) MessageHandler[*sarama.ConsumerMessage] {
	return func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		err := handler(ctx, msg)
		if err == nil {
			return nil
		}
		if fbErr := fallback(ctx, msg, err); fbErr != nil {
			return fmt.Errorf("handler failed: %w; fallback failed: %w", err, fbErr)
		}
		return nil
	}
}

// WrapWithFallbackBatchHandler is WrapWithFallbackHandler for batch consumers: when the batch handler fails,
// every message of the batch goes to fallback.
func WrapWithFallbackBatchHandler(handler MessagesHandler[*sarama.ConsumerMessage], fallback FallbackFunc) MessagesHandler[*sarama.ConsumerMessage] {
	return func(messages []*MessageWithContext[*sarama.ConsumerMessage]) error {
		err := handler(messages)
		if err == nil {
			return nil
		}
		for _, m := range messages {
			ctx, msg := m.Get()
			if fbErr := fallback(ctx, msg, err); fbErr != nil {
				return fmt.Errorf("batch handler failed: %w; fallback failed at offset %d: %w", err, msg.Offset, fbErr)
			}
		}
		return nil
	}
}

// Headers DeadLetterTopic adds to a dead-lettered message, next to the original headers.
const (
	HeaderDeadLetterError     = "dead-letter-error"
	HeaderDeadLetterTopic     = "dead-letter-source-topic"
	HeaderDeadLetterPartition = "dead-letter-source-partition"
	HeaderDeadLetterOffset    = "dead-letter-source-offset"
)

// DeadLetterTopic returns a FallbackFunc that publishes the failed message, unchanged (key, value, headers), to
// topic, adding the error and the original topic, partition and offset as headers.
func DeadLetterTopic(producer SyncProducer, topic string) FallbackFunc {
	return func(ctx context.Context, msg *sarama.ConsumerMessage, handlerErr error) error {
		headers := make(map[string]string, len(msg.Headers)+4)
		for _, h := range msg.Headers {
			if h != nil {
				headers[string(h.Key)] = string(h.Value)
			}
		}
		headers[HeaderDeadLetterError] = handlerErr.Error()
		headers[HeaderDeadLetterTopic] = msg.Topic
		headers[HeaderDeadLetterPartition] = strconv.Itoa(int(msg.Partition))
		headers[HeaderDeadLetterOffset] = strconv.FormatInt(msg.Offset, 10)

		// WithoutCancel: a shutdown mid-handler must not lose the message between the failure and its dead-letter copy.
		_, _, err := producer.PublishRawAtLeastOnce(context.WithoutCancel(ctx), topic, string(msg.Key), msg.Value, headers)
		return err
	}
}
