package kafkaclient

import (
	"context"

	"github.com/IBM/sarama"
)

type Message interface {
	KafkaTopic() string
	// KafkaMessageKey is a Kafka message key. By default, this is used to determine Kafka partition.
	// An empty message key will cause a message to publish into a random partition.
	// If you want to specify partition key that differ from the message key, you should implement MessageWithPartitionKey interface.
	KafkaMessageKey() string
	KafkaMessagePayload() ([]byte, error)
}

type MessageWithHeader interface {
	KafkaMessageHeaders() map[string]string
}

func WrapWithSaramaMessageHandler[T any](handler MessageHandler[T], converter ConsumerMessageConverter[T]) MessageHandler[*sarama.ConsumerMessage] {
	return func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		converted, err := converter.Convert(msg)
		if err != nil {
			return err
		}
		return handler(ctx, converted)
	}
}

type MessageWithPartitionKey interface {
	// KafkaPartitionKey is Kafka partition key. An empty partition key will cause a message to publish into a random partition.
	KafkaPartitionKey() string
}

// ConsumerGroup is an abstraction of kafka consumer group.
type ConsumerGroup interface {
	// Start starts the consumer in another goroutine. This returns error if the consumer is running.
	Start() error
	// Close releases the used resources.
	Close()
	// IsRunning returns true if the consumer is running.
	IsRunning() bool
	// Health returns error if consumer is not running. This is usually used in health check REST endpoint.
	Health() error
	// Run starts the consumer and blocks until ctx is canceled or the consumer stops on its own, then closes it.
	// It has the signature of a gosoline kernel.Module.
	Run(ctx context.Context) error
}

// MessageHandler handles a single message. ctx carries the trace context propagated in the message headers.
type MessageHandler[T any] func(ctx context.Context, msg T) error

type ConsumerMessageConverter[T any] interface {
	Convert(*sarama.ConsumerMessage) (T, error)
}

type ConsumerMessageConverterFunc[T any] func(*sarama.ConsumerMessage) (T, error)

func (c ConsumerMessageConverterFunc[T]) Convert(message *sarama.ConsumerMessage) (T, error) {
	return c(message)
}

type rawMessage struct {
	topic   string
	key     string
	payload []byte
	header  map[string]string
}

func (r *rawMessage) KafkaMessageHeaders() map[string]string {
	return r.header
}

func (r *rawMessage) KafkaTopic() string {
	return r.topic
}

func (r *rawMessage) KafkaMessageKey() string {
	return r.key
}

func (r *rawMessage) KafkaMessagePayload() ([]byte, error) {
	return r.payload, nil
}

func (r *rawMessage) KafkaPartitionKey() string {
	return r.key
}

// MessagesHandler processes a batch of messages
type MessagesHandler[T any] func(messages []*MessageWithContext[T]) error

type MessageWithContext[T any] struct {
	msg T
	ctx context.Context
}

// NewMessageWithContext pairs a message with its context (e.g. to re-wrap messages in a handler middleware).
func NewMessageWithContext[T any](ctx context.Context, msg T) *MessageWithContext[T] {
	return &MessageWithContext[T]{ctx: ctx, msg: msg}
}

func (m *MessageWithContext[T]) Get() (context.Context, T) {
	return m.ctx, m.msg
}

func WrapWithSaramaMessagesHandler[T any](handler MessagesHandler[T], converter ConsumerMessageConverter[T]) MessagesHandler[*sarama.ConsumerMessage] {
	return func(messages []*MessageWithContext[*sarama.ConsumerMessage]) error {
		convertedMessages := make([]*MessageWithContext[T], len(messages))
		for i, m := range messages {
			converted, err := converter.Convert(m.msg)
			if err != nil {
				return err
			}
			convertedMessages[i] = &MessageWithContext[T]{ctx: m.ctx, msg: converted}
		}
		return handler(convertedMessages)
	}
}
