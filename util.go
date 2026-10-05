package kafkaclient

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel/propagation"

	"github.com/asabaki/kafka-client/internal/timeutil"
)

var (
	RecordHeaderKeyPartitionKey = "partition_key"
)

type MessageOption struct {
	timestamp time.Time
}

type Option func(*MessageOption)

func WithTimestamp(timestamp time.Time) Option {
	return func(o *MessageOption) {
		o.timestamp = timestamp
	}
}

func mapToRecordHeader(headers map[string]string) []sarama.RecordHeader {
	if headers == nil {
		return nil
	}
	if len(headers) == 0 {
		return []sarama.RecordHeader{}
	}

	recordHeaders := make([]sarama.RecordHeader, 0, len(headers))
	for key, val := range headers {
		recordHeaders = append(recordHeaders, sarama.RecordHeader{
			Key:   []byte(key),
			Value: []byte(val),
		})
	}
	return recordHeaders
}

func toProducerMessage(ctx context.Context, propagator propagation.TextMapPropagator, msg Message, opts ...Option) (*sarama.ProducerMessage, error) {
	var messageOptions MessageOption
	for _, opt := range opts {
		opt(&messageOptions)
	}

	payload, err := msg.KafkaMessagePayload()
	if err != nil {
		return nil, err
	}

	timestamp := func() time.Time {
		if messageOptions.timestamp.IsZero() {
			return timeutil.Now()
		}
		return messageOptions.timestamp
	}()

	pMsg := &sarama.ProducerMessage{
		Topic:     msg.KafkaTopic(),
		Key:       messageKey(msg),
		Value:     sarama.ByteEncoder(payload),
		Metadata:  nil,
		Timestamp: timestamp,
	}

	if headers := messageHeaders(msg); len(headers) > 0 {
		pMsg.Headers = headers
	}

	propagator.Inject(ctx, producerMessageCarrier{msg: pMsg})

	return pMsg, nil
}

func messageKey(msg Message) sarama.Encoder {
	key := msg.KafkaMessageKey()
	if key == "" {
		return nil
	}
	return sarama.StringEncoder(key)
}

func receiveUntilClose[T any](ch <-chan T, process func(T)) {
	if process == nil {
		process = func(T) {}
	}

	for x := range ch {
		process(x)
	}
}

func messageHeaders(msg Message) []sarama.RecordHeader {
	var headers map[string]string
	if msgWH, ok := msg.(MessageWithHeader); ok {
		headers = msgWH.KafkaMessageHeaders()
	}

	// Attach the partition key of a message to headers if it does not exist.
	if pKey := partitionKey(msg); pKey != "" {
		if headers == nil {
			headers = make(map[string]string)
		}

		if _, exists := headers[RecordHeaderKeyPartitionKey]; !exists {
			headers[RecordHeaderKeyPartitionKey] = pKey
		}
	}

	return mapToRecordHeader(headers)
}

func partitionKey(msg Message) string {
	var key string
	if v, ok := msg.(MessageWithPartitionKey); ok {
		key = v.KafkaPartitionKey()
	}
	return key
}

func convertBytesToString(input []byte) string {
	if input == nil {
		return ""
	}

	return string(input)
}
