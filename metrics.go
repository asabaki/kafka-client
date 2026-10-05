package kafkaclient

import (
	"context"
	"errors"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/asabaki/kafka-client/internal/timeutil"
)

const (
	resultSuccess  = "success"
	resultError    = "error"
	resultCanceled = "canceled"
)

// producerMetrics records per-message producer metrics on the global meter provider.
type producerMetrics struct {
	kind     attribute.KeyValue // producer=sync|async
	messages metric.Int64Counter
	duration metric.Int64Histogram // sync only: publish call → acknowledgement
}

func newProducerMetrics(kind string) (*producerMetrics, error) {
	meter := otel.Meter(instrumentationName)

	messages, err := meter.Int64Counter("kafka_client_producer_messages",
		metric.WithDescription("messages handed to the producer, by outcome"),
		metric.WithUnit("{message}"))
	if err != nil {
		return nil, err
	}
	duration, err := meter.Int64Histogram("kafka_client_producer_publish_duration",
		metric.WithDescription("sync publish duration in milliseconds, until acknowledged or failed"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000))
	if err != nil {
		return nil, err
	}

	return &producerMetrics{kind: attribute.String("producer", kind), messages: messages, duration: duration}, nil
}

func (m *producerMetrics) start() time.Time {
	return timeutil.Now()
}

// published records a finished sync publish.
func (m *producerMetrics) published(ctx context.Context, start time.Time, topic string, err error) {
	attrs := metric.WithAttributes(m.kind, attribute.String("topic", topic), attribute.String("result", resultOf(err)))
	m.messages.Add(context.WithoutCancel(ctx), 1, attrs)
	m.duration.Record(context.WithoutCancel(ctx), timeutil.Since(start).Milliseconds(), attrs)
}

// delivered records an async delivery report (err nil = acknowledged).
func (m *producerMetrics) delivered(topic string, err error) {
	m.messages.Add(context.Background(), 1,
		metric.WithAttributes(m.kind, attribute.String("topic", topic), attribute.String("result", resultOf(err))))
}

func resultOf(err error) string {
	switch {
	case err == nil:
		return resultSuccess
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return resultCanceled
	default:
		return resultError
	}
}

// breakerMetrics records the active-partition partitioner's circuit breaker transitions.
type breakerMetrics struct {
	trips metric.Int64Counter
	open  metric.Int64UpDownCounter
	topic attribute.KeyValue
}

func newBreakerMetrics(topic string) *breakerMetrics {
	meter := otel.Meter(instrumentationName)
	// Instrument creation only fails for invalid names; fall back to no-op instruments rather than failing a send.
	trips, _ := meter.Int64Counter("kafka_client_partition_breaker_trips",
		metric.WithDescription("times a partition was taken out of rotation by the active-partition partitioner"),
		metric.WithUnit("{trip}"))
	open, _ := meter.Int64UpDownCounter("kafka_client_partition_breakers_open",
		metric.WithDescription("partitions currently out of rotation"),
		metric.WithUnit("{partition}"))

	return &breakerMetrics{trips: trips, open: open, topic: attribute.String("topic", topic)}
}

func (m *breakerMetrics) opened(partition int32, alreadyOpen bool) {
	attrs := metric.WithAttributes(m.topic, attribute.String("partition", strconv.Itoa(int(partition))))
	m.trips.Add(context.Background(), 1, attrs)
	if !alreadyOpen {
		m.open.Add(context.Background(), 1, attrs)
	}
}

func (m *breakerMetrics) closed(partition int32) {
	m.open.Add(context.Background(), -1,
		metric.WithAttributes(m.topic, attribute.String("partition", strconv.Itoa(int(partition)))))
}
