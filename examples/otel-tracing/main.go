// otel-tracing sets up the OpenTelemetry SDK and shows a trace flowing producer → Kafka → consumer.
// kafka-client writes the W3C traceparent header on publish and extracts it into the handler's ctx; it creates no
// spans itself, so the example starts one on each side.
//
//	go run ./otel-tracing
//
// Spans are printed to stdout; swap stdouttrace for an OTLP exporter in a real service.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	kafkaclient "github.com/asabaki/kafka-client"
	"github.com/asabaki/kafka-client/examples/internal/env"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint(), stdouttrace.WithWriter(os.Stdout))
	if err != nil {
		return err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer tp.Shutdown(context.Background())

	// kafka-client's default propagator delegates to these globals.
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tracer := otel.Tracer("otel-example")

	cfg := env.KafkaConfig()
	const topic = "otel-demo"

	producer, err := kafkaclient.NewSyncProducer(cfg)
	if err != nil {
		return err
	}
	defer producer.Close()

	// Producer side: the span in ctx is written to the message's traceparent header.
	ctx, span := tracer.Start(context.Background(), "publish order", trace.WithSpanKind(trace.SpanKindProducer))
	_, _, err = producer.PublishRawAtLeastOnce(ctx, topic, "order-1", []byte(`{"id":"order-1"}`), nil)
	span.End()
	if err != nil {
		return err
	}
	log.Printf("published with trace %s", span.SpanContext().TraceID())

	// Consumer side: ctx already carries the producer's span context; child spans join the same trace.
	done := make(chan struct{})
	consumer, err := kafkaclient.NewConsumerGroup(cfg, "otel-demo", topic, false,
		func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			_, span := tracer.Start(ctx, "handle order", trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(attribute.Int64("kafka.offset", msg.Offset)))
			defer span.End()

			log.Printf("consumed with trace %s", span.SpanContext().TraceID())
			close(done)
			return nil
		},
	)
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			log.Print("timed out waiting for the message")
		}
		cancel()
	}()
	return consumer.Run(runCtx)
}
