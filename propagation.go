package kafkaclient

import (
	"context"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/asabaki/kafka-client/internal/carrier"
)

// globalPropagator resolves otel's global propagator on every call, so a propagator registered after the
// client was created (e.g. by an OpenTelemetry SDK setup) is still used.
type globalPropagator struct{}

func (globalPropagator) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

func (globalPropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

func (globalPropagator) Fields() []string {
	return otel.GetTextMapPropagator().Fields()
}

func injectTraceContext(ctx context.Context, propagator propagation.TextMapPropagator, msg *sarama.ProducerMessage) {
	propagator.Inject(ctx, carrier.Producer{Msg: msg})
}

func extractTraceContext(ctx context.Context, propagator propagation.TextMapPropagator, msg *sarama.ConsumerMessage) context.Context {
	return propagator.Extract(ctx, carrier.Consumer{Msg: msg})
}

// OTelGlobalPropagator returns a propagator that delegates to OpenTelemetry's global propagator at call time.
// This is the default used when WithTracePropagator is not set.
func OTelGlobalPropagator() propagation.TextMapPropagator {
	return globalPropagator{}
}
