package kafkaclient

import (
	"context"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

var (
	_ propagation.TextMapCarrier = producerMessageCarrier{}
	_ propagation.TextMapCarrier = consumerMessageCarrier{}
)

// producerMessageCarrier exposes producer message headers as an OpenTelemetry TextMapCarrier.
type producerMessageCarrier struct {
	msg *sarama.ProducerMessage
}

func (c producerMessageCarrier) Get(key string) string {
	for _, h := range c.msg.Headers {
		if string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c producerMessageCarrier) Set(key, val string) {
	for i := range c.msg.Headers {
		if string(c.msg.Headers[i].Key) == key {
			c.msg.Headers[i].Value = []byte(val)
			return
		}
	}
	c.msg.Headers = append(c.msg.Headers, sarama.RecordHeader{Key: []byte(key), Value: []byte(val)})
}

func (c producerMessageCarrier) Keys() []string {
	out := make([]string, 0, len(c.msg.Headers))
	for _, h := range c.msg.Headers {
		out = append(out, string(h.Key))
	}
	return out
}

// consumerMessageCarrier exposes consumer message headers as an OpenTelemetry TextMapCarrier (read-only).
type consumerMessageCarrier struct {
	msg *sarama.ConsumerMessage
}

func (c consumerMessageCarrier) Get(key string) string {
	for _, h := range c.msg.Headers {
		if h != nil && string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c consumerMessageCarrier) Set(string, string) {}

func (c consumerMessageCarrier) Keys() []string {
	out := make([]string, 0, len(c.msg.Headers))
	for _, h := range c.msg.Headers {
		if h != nil {
			out = append(out, string(h.Key))
		}
	}
	return out
}

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

func extractTraceContext(ctx context.Context, propagator propagation.TextMapPropagator, msg *sarama.ConsumerMessage) context.Context {
	return propagator.Extract(ctx, consumerMessageCarrier{msg: msg})
}

// OTelGlobalPropagator returns a propagator that delegates to OpenTelemetry's global propagator at call time.
// This is the default used when WithTracePropagator is not set.
func OTelGlobalPropagator() propagation.TextMapPropagator {
	return globalPropagator{}
}
