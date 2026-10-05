package kafkagoso

import (
	"context"

	"github.com/justtrackio/gosoline/pkg/tracing"
	"go.opentelemetry.io/otel/propagation"
)

// TraceIdHeader is the message header gosoline uses to carry a trace across services
// (the same message attribute gosoline's stream encoder uses).
const TraceIdHeader = "traceId"

var _ propagation.TextMapPropagator = GosolinePropagator{}

// GosolinePropagator carries gosoline's trace (tracing.Trace) in the "traceId" message header, formatted as
// "Root=<trace-id>;Parent=<span-id>;Sampled=<0|1>".
//
//   - Inject: writes the trace of the current gosoline span (or the trace stored in ctx) into the header.
//     Nothing is written if ctx carries no gosoline trace.
//   - Extract: parses the header and stores the trace in ctx (tracing.ContextWithTrace). A missing or malformed
//     header leaves ctx unchanged.
//
// With the trace in ctx, gosoline's logger adds a trace_id field to every log call made with that ctx, and
// tracer.StartSpanFromContext continues the same trace.
type GosolinePropagator struct{}

func (GosolinePropagator) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	if traceId := tracing.GetTraceIdFromContext(ctx); traceId != nil {
		carrier.Set(TraceIdHeader, *traceId)
	}
}

func (GosolinePropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	value := carrier.Get(TraceIdHeader)
	if value == "" {
		return ctx
	}

	trace, err := tracing.StringToTrace(value)
	if err != nil || trace == nil {
		return ctx
	}

	return tracing.ContextWithTrace(ctx, trace)
}

func (GosolinePropagator) Fields() []string {
	return []string{TraceIdHeader}
}
