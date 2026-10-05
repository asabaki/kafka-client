package kafkagoso_test

import (
	"context"
	"testing"

	"github.com/justtrackio/gosoline/pkg/tracing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"

	"github.com/asabaki/kafka-client/kafkagoso"
)

func TestGosolinePropagator_RoundTrip(t *testing.T) {
	in := &tracing.Trace{TraceId: "1-5f84c7a1-abcdef012345678901234567", Id: "53995c3f42cd8ad8", Sampled: true}
	ctx := tracing.ContextWithTrace(context.Background(), in)

	carrier := propagation.MapCarrier{}
	kafkagoso.GosolinePropagator{}.Inject(ctx, carrier)
	assert.Equal(t, "Root=1-5f84c7a1-abcdef012345678901234567;Parent=53995c3f42cd8ad8;Sampled=1", carrier["traceId"])

	// same attribute gosoline's stream encoder writes, so it must decode it identically
	_, attrs, err := tracing.NewMessageWithTraceEncoder(nil).Decode(context.Background(), nil, map[string]string{"traceId": carrier["traceId"]})
	require.NoError(t, err)
	assert.Empty(t, attrs)

	out := tracing.GetTraceFromContext(kafkagoso.GosolinePropagator{}.Extract(context.Background(), carrier))
	require.NotNil(t, out)
	assert.Equal(t, in.TraceId, out.TraceId)
	assert.Equal(t, in.Id, out.ParentId, "the producer's span becomes the consumer's parent")
	assert.True(t, out.Sampled)
}

func TestGosolinePropagator_NoTraceOrInvalidHeader(t *testing.T) {
	carrier := propagation.MapCarrier{}
	kafkagoso.GosolinePropagator{}.Inject(context.Background(), carrier)
	assert.Empty(t, carrier, "nothing is written without a trace in ctx")

	ctx := kafkagoso.GosolinePropagator{}.Extract(context.Background(), propagation.MapCarrier{"traceId": "garbage"})
	assert.Nil(t, tracing.GetTraceFromContext(ctx))
}
