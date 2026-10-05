package kafkaclient

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/asabaki/kafka-client/internal/carrier"
)

func TestTraceContextPropagatesThroughHeaders(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	traceID, _ := trace.TraceIDFromHex("0af7651916cd43dd8448eb211c80319c")
	spanID, _ := trace.SpanIDFromHex("b7ad6b7169203331")
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	pMsg, err := toProducerMessage(ctx, globalPropagator{}, &rawMessage{topic: "t", key: "k", payload: []byte("v"), header: map[string]string{"a": "b"}})
	require.NoError(t, err)

	// what the broker hands back to a consumer
	cMsg := &sarama.ConsumerMessage{}
	for i := range pMsg.Headers {
		cMsg.Headers = append(cMsg.Headers, &pMsg.Headers[i])
	}

	assert.Equal(t, "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", carrier.Consumer{Msg: cMsg}.Get("traceparent"))
	assert.Equal(t, "b", carrier.Consumer{Msg: cMsg}.Get("a"), "user headers must be kept")

	got := trace.SpanContextFromContext(extractTraceContext(context.Background(), globalPropagator{}, cMsg))
	assert.Equal(t, traceID, got.TraceID())
	assert.Equal(t, spanID, got.SpanID())
	assert.True(t, got.IsRemote())
}
