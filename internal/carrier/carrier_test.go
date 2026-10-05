package carrier

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducerSetOverwritesExistingHeader(t *testing.T) {
	msg := &sarama.ProducerMessage{Headers: []sarama.RecordHeader{{Key: []byte("traceparent"), Value: []byte("old")}}}
	c := Producer{Msg: msg}

	c.Set("traceparent", "new")

	require.Len(t, msg.Headers, 1)
	assert.Equal(t, "new", c.Get("traceparent"))
	assert.Equal(t, []string{"traceparent"}, c.Keys())
}

func TestConsumerSkipsNilHeaders(t *testing.T) {
	c := Consumer{Msg: &sarama.ConsumerMessage{Headers: []*sarama.RecordHeader{nil, {Key: []byte("a"), Value: []byte("b")}}}}

	assert.Equal(t, "b", c.Get("a"))
	assert.Equal(t, []string{"a"}, c.Keys())
	c.Set("a", "x")
	assert.Equal(t, "b", c.Get("a"), "consumer carrier is read-only")
}
