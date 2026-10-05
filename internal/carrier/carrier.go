// Package carrier exposes sarama message headers as OpenTelemetry TextMapCarriers.
package carrier

import (
	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel/propagation"
)

var (
	_ propagation.TextMapCarrier = Producer{}
	_ propagation.TextMapCarrier = Consumer{}
)

// Producer reads and writes the headers of a producer message.
type Producer struct {
	Msg *sarama.ProducerMessage
}

func (c Producer) Get(key string) string {
	for _, h := range c.Msg.Headers {
		if string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c Producer) Set(key, val string) {
	for i := range c.Msg.Headers {
		if string(c.Msg.Headers[i].Key) == key {
			c.Msg.Headers[i].Value = []byte(val)
			return
		}
	}
	c.Msg.Headers = append(c.Msg.Headers, sarama.RecordHeader{Key: []byte(key), Value: []byte(val)})
}

func (c Producer) Keys() []string {
	out := make([]string, 0, len(c.Msg.Headers))
	for _, h := range c.Msg.Headers {
		out = append(out, string(h.Key))
	}
	return out
}

// Consumer reads the headers of a consumer message. Set is a no-op.
type Consumer struct {
	Msg *sarama.ConsumerMessage
}

func (c Consumer) Get(key string) string {
	for _, h := range c.Msg.Headers {
		if h != nil && string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c Consumer) Set(string, string) {}

func (c Consumer) Keys() []string {
	out := make([]string, 0, len(c.Msg.Headers))
	for _, h := range c.Msg.Headers {
		if h != nil {
			out = append(out, string(h.Key))
		}
	}
	return out
}
