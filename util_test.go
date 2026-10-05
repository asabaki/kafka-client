package kafkaclient

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"

	"github.com/asabaki/kafka-client/internal/timeutil"
)

func TestToProducerMessage(t *testing.T) {
	tests := []struct {
		name string
		give Message
		want *sarama.ProducerMessage
	}{
		{
			name: "raw message",
			give: &rawMessage{
				topic:   "topic-a",
				key:     "msg-1",
				payload: []byte("value-1"),
			},
			want: &sarama.ProducerMessage{
				Topic: "topic-a",
				Key:   sarama.StringEncoder("msg-1"),
				Value: sarama.ByteEncoder([]byte("value-1")),
				Headers: []sarama.RecordHeader{{
					Key:   []byte(RecordHeaderKeyPartitionKey),
					Value: []byte("msg-1"),
				}},
			},
		},
		{
			name: "raw message without key",
			give: &rawMessage{
				topic:   "topic-b",
				payload: []byte("value-2"),
			},
			want: &sarama.ProducerMessage{
				Topic: "topic-b",
				Value: sarama.ByteEncoder([]byte("value-2")),
			},
		},
		{
			name: "custom message",
			give: &testCustomMessage{
				Key:   "msg-3",
				Value: "value-3",
			},
			want: &sarama.ProducerMessage{
				Topic: "test.custom.message",
				Key:   sarama.StringEncoder("msg-3"),
				Value: sarama.ByteEncoder([]byte("value-3")),
			},
		},
		{
			name: "custom message without key",
			give: &testCustomMessage{
				Value: "value-4",
			},
			want: &sarama.ProducerMessage{
				Topic: "test.custom.message",
				Value: sarama.ByteEncoder([]byte("value-4")),
			},
		},
		{
			name: "custom message with partition key",
			give: &testCustomMessage{
				Key:          "msg-5",
				Value:        "value-5",
				PartitionKey: "p-key-05",
			},
			want: &sarama.ProducerMessage{
				Topic: "test.custom.message",
				Key:   sarama.StringEncoder("msg-5"),
				Value: sarama.ByteEncoder([]byte("value-5")),
				Headers: []sarama.RecordHeader{{
					Key:   []byte(RecordHeaderKeyPartitionKey),
					Value: []byte("p-key-05"),
				}},
			},
		},
		{
			name: "custom message with headers and partition key",
			give: &testCustomMessage{
				Key:          "msg-6",
				Value:        "value-6",
				PartitionKey: "p-key-06",
				Headers: map[string]string{
					"h1": "h-value-1",
					"h2": "h-value-2",
				},
			},
			want: &sarama.ProducerMessage{
				Topic: "test.custom.message",
				Key:   sarama.StringEncoder("msg-6"),
				Value: sarama.ByteEncoder([]byte("value-6")),
				Headers: []sarama.RecordHeader{
					{
						Key:   []byte("h1"),
						Value: []byte("h-value-1"),
					},
					{
						Key:   []byte("h2"),
						Value: []byte("h-value-2"),
					},
					{
						Key:   []byte(RecordHeaderKeyPartitionKey),
						Value: []byte("p-key-06"),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := toProducerMessage(context.Background(), globalPropagator{}, tt.give)
			assert.NoError(t, err)
			assert.Equal(t, tt.want.Topic, out.Topic)
			assert.Equal(t, tt.want.Key, out.Key)
			assert.Equal(t, tt.want.Value, out.Value)
			assert.ElementsMatch(t, tt.want.Headers, out.Headers)
		})
	}
}

func TestToProducerMessageWithTimestamp(t *testing.T) {
	frozen := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	timeutil.Freeze(t, frozen)

	tests := []struct {
		name          string
		give          Message
		giveTimestamp time.Time
		want          *sarama.ProducerMessage
	}{
		{
			name: "zero timestamp",
			give: &rawMessage{
				topic:   "topic-a",
				key:     "msg-1",
				payload: []byte("value-1"),
			},
			giveTimestamp: time.Time{},
			want: &sarama.ProducerMessage{
				Topic: "topic-a",
				Key:   sarama.StringEncoder("msg-1"),
				Value: sarama.ByteEncoder("value-1"),
				Headers: []sarama.RecordHeader{{
					Key:   []byte(RecordHeaderKeyPartitionKey),
					Value: []byte("msg-1"),
				}},
				Timestamp: frozen,
			},
		},
		{
			name: "non-zero timestamp",
			give: &rawMessage{
				topic:   "topic-a",
				key:     "msg-1",
				payload: []byte("value-1"),
			},
			giveTimestamp: frozen.Add(time.Hour),
			want: &sarama.ProducerMessage{
				Topic: "topic-a",
				Key:   sarama.StringEncoder("msg-1"),
				Value: sarama.ByteEncoder("value-1"),
				Headers: []sarama.RecordHeader{{
					Key:   []byte(RecordHeaderKeyPartitionKey),
					Value: []byte("msg-1"),
				}},
				Timestamp: frozen.Add(time.Hour),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := toProducerMessage(context.Background(), globalPropagator{}, tt.give, WithTimestamp(tt.giveTimestamp))
			assert.NoError(t, err)
			assert.Equal(t, tt.want.Topic, out.Topic)
			assert.Equal(t, tt.want.Key, out.Key)
			assert.Equal(t, tt.want.Value, out.Value)
			assert.ElementsMatch(t, tt.want.Headers, out.Headers)
			assert.Equal(t, tt.want.Timestamp, out.Timestamp)
		})
	}
}

type testCustomMessage struct {
	Key          string
	Value        string
	PartitionKey string
	Headers      map[string]string
}

func (u *testCustomMessage) KafkaTopic() string {
	return "test.custom.message"
}

func (u *testCustomMessage) KafkaMessageKey() string {
	return u.Key
}

func (u *testCustomMessage) KafkaMessagePayload() ([]byte, error) {
	return []byte(u.Value), nil
}

func (u *testCustomMessage) KafkaPartitionKey() string {
	return u.PartitionKey
}

func (u *testCustomMessage) KafkaMessageHeaders() map[string]string {
	return u.Headers
}

func Test_convertBytesToString(t *testing.T) {
	type args struct {
		input []byte
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "normal case",
			args: args{
				input: []byte("hello world"),
			},
			want: "hello world",
		},
		{
			name: "nil",
			args: args{
				input: nil,
			},
			want: "",
		},
		{
			name: "empty",
			args: args{
				input: []byte(""),
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, convertBytesToString(tt.args.input), "convertBytesToString(%v)", tt.args.input)
		})
	}
}
