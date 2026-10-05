package kafkaclient_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kafkaclient "github.com/asabaki/kafka-client"
)

func TestDeadLetterTopic_RealBroker(t *testing.T) {
	requireKafka(t)

	admin := createClusterAdmin(t, getKafkaBootstrapServers())
	dlq := createTestTopic(t, admin, "dlq")

	producer, err := kafkaclient.NewSyncProducer(makeKafkaConfigForTest())
	require.NoError(t, err)
	defer producer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // a shutdown mid-handler must still get the message parked

	err = kafkaclient.DeadLetterTopic(producer, dlq)(ctx, &sarama.ConsumerMessage{
		Topic: "orders", Partition: 3, Offset: 42, Key: []byte("k"), Value: []byte("v"),
		Headers: []*sarama.RecordHeader{{Key: []byte("traceId"), Value: []byte("t-1")}},
	}, errors.New("boom"))
	require.NoError(t, err)

	consumer, err := sarama.NewConsumer(getKafkaBootstrapServers(), sarama.NewConfig())
	require.NoError(t, err)
	defer consumer.Close()
	pc, err := consumer.ConsumePartition(dlq, 0, sarama.OffsetOldest)
	require.NoError(t, err)
	defer pc.Close()

	select {
	case msg := <-pc.Messages():
		assert.Equal(t, "k", string(msg.Key))
		assert.Equal(t, "v", string(msg.Value))
		headers := map[string]string{}
		for _, h := range msg.Headers {
			headers[string(h.Key)] = string(h.Value)
		}
		assert.Equal(t, "t-1", headers["traceId"], "original headers are kept")
		assert.Equal(t, "boom", headers[kafkaclient.HeaderDeadLetterError])
		assert.Equal(t, "orders", headers[kafkaclient.HeaderDeadLetterTopic])
		assert.Equal(t, "3", headers[kafkaclient.HeaderDeadLetterPartition])
		assert.Equal(t, "42", headers[kafkaclient.HeaderDeadLetterOffset])
	case <-time.After(15 * time.Second):
		t.Fatal("dead-lettered message not found")
	}
}
