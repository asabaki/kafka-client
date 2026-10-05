package kafkaclient_test

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kafkaclient "github.com/asabaki/kafka-client"
)

// mockBrokerCompatible disables the ApiVersions handshake, which sarama's MockBroker doesn't answer
// (every connection would otherwise wait out the 60s dial timeout).
var mockBrokerCompatible = kafkaclient.WithSaramaConfigHookForTest(func(c *sarama.Config) {
	c.ApiVersionsRequest = false
	c.Version = sarama.V2_8_0_0
})

// TestActivePartitionPartitioner_ReroutesAroundFailingPartition runs the real sync producer against sarama's mock
// broker, where one partition of four keeps failing (like a partition waiting for a new leader).
func TestActivePartitionPartitioner_ReroutesAroundFailingPartition(t *testing.T) {
	const topic = "orders"

	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()

	// find the partition "user-1" hashes to, so exactly that one fails
	home, err := kafkaclient.NewConsistentHashPartition(topic).Partition(&sarama.ProducerMessage{Key: sarama.StringEncoder("user-1")}, 4)
	require.NoError(t, err)

	metadata := sarama.NewMockMetadataResponse(t).SetBroker(broker.Addr(), broker.BrokerID())
	for p := int32(0); p < 4; p++ {
		metadata.SetLeader(topic, p, broker.BrokerID())
	}
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": metadata,
		"ProduceRequest":  sarama.NewMockProduceResponse(t).SetError(topic, home, sarama.ErrNotEnoughReplicas),
	})

	cfg := kafkaclient.KafkaConfig{
		BootstrapServers:     []string{broker.Addr()},
		ProducerRequiredAcks: -1,
		ProducerRetryMax:     0,
		ProducerTimeout:      time.Second,
		MetricsDisabled:      true,
	}
	producer, err := kafkaclient.NewSyncProducer(cfg, mockBrokerCompatible,
		kafkaclient.WithPartitioner(kafkaclient.NewActivePartitionPartitioner(kafkaclient.ActivePartitionConfig{
			MaxFailures: 2, OpenDuration: time.Minute,
		})),
	)
	require.NoError(t, err)
	defer producer.Close()

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		partition, _, err := producer.PublishRawAtLeastOnce(ctx, topic, "user-1", []byte("v"), nil)
		require.ErrorIs(t, err, sarama.ErrNotEnoughReplicas)
		assert.Equal(t, int32(-1), partition)
	}

	moved, _, err := producer.PublishRawAtLeastOnce(ctx, topic, "user-1", []byte("v"), nil)
	require.NoError(t, err, "breaker open after 2 failures: the next message goes elsewhere and succeeds")
	assert.NotEqual(t, home, moved)

	for i := 0; i < 20; i++ {
		key := string(rune('a' + i))
		partition, _, err := producer.PublishRawAtLeastOnce(ctx, topic, key, []byte("v"), nil)
		require.NoError(t, err)
		assert.NotEqual(t, home, partition, "no key is sent to the open partition")
	}
}

// TestDefaultPartitioner_KeepsKeyOnFailingPartition documents the default: keyed messages never move.
func TestDefaultPartitioner_KeepsKeyOnFailingPartition(t *testing.T) {
	const topic = "orders"

	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()

	home, err := kafkaclient.NewConsistentHashPartition(topic).Partition(&sarama.ProducerMessage{Key: sarama.StringEncoder("user-1")}, 4)
	require.NoError(t, err)

	metadata := sarama.NewMockMetadataResponse(t).SetBroker(broker.Addr(), broker.BrokerID())
	for p := int32(0); p < 4; p++ {
		metadata.SetLeader(topic, p, broker.BrokerID())
	}
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": metadata,
		"ProduceRequest":  sarama.NewMockProduceResponse(t).SetError(topic, home, sarama.ErrNotEnoughReplicas),
	})

	producer, err := kafkaclient.NewSyncProducer(kafkaclient.KafkaConfig{
		BootstrapServers:     []string{broker.Addr()},
		ProducerRequiredAcks: -1,
		ProducerTimeout:      time.Second,
		MetricsDisabled:      true,
	}, mockBrokerCompatible)
	require.NoError(t, err)
	defer producer.Close()

	for i := 0; i < 5; i++ {
		_, _, err := producer.PublishRawAtLeastOnce(context.Background(), topic, "user-1", []byte("v"), nil)
		require.ErrorIs(t, err, sarama.ErrNotEnoughReplicas)
	}
}
