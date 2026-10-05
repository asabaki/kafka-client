package kafkaclient_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kafkaclient "github.com/asabaki/kafka-client"
)

func TestConsumerWorkers_RealBroker(t *testing.T) {
	requireKafka(t)

	admin := createClusterAdmin(t, getKafkaBootstrapServers())
	topic := createTestTopicWithPartitions(t, admin, "workers", 2)
	producer := createProducerForTest(t, getKafkaBootstrapServers())

	const total = 300
	for i := 0; i < total; i++ {
		_, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("user-%d", i%15)),
			Value: sarama.StringEncoder(fmt.Sprint(i)),
		})
		require.NoError(t, err)
	}

	var mu sync.Mutex
	perKey := map[string][]int64{}
	var handled, running, maxRunning atomic.Int32
	done := make(chan struct{})

	consumer, err := kafkaclient.NewConsumerGroup(makeKafkaConfigForTest(), topic+"-g", topic, false,
		func(_ context.Context, msg *sarama.ConsumerMessage) error {
			now := running.Add(1)
			for prev := maxRunning.Load(); now > prev && !maxRunning.CompareAndSwap(prev, now); prev = maxRunning.Load() {
			}
			time.Sleep(3 * time.Millisecond)
			running.Add(-1)

			mu.Lock()
			key := fmt.Sprintf("%d/%s", msg.Partition, msg.Key)
			perKey[key] = append(perKey[key], msg.Offset)
			mu.Unlock()

			if handled.Add(1) == total {
				close(done)
			}
			return nil
		},
		kafkaclient.WithConsumerWorkers(4),
	)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(ctx) }()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatalf("handled only %d of %d", handled.Load(), total)
	}
	cancel()
	require.NoError(t, <-runErr)

	for key, offsets := range perKey {
		assert.IsIncreasing(t, offsets, "key %s out of order", key)
	}
	assert.Greater(t, maxRunning.Load(), int32(2), "more than one handler per partition ran at once")

	// every offset was committed: a new member of the group has nothing left to read
	offsets, err := admin.ListConsumerGroupOffsets(topic+"-g", map[string][]int32{topic: {0, 1}})
	require.NoError(t, err)
	var committed int64
	for _, block := range offsets.Blocks[topic] {
		committed += block.Offset
	}
	assert.Equal(t, int64(total), committed)
}
