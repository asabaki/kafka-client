package kafkaclient_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kafkaclient "github.com/asabaki/kafka-client"
)

// recordingPartitioner wraps the default partitioner and records the send results producers report back.
type recordingPartitioner struct {
	sarama.Partitioner

	mu        sync.Mutex
	successes []int32
	failures  []error
}

func (r *recordingPartitioner) OnSuccess(msg *sarama.ProducerMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.successes = append(r.successes, msg.Partition)
}

func (r *recordingPartitioner) OnError(_ *sarama.ProducerMessage, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, err)
}

func (r *recordingPartitioner) counts() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.successes), len(r.failures)
}

func recordingConstructor() (sarama.PartitionerConstructor, func(topic string) *recordingPartitioner) {
	var mu sync.Mutex
	byTopic := map[string]*recordingPartitioner{}

	constructor := func(topic string) sarama.Partitioner {
		p := &recordingPartitioner{Partitioner: kafkaclient.NewConsistentHashPartition(topic)}
		mu.Lock()
		byTopic[topic] = p
		mu.Unlock()
		return p
	}
	get := func(topic string) *recordingPartitioner {
		mu.Lock()
		defer mu.Unlock()
		return byTopic[topic]
	}

	return constructor, get
}

func TestPartitionFeedback_SyncProducer(t *testing.T) {
	requireKafka(t)

	admin := createClusterAdmin(t, getKafkaBootstrapServers())
	topic := createTestTopic(t, admin, "feedback-sync")

	constructor, get := recordingConstructor()
	cfg := makeKafkaConfigForTest()
	producer, err := kafkaclient.NewSyncProducer(cfg, kafkaclient.WithPartitioner(constructor))
	require.NoError(t, err)
	defer producer.Close()

	partition, _, err := producer.PublishRawAtLeastOnce(context.Background(), topic, "k1", []byte("v"), nil)
	require.NoError(t, err)

	// a message bigger than the broker's max.message.bytes (1MB default) is rejected by the broker
	_, _, err = producer.PublishRawAtLeastOnce(context.Background(), topic, "k2", make([]byte, 2<<20), nil)
	require.Error(t, err)

	rec := get(topic)
	require.NotNil(t, rec)
	ok, failed := rec.counts()
	assert.Equal(t, 1, ok)
	assert.Equal(t, 1, failed)
	assert.Equal(t, []int32{partition}, rec.successes)
}

func TestPartitionFeedback_AsyncProducer(t *testing.T) {
	requireKafka(t)

	admin := createClusterAdmin(t, getKafkaBootstrapServers())
	topic := createTestTopic(t, admin, "feedback-async")

	constructor, get := recordingConstructor()
	var handlerErrors int
	producer, err := kafkaclient.NewAsyncProducer(makeKafkaConfigForTest(),
		kafkaclient.WithPartitioner(constructor),
		kafkaclient.WithAsyncErrorHandler(func(*sarama.ProducerError) { handlerErrors++ }),
	)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		producer.PublishRawAtMostOnce(context.Background(), topic, "k", []byte("v"), nil)
	}
	producer.PublishRawAtMostOnce(context.Background(), topic, "big", make([]byte, 2<<20), nil)
	require.NoError(t, producer.Close())

	rec := get(topic)
	require.NotNil(t, rec)
	ok, failed := rec.counts()
	assert.Equal(t, 10, ok)
	assert.Equal(t, 1, failed)
	assert.Equal(t, 1, handlerErrors, "the user's error handler still runs")
}

func TestActivePartitionPartitioner_ProducesToRealBroker(t *testing.T) {
	requireKafka(t)

	admin := createClusterAdmin(t, getKafkaBootstrapServers())
	topic := createTestTopicWithPartitions(t, admin, "active-partition", 4)

	producer, err := kafkaclient.NewSyncProducer(makeKafkaConfigForTest(),
		kafkaclient.WithPartitioner(kafkaclient.NewActivePartitionPartitioner(kafkaclient.ActivePartitionConfig{
			MaxFailures: 1, OpenDuration: time.Second,
		})),
	)
	require.NoError(t, err)
	defer producer.Close()

	first, _, err := producer.PublishRawAtLeastOnce(context.Background(), topic, "same-key", []byte("v"), nil)
	require.NoError(t, err)

	// rejected client-side before partitioning: must not open any breaker even with MaxFailures 1
	for i := 0; i < 3; i++ {
		_, _, err = producer.PublishRawAtLeastOnce(context.Background(), topic, "same-key", make([]byte, 2<<20), nil)
		require.Error(t, err)
	}

	for i := 0; i < 5; i++ {
		partition, _, err := producer.PublishRawAtLeastOnce(context.Background(), topic, "same-key", []byte("v"), nil)
		require.NoError(t, err)
		assert.Equal(t, first, partition, "same key, healthy partitions: same partition")
	}
}
