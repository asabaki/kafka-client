package kafkagoso_test

import (
	"testing"
	"time"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/asabaki/kafka-client/kafkagoso"
)

func producerConfig(producer map[string]any) cfg.Config {
	return cfg.New(map[string]any{
		"kafka_client": map[string]any{
			"producer": map[string]any{"p": producer},
		},
	})
}

func TestReadProducerSettings_Defaults(t *testing.T) {
	s, err := kafkagoso.ReadProducerSettings(producerConfig(map[string]any{}), "p")
	require.NoError(t, err)

	assert.Equal(t, "default", s.Connection)
	assert.Equal(t, kafkagoso.PartitionerConsistentHash, s.Partitioner)
	assert.Equal(t, 3, s.CircuitBreaker.MaxFailures)
	assert.Equal(t, 30*time.Second, s.CircuitBreaker.OpenDuration)
}

func TestReadProducerSettings_ActivePartition(t *testing.T) {
	s, err := kafkagoso.ReadProducerSettings(producerConfig(map[string]any{
		"partitioner": "active_partition",
		"circuit_breaker": map[string]any{
			"max_failures":  5,
			"open_duration": "10s",
		},
	}), "p")
	require.NoError(t, err)

	assert.Equal(t, kafkagoso.PartitionerActivePartition, s.Partitioner)
	assert.Equal(t, 5, s.CircuitBreaker.MaxFailures)
	assert.Equal(t, 10*time.Second, s.CircuitBreaker.OpenDuration)
}

func TestReadProducerSettings_Invalid(t *testing.T) {
	for name, producer := range map[string]map[string]any{
		"unknown partitioner": {"partitioner": "round_robin"},
		"zero max failures": {
			"partitioner":     "active_partition",
			"circuit_breaker": map[string]any{"max_failures": 0},
		},
		"zero open duration": {
			"partitioner":     "active_partition",
			"circuit_breaker": map[string]any{"open_duration": "0s"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := kafkagoso.ReadProducerSettings(producerConfig(producer), "p")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "kafka_client.producer.p.")
		})
	}
}

func consumerConfig(consumer map[string]any) cfg.Config {
	return cfg.New(map[string]any{
		"kafka_client": map[string]any{
			"consumer": map[string]any{"c": consumer},
		},
	})
}

func TestReadConsumerSettings_WorkerCount(t *testing.T) {
	s, err := kafkagoso.ReadConsumerSettings(consumerConfig(map[string]any{"topic": "t", "group_id": "g"}), "c")
	require.NoError(t, err)
	assert.Equal(t, 1, s.WorkerCount)

	s, err = kafkagoso.ReadConsumerSettings(consumerConfig(map[string]any{"topic": "t", "group_id": "g", "worker_count": 4}), "c")
	require.NoError(t, err)
	assert.Equal(t, 4, s.WorkerCount)

	_, err = kafkagoso.ReadConsumerSettings(consumerConfig(map[string]any{"topic": "t", "group_id": "g", "worker_count": 0}), "c")
	assert.ErrorContains(t, err, "worker_count must be >= 1")

	_, err = kafkagoso.ReadConsumerSettings(consumerConfig(map[string]any{"topic": "t", "group_id": "g", "worker_count": 4, "batch_size": 10}), "c")
	assert.ErrorContains(t, err, "not supported for batch consumers")
}
