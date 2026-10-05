package kafkaclient

import (
	"github.com/IBM/sarama"

	"github.com/asabaki/kafka-client/internal/hash"
)

var _ sarama.DynamicConsistencyPartitioner = (*ConsistentHashPartitioner)(nil)

// NewConsistentHashPartition is the default partitioner. See ConsistentHashPartitioner.
func NewConsistentHashPartition(topic string) sarama.Partitioner {
	return &ConsistentHashPartitioner{
		random: sarama.NewRandomPartitioner(topic),
	}
}

// ConsistentHashPartitioner hashes the partition key (header "partition_key", set from MessageWithPartitionKey) or,
// without one, the message key, using xxhash + jump consistent hash. Messages with neither go to a random partition.
type ConsistentHashPartitioner struct {
	random sarama.Partitioner
}

func (c *ConsistentHashPartitioner) RequiresConsistency() bool {
	return true
}

// MessageRequiresConsistency lets sarama pick among partitions that currently have a leader for messages without a
// key: they have no key → partition mapping to keep, so they avoid offline partitions. Keyed messages always see
// every partition, so the same key keeps landing on the same partition.
func (c *ConsistentHashPartitioner) MessageRequiresConsistency(message *sarama.ProducerMessage) bool {
	return c.getKey(message) != nil
}

func (c *ConsistentHashPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	key := c.getKey(message)
	if key == nil {
		return c.random.Partition(message, numPartitions)
	}
	return hashKey(key, numPartitions)
}

// getKey returns the key used for partitioning: the partition key header if present, else the message key.
func (c *ConsistentHashPartitioner) getKey(message *sarama.ProducerMessage) sarama.Encoder {
	return partitioningKey(message)
}

func partitioningKey(message *sarama.ProducerMessage) sarama.Encoder {
	for _, h := range message.Headers {
		if string(h.Key) == RecordHeaderKeyPartitionKey {
			return sarama.ByteEncoder(h.Value)
		}
	}

	return message.Key
}

func hashKey(key sarama.Encoder, numPartitions int32) (int32, error) {
	keyBytes, err := key.Encode()
	if err != nil {
		return -1, err
	}
	return hash.Partition(keyBytes, numPartitions), nil
}
