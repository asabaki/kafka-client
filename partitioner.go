package kafkaclient

import (
	"github.com/IBM/sarama"
	"github.com/cespare/xxhash/v2"
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
	return jumpHash(xxhash.Sum64(keyBytes), numPartitions), nil
}

// jumpHash implements consistent hashing from this paper: http://arxiv.org/abs/1406.2294
// code is copied from https://github.com/dgryski/go-jump/blob/master/jump.go
func jumpHash(key uint64, numBuckets int32) int32 {
	var b int64 = -1
	var j int64

	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	// b < numBuckets (an int32), so the conversion cannot overflow.
	return int32(b) // #nosec G115
}
