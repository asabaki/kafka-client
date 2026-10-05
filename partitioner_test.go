package kafkaclient

import (
	"fmt"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
)

func TestJumpHash(t *testing.T) {
	t.SkipNow()
	for bucket := int32(1); bucket <= 10; bucket++ {
		fmt.Printf("Bucket: %d\n", bucket)
		for i := uint64(0); i < 80; i++ {
			fmt.Printf("%2d|", jumpHash(i, bucket))
		}
		fmt.Println()
	}
}

func TestConsistentHashPartitioner_Partition_ByMessageKey(t *testing.T) {
	partitioner := NewConsistentHashPartition("topic")
	var numPartition int32 = 999

	msgA1 := &sarama.ProducerMessage{
		Key: sarama.StringEncoder("aaaaa"),
	}
	msgA2 := &sarama.ProducerMessage{
		Key: sarama.StringEncoder("aaaaa"),
	}
	msgB := &sarama.ProducerMessage{
		Key: sarama.StringEncoder("bbbbb"),
	}

	partA1, _ := partitioner.Partition(msgA1, numPartition)
	partA2, _ := partitioner.Partition(msgA2, numPartition)
	partB, _ := partitioner.Partition(msgB, numPartition)

	assert.Equal(t, partA1, partA2)
	assert.NotEqual(t, partA1, partB)
}

func TestConsistentHashPartitioner_Partition_ByPartitionKey(t *testing.T) {
	partitioner := NewConsistentHashPartition("topic")
	var numPartition int32 = 999

	msgA1 := &sarama.ProducerMessage{
		Key: sarama.StringEncoder("11111"),
		Headers: []sarama.RecordHeader{{
			Key:   []byte(RecordHeaderKeyPartitionKey),
			Value: []byte("aaaaa"),
		}},
	}
	msgA2 := &sarama.ProducerMessage{
		Key: sarama.StringEncoder("22222"),
		Headers: []sarama.RecordHeader{{
			Key:   []byte(RecordHeaderKeyPartitionKey),
			Value: []byte("aaaaa"),
		}},
	}
	msgB := &sarama.ProducerMessage{
		Key: sarama.StringEncoder("33333"),
		Headers: []sarama.RecordHeader{{
			Key:   []byte(RecordHeaderKeyPartitionKey),
			Value: []byte("bbbbb"),
		}},
	}

	partA1, _ := partitioner.Partition(msgA1, numPartition)
	partA2, _ := partitioner.Partition(msgA2, numPartition)
	partB, _ := partitioner.Partition(msgB, numPartition)

	assert.Equal(t, partA1, partA2)
	assert.NotEqual(t, partA1, partB)
}
