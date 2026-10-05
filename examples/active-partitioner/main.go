// active-partitioner keeps publishing while some partitions are unavailable (e.g. brokers on spot instances that
// were reclaimed and whose partitions wait for a new leader).
//
// With the default partitioner, a key always goes to its partition: while that partition is down, every message
// for the key fails. With the active-partition partitioner, a partition that fails MaxFailures times in a row is
// taken out of rotation for OpenDuration, and its keys are hashed onto the remaining partitions.
//
// Only use it for topics whose consumers don't need per-key ordering: while a partition is out of rotation, one
// key's messages end up on two partitions.
//
//	go run ./active-partitioner
//
// Stop a broker of a multi-broker cluster while it runs to see the rerouting in the log.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"

	kafkaclient "github.com/asabaki/kafka-client"
	"github.com/asabaki/kafka-client/examples/internal/env"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg := env.KafkaConfig()
	// Fail fast so the partitioner learns quickly; sarama retries the same partition, rerouting only affects the
	// messages sent after the circuit breaker opened.
	cfg.ProducerRetryMax = 1
	cfg.ProducerTimeout = 2 * time.Second

	if err := ensureTopic(cfg, "clicks", 6); err != nil {
		return err
	}

	producer, err := kafkaclient.NewSyncProducer(cfg,
		kafkaclient.WithPartitioner(kafkaclient.NewActivePartitionPartitioner(kafkaclient.ActivePartitionConfig{
			MaxFailures:  3,                // consecutive failures that take a partition out of rotation
			OpenDuration: 30 * time.Second, // how long before it gets traffic again
		})),
	)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer producer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		key := fmt.Sprintf("user-%d", i%10)
		partition, _, err := producer.PublishRawAtLeastOnce(ctx, "clicks", key, []byte(`{"x":1}`), nil)
		if err != nil {
			// This message is not moved: store it elsewhere (e.g. a fallback queue) if it must not be lost.
			log.Printf("%s failed: %s", key, err)
			continue
		}
		log.Printf("%s -> partition %d", key, partition)
	}
}

// ensureTopic creates the topic with several partitions, so there is somewhere to reroute to.
func ensureTopic(cfg kafkaclient.KafkaConfig, topic string, partitions int32) error {
	admin, err := sarama.NewClusterAdmin(cfg.BootstrapServers, cfg.ToSaramaConfig())
	if err != nil {
		return fmt.Errorf("create admin: %w", err)
	}
	defer admin.Close()

	err = admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: partitions, ReplicationFactor: -1}, false)
	if err != nil && !errors.Is(err, sarama.ErrTopicAlreadyExists) {
		return fmt.Errorf("create topic: %w", err)
	}
	return nil
}
