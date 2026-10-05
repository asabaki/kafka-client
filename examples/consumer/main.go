// consumer reads a topic in a consumer group until Ctrl-C, then shuts down gracefully:
// the message in progress finishes, offsets are committed and the group is left.
//
//	go run ./consumer
//
// Run it twice with the same group to see the partitions being split between both instances.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"

	kafkaclient "github.com/asabaki/kafka-client"
	"github.com/asabaki/kafka-client/examples/internal/env"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	log.Print("consumer stopped cleanly")
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	consumer, err := kafkaclient.NewConsumerGroup(env.KafkaConfig(), "orders-printer", "orders",
		false, // no committed offset yet: start from the oldest message
		func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			log.Printf("partition %d offset %d key %s: %s", msg.Partition, msg.Offset, msg.Key, msg.Value)
			return nil // nil = mark as consumed; an error ends the session and redelivers the message
		},
	)
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}

	// Run blocks until ctx is done (Ctrl-C) or the consumer gives up reconnecting, then closes it.
	if err := consumer.Run(ctx); err != nil {
		return fmt.Errorf("consumer stopped: %w", err)
	}
	return nil
}
