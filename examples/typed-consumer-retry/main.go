// typed-consumer-retry decodes messages into a Go type, retries temporary failures with backoff, and parks what
// still fails on a dead-letter topic, so one bad message can't block its partition.
//
//	go run ./typed-consumer-retry
package main

import (
	"context"
	"encoding/json"
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

type OrderCreated struct {
	ID       string `json:"id"`
	Customer string `json:"customer"`
	Amount   int    `json:"amount"`
}

var errTemporary = errors.New("payment service unavailable")

var decodeOrder = kafkaclient.ConsumerMessageConverterFunc[OrderCreated](func(m *sarama.ConsumerMessage) (OrderCreated, error) {
	var o OrderCreated
	if err := json.Unmarshal(m.Value, &o); err != nil {
		return o, fmt.Errorf("decode order: %w", err)
	}
	return o, nil
})

func chargeOrder(_ context.Context, o OrderCreated) error {
	if o.Amount > 1000 {
		return errTemporary // pretend large orders hit a flaky dependency
	}
	log.Printf("charged %s: %d", o.ID, o.Amount)
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := env.KafkaConfig()

	deadLetters, err := kafkaclient.NewSyncProducer(cfg)
	if err != nil {
		return fmt.Errorf("create dead-letter producer: %w", err)
	}
	defer deadLetters.Close()

	// outermost → innermost: park on failure ← retry temporary errors ← decode + charge
	handler := kafkaclient.WrapWithFallbackHandler(
		kafkaclient.WrapWithRetryBackoffHandler(
			kafkaclient.WrapWithSaramaMessageHandler(chargeOrder, decodeOrder),
			kafkaclient.RetryConfig{
				MaxRetries: 3,
				BaseDelay:  200 * time.Millisecond, // 200ms, 400ms, 800ms
				MaxDelay:   2 * time.Second,
				// Only temporary errors are worth retrying; a decode error fails the same way every time.
				RetryCondition: func(err error) bool { return errors.Is(err, errTemporary) },
				ReturnError:    true, // hand the final error to the fallback instead of dropping the message
			},
		),
		// Copies key, value and headers to the dead-letter topic, plus the error and the source position.
		// If that write fails too, the message is redelivered rather than lost.
		kafkaclient.DeadLetterTopic(deadLetters, "orders.dead-letter"),
	)

	consumer, err := kafkaclient.NewConsumerGroup(cfg, "order-charger", "orders", false, handler)
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}

	return consumer.Run(ctx)
}
