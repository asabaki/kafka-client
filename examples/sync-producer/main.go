// sync-producer publishes typed messages and waits for the broker's acknowledgement of each one.
//
//	go run ./sync-producer
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	kafkaclient "github.com/asabaki/kafka-client"
	"github.com/asabaki/kafka-client/examples/internal/env"
)

// OrderCreated implements kafkaclient.Message, MessageWithHeader and MessageWithPartitionKey.
type OrderCreated struct {
	ID       string    `json:"id"`
	Customer string    `json:"customer"`
	Amount   int       `json:"amount"`
	At       time.Time `json:"at"`
}

func (o *OrderCreated) KafkaTopic() string                   { return "orders" }
func (o *OrderCreated) KafkaMessageKey() string              { return o.ID }
func (o *OrderCreated) KafkaMessagePayload() ([]byte, error) { return json.Marshal(o) }

// KafkaPartitionKey keeps all orders of a customer on one partition, so they are consumed in order.
func (o *OrderCreated) KafkaPartitionKey() string { return o.Customer }

func (o *OrderCreated) KafkaMessageHeaders() map[string]string {
	return map[string]string{"content-type": "application/json", "schema": "order-created/v1"}
}

func main() {
	producer, err := kafkaclient.NewSyncProducer(env.KafkaConfig())
	if err != nil {
		log.Fatalf("create producer: %s", err)
	}
	defer producer.Close()

	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		order := &OrderCreated{
			ID:       fmt.Sprintf("order-%d", i),
			Customer: fmt.Sprintf("customer-%d", i%2),
			Amount:   i * 100,
			At:       time.Now(),
		}

		// The deadline bounds the whole publish, including a broker that stopped answering.
		publishCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		partition, offset, err := producer.PublishAtLeastOnce(publishCtx, order, kafkaclient.WithTimestamp(order.At))
		cancel()
		if err != nil {
			// Not stored, or (on a deadline) not known to be stored. Retry, report upstream, or write it somewhere
			// durable; a retry after a deadline can create a duplicate.
			log.Printf("publish %s: %s", order.ID, err)
			continue
		}
		log.Printf("stored %s (customer %s) at partition %d offset %d", order.ID, order.Customer, partition, offset)
	}

	// Raw bytes, no Message type needed.
	if _, _, err := producer.PublishRawAtLeastOnce(ctx, "orders", "order-raw", []byte(`{"id":"order-raw"}`),
		map[string]string{"source": "example"}); err != nil {
		log.Printf("publish raw: %s", err)
	}
}
