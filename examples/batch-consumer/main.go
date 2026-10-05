// batch-consumer hands messages to the handler in batches: up to 100 at once, or whatever arrived within 2 seconds.
// Useful for bulk writes (databases, object storage, search indexes).
//
//	go run ./batch-consumer
package main

import (
	"context"
	"encoding/json"
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

type PageView struct {
	User string `json:"-"`
	Path string `json:"path"`
}

var decodePageView = kafkaclient.ConsumerMessageConverterFunc[PageView](func(m *sarama.ConsumerMessage) (PageView, error) {
	var v PageView
	err := json.Unmarshal(m.Value, &v)
	v.User = string(m.Key)
	return v, err
})

// bulkInsert stands in for a database bulk write.
func bulkInsert(ctx context.Context, rows []PageView) error {
	log.Printf("inserting %d page views (first user %s)", len(rows), rows[0].User)
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

	consumer, err := kafkaclient.NewBatchConsumerGroup(env.KafkaConfig(), "page-view-archiver", "page-views",
		100,           // batch size
		2*time.Second, // flush a partial batch after this long
		false,
		kafkaclient.WrapWithSaramaMessagesHandler(func(messages []*kafkaclient.MessageWithContext[PageView]) error {
			rows := make([]PageView, 0, len(messages))
			for _, m := range messages {
				_, view := m.Get() // each message also has its own ctx (trace context from its headers)
				rows = append(rows, view)
			}

			// nil marks the whole batch as consumed; an error redelivers the whole batch.
			return bulkInsert(ctx, rows)
		}, decodePageView),
	)
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}

	return consumer.Run(ctx)
}
