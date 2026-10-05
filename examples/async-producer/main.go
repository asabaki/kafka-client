// async-producer publishes fire-and-forget messages and learns about delivery through callbacks.
// It shows the part that is easy to get wrong: Close on shutdown, so buffered messages are not lost.
//
//	go run ./async-producer
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IBM/sarama"

	kafkaclient "github.com/asabaki/kafka-client"
	"github.com/asabaki/kafka-client/examples/internal/env"
)

func main() {
	var delivered, failed atomic.Int64

	cfg := env.KafkaConfig()
	cfg.ProducerFlushFrequency = 50 * time.Millisecond // send buffered messages at least every 50ms
	cfg.ProducerFlushMessages = 500                    // or as soon as 500 are buffered

	producer, err := kafkaclient.NewAsyncProducer(cfg,
		kafkaclient.WithAsyncSuccessHandler(func(*sarama.ProducerMessage) {
			delivered.Add(1)
		}),
		kafkaclient.WithAsyncErrorHandler(func(pe *sarama.ProducerError) {
			failed.Add(1)
			// The message is lost unless you store it here (e.g. a fallback queue or a local file).
			log.Printf("delivery to %s failed: %s", pe.Msg.Topic, pe.Err)
		}),
	)
	if err != nil {
		log.Fatalf("create producer: %s", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Publish page views until Ctrl-C, or 3 seconds.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	published := 0
	for ctx.Err() == nil {
		user := fmt.Sprintf("user-%d", published%50)
		// Returns immediately; blocks only when sarama's input queue is full (backpressure).
		producer.PublishRawAtMostOnce(ctx, "page-views", user, []byte(`{"path":"/"}`), nil)
		published++
		time.Sleep(time.Millisecond)
	}

	// Required: flushes every buffered message and waits until all callbacks above have run.
	if err := producer.Close(); err != nil {
		log.Printf("close: %s", err)
	}
	log.Printf("published %d, delivered %d, failed %d", published, delivered.Load(), failed.Load())
}
