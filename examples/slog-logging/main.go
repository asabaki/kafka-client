// slog-logging opts in to kafka-client logging with the standard library's log/slog.
// kafka-client logs nothing unless a Logger is passed with WithLogger.
//
//	go run ./slog-logging
//	SARAMA_DEBUG=1 go run ./slog-logging   # also forward sarama's internal logs at debug level
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"

	kafkaclient "github.com/asabaki/kafka-client"
	"github.com/asabaki/kafka-client/examples/internal/env"
)

// slogAdapter implements kafkaclient.Logger.
type slogAdapter struct{ l *slog.Logger }

func (a slogAdapter) Debug(ctx context.Context, f string, args ...any) {
	a.l.DebugContext(ctx, fmt.Sprintf(f, args...))
}

func (a slogAdapter) Info(ctx context.Context, f string, args ...any) {
	a.l.InfoContext(ctx, fmt.Sprintf(f, args...))
}

func (a slogAdapter) Warn(ctx context.Context, f string, args ...any) {
	a.l.WarnContext(ctx, fmt.Sprintf(f, args...))
}

func (a slogAdapter) Error(ctx context.Context, f string, args ...any) {
	a.l.ErrorContext(ctx, fmt.Sprintf(f, args...))
}

func main() {
	if err := run(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func run() error {
	level := slog.LevelInfo
	cfg := env.KafkaConfig()
	if os.Getenv("SARAMA_DEBUG") != "" {
		level = slog.LevelDebug
		cfg.Debug = true
	}
	cfg.SlowMessageThreshold = time.Millisecond // log every message as "slow" to show the consumer-side logs

	logger := slogAdapter{slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})).With("component", "kafka")}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	producer, err := kafkaclient.NewSyncProducer(cfg, kafkaclient.WithLogger(logger))
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer producer.Close()

	if _, _, err := producer.PublishRawAtLeastOnce(ctx, "logging-demo", "k", []byte("hello"), nil); err != nil {
		logger.Error(ctx, "publish: %s", err)
	}

	consumer, err := kafkaclient.NewConsumerGroup(cfg, "logging-demo", "logging-demo", false,
		func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			slog.InfoContext(ctx, "handled", "value", string(msg.Value))
			return nil
		},
		kafkaclient.WithLogger(logger),
	)
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}

	// Runs for 10 seconds; the "slow message" warning and shutdown logs come from kafka-client.
	return consumer.Run(ctx)
}
