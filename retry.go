package kafkaclient

import (
	"context"
	"time"

	"github.com/IBM/sarama"
)

type RetryConfig struct {
	MaxRetries     int
	BaseDelay      time.Duration
	MaxDelay       time.Duration
	RetryCondition func(error) bool
	// ReturnError returns the last error once retries are exhausted or RetryCondition rejects it, instead of nil.
	// Use it to chain a WrapWithFallbackHandler after the retries; on its own, an error blocks the partition.
	ReturnError bool
	// Logger is optional; retries are logged only when it is set.
	Logger Logger
}

// WrapWithRetryBackoffHandler wraps a MessageHandler with retry and backoff logic.
// The wait between attempts ends early when ctx is canceled.
func WrapWithRetryBackoffHandler(handler MessageHandler[*sarama.ConsumerMessage], cfg RetryConfig) MessageHandler[*sarama.ConsumerMessage] {
	return func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		return retryWithBackoff(ctx, cfg, func() error {
			return handler(ctx, msg)
		})
	}
}

// WrapWithRetryBackoffBatchHandler wraps a MessagesHandler with retry and backoff logic
func WrapWithRetryBackoffBatchHandler(handler MessagesHandler[*sarama.ConsumerMessage], cfg RetryConfig) MessagesHandler[*sarama.ConsumerMessage] {
	return func(messages []*MessageWithContext[*sarama.ConsumerMessage]) error {
		return retryWithBackoff(context.Background(), cfg, func() error {
			return handler(messages)
		})
	}
}

// RetryWithBackoff calls handler until it succeeds, RetryCondition rejects the error, or MaxRetries is exhausted.
// Then it returns nil (the message is marked as consumed), or the last error when cfg.ReturnError is set.
func RetryWithBackoff(cfg RetryConfig, handler func() error) error {
	return retryWithBackoff(context.Background(), cfg, handler)
}

func retryWithBackoff(ctx context.Context, cfg RetryConfig, handler func() error) error {
	logger := loggerOrNop(cfg.Logger)
	giveUp := func(err error) error {
		if cfg.ReturnError {
			return err
		}
		return nil
	}

	for retryCount := 0; retryCount <= cfg.MaxRetries; retryCount++ {
		err := handler()
		if err == nil {
			return nil
		}

		if cfg.RetryCondition != nil && !cfg.RetryCondition(err) {
			return giveUp(err)
		}

		if retryCount == cfg.MaxRetries {
			logger.Warn(ctx, "message exceeded max retries (%d), marking as processed: %s", cfg.MaxRetries, err)
			return giveUp(err)
		}

		backoff := calculateBackoff(retryCount, cfg.BaseDelay, cfg.MaxDelay)
		logger.Info(ctx, "retrying message handling (attempt %d) after %s: %s", retryCount+1, backoff, err)

		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return giveUp(err)
		}
	}

	return nil
}

func calculateBackoff(retryCount int, baseDelay, maxDelay time.Duration) time.Duration {
	if retryCount < 0 {
		retryCount = 0
	}

	backoff := baseDelay * time.Duration(1<<retryCount)
	if backoff > maxDelay {
		return maxDelay
	}
	return backoff
}
