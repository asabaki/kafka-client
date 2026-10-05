package kafkaclient_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kafkaclient "github.com/asabaki/kafka-client"
)

func TestWrapWithFallbackHandler(t *testing.T) {
	boom := errors.New("boom")
	msg := &sarama.ConsumerMessage{Offset: 7}

	t.Run("success skips fallback", func(t *testing.T) {
		called := false
		h := kafkaclient.WrapWithFallbackHandler(
			func(context.Context, *sarama.ConsumerMessage) error { return nil },
			func(context.Context, *sarama.ConsumerMessage, error) error { called = true; return nil },
		)
		require.NoError(t, h(context.Background(), msg))
		assert.False(t, called)
	})

	t.Run("failure is parked and consumed", func(t *testing.T) {
		var got error
		h := kafkaclient.WrapWithFallbackHandler(
			func(context.Context, *sarama.ConsumerMessage) error { return boom },
			func(_ context.Context, m *sarama.ConsumerMessage, err error) error { got = err; return nil },
		)
		require.NoError(t, h(context.Background(), msg))
		assert.ErrorIs(t, got, boom)
	})

	t.Run("fallback failure redelivers", func(t *testing.T) {
		fbErr := errors.New("dlq down")
		h := kafkaclient.WrapWithFallbackHandler(
			func(context.Context, *sarama.ConsumerMessage) error { return boom },
			func(context.Context, *sarama.ConsumerMessage, error) error { return fbErr },
		)
		err := h(context.Background(), msg)
		assert.ErrorIs(t, err, boom)
		assert.ErrorIs(t, err, fbErr)
	})
}

func TestWrapWithFallbackBatchHandler(t *testing.T) {
	boom := errors.New("boom")
	batch := []*kafkaclient.MessageWithContext[*sarama.ConsumerMessage]{
		kafkaclient.NewMessageWithContext(context.Background(), &sarama.ConsumerMessage{Offset: 1}),
		kafkaclient.NewMessageWithContext(context.Background(), &sarama.ConsumerMessage{Offset: 2}),
	}

	var parked []int64
	h := kafkaclient.WrapWithFallbackBatchHandler(
		func([]*kafkaclient.MessageWithContext[*sarama.ConsumerMessage]) error { return boom },
		func(_ context.Context, m *sarama.ConsumerMessage, _ error) error {
			parked = append(parked, m.Offset)
			return nil
		},
	)
	require.NoError(t, h(batch))
	assert.Equal(t, []int64{1, 2}, parked)
}

func TestRetryReturnErrorChainsIntoFallback(t *testing.T) {
	temporary := errors.New("temporary")
	attempts := 0
	var parkedErr error

	h := kafkaclient.WrapWithFallbackHandler(
		kafkaclient.WrapWithRetryBackoffHandler(
			func(context.Context, *sarama.ConsumerMessage) error { attempts++; return temporary },
			kafkaclient.RetryConfig{MaxRetries: 2, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond, ReturnError: true},
		),
		func(_ context.Context, _ *sarama.ConsumerMessage, err error) error { parkedErr = err; return nil },
	)

	require.NoError(t, h(context.Background(), &sarama.ConsumerMessage{}))
	assert.Equal(t, 3, attempts, "1 attempt + 2 retries before falling back")
	assert.ErrorIs(t, parkedErr, temporary)
}

func TestRetryBackoffStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0

	h := kafkaclient.WrapWithRetryBackoffHandler(
		func(context.Context, *sarama.ConsumerMessage) error {
			attempts++
			cancel()
			return errors.New("fail")
		},
		kafkaclient.RetryConfig{MaxRetries: 5, BaseDelay: time.Hour, MaxDelay: time.Hour, ReturnError: true},
	)

	start := time.Now()
	err := h(ctx, &sarama.ConsumerMessage{})
	assert.Error(t, err)
	assert.Equal(t, 1, attempts)
	assert.Less(t, time.Since(start), time.Second, "the backoff wait ends when ctx is canceled")
}
