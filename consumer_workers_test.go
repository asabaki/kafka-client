package kafkaclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSession records marked offsets.
type fakeSession struct {
	sarama.ConsumerGroupSession
	ctx context.Context

	mu     sync.Mutex
	marked []int64
}

func (s *fakeSession) Context() context.Context { return s.ctx }
func (s *fakeSession) MarkMessage(msg *sarama.ConsumerMessage, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marked = append(s.marked, msg.Offset)
}

func (s *fakeSession) markedOffsets() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.marked...)
}

type fakeClaim struct {
	sarama.ConsumerGroupClaim
	messages chan *sarama.ConsumerMessage
}

func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

func msgAt(offset int64, key string) *sarama.ConsumerMessage {
	m := &sarama.ConsumerMessage{Topic: "t", Offset: offset}
	if key != "" {
		m.Key = []byte(key)
	}
	return m
}

func TestOffsetTracker_MarksInOrderWhateverFinishesFirst(t *testing.T) {
	session := &fakeSession{ctx: context.Background()}
	tr := newOffsetTracker(session)

	m0, m1, m2, m3 := msgAt(0, ""), msgAt(1, ""), msgAt(2, ""), msgAt(3, "")
	for _, m := range []*sarama.ConsumerMessage{m0, m1, m2, m3} {
		tr.started(m)
	}

	tr.done(m2)
	tr.done(m1)
	assert.Empty(t, session.markedOffsets(), "offset 0 still running: nothing can be committed")

	tr.done(m0)
	assert.Equal(t, []int64{0, 1, 2}, session.markedOffsets())

	tr.done(m3)
	assert.Equal(t, []int64{0, 1, 2, 3}, session.markedOffsets())
}

func newWorkerHandler(t *testing.T, workers int, handler MessageHandler[*sarama.ConsumerMessage]) *csmGrpHandler {
	t.Helper()
	cfg := KafkaConfig{}.withOptions(WithConsumerWorkers(workers))
	h, err := createSaramaConsumerGroupHandler(cfg, "t", "g", handler)
	require.NoError(t, err)
	return h
}

func TestConsumerWorkers_KeepsPerKeyOrderAndRunsConcurrently(t *testing.T) {
	var mu sync.Mutex
	seen := map[string][]int64{}
	var running, maxRunning atomic.Int32

	h := newWorkerHandler(t, 4, func(_ context.Context, msg *sarama.ConsumerMessage) error {
		now := running.Add(1)
		for {
			prev := maxRunning.Load()
			if now <= prev || maxRunning.CompareAndSwap(prev, now) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		running.Add(-1)

		mu.Lock()
		seen[string(msg.Key)] = append(seen[string(msg.Key)], msg.Offset)
		mu.Unlock()
		return nil
	})

	session := &fakeSession{ctx: context.Background()}
	claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 200)}
	for i := int64(0); i < 200; i++ {
		claim.messages <- msgAt(i, fmt.Sprintf("user-%d", i%10))
	}
	close(claim.messages)

	require.NoError(t, h.ConsumeClaim(session, claim))

	for key, offsets := range seen {
		assert.IsIncreasing(t, offsets, "key %s handled out of order", key)
		assert.Len(t, offsets, 20)
	}
	assert.Greater(t, maxRunning.Load(), int32(1), "handlers ran concurrently")

	marked := session.markedOffsets()
	require.Len(t, marked, 200)
	assert.IsIncreasing(t, marked, "offsets are marked in order")
}

func TestConsumerWorkers_ErrorStopsAndNeverCommitsPastTheFailure(t *testing.T) {
	boom := errors.New("boom")
	h := newWorkerHandler(t, 4, func(_ context.Context, msg *sarama.ConsumerMessage) error {
		if msg.Offset == 10 {
			return boom
		}
		return nil
	})

	session := &fakeSession{ctx: context.Background()}
	claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 100)}
	for i := int64(0); i < 100; i++ {
		claim.messages <- msgAt(i, fmt.Sprintf("user-%d", i%7))
	}
	close(claim.messages)

	err := h.ConsumeClaim(session, claim)
	require.ErrorIs(t, err, boom)

	for _, offset := range session.markedOffsets() {
		assert.Less(t, offset, int64(10), "nothing at or after the failed offset may be committed")
	}
}

func TestConsumerWorkers_StopsOnSessionEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 1)
	h := newWorkerHandler(t, 2, func(context.Context, *sarama.ConsumerMessage) error {
		select {
		case started <- struct{}{}:
		default:
		}
		return nil
	})

	session := &fakeSession{ctx: ctx}
	claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage)} // stays open, like a live partition
	done := make(chan error, 1)
	go func() { done <- h.ConsumeClaim(session, claim) }()

	claim.messages <- msgAt(0, "k")
	<-started
	cancel()

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("ConsumeClaim did not return after the session ended")
	}
}
