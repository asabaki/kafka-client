package kafkaclient

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
	"github.com/cespare/xxhash/v2"

	"github.com/asabaki/kafka-client/internal/timeutil"
)

// WithConsumerWorkers handles each partition's messages with n goroutines instead of one (NewConsumerGroup only).
//
// Messages are routed by key (the partition key header, else the message key): all messages with the same key go
// to the same worker, in order, so per-key ordering is kept. Messages without a key are spread over the workers.
// Offsets are committed only up to the oldest message not yet handled, so a restart never skips one.
//
// n is per partition: a consumer that owns 6 partitions with n=4 runs up to 24 handlers at once.
// Handlers must be safe for concurrent use. n <= 1 keeps the default of one goroutine per partition.
//
// When a handler returns an error, no new messages are started, the ones already running finish, and the
// session ends so the failed message (and everything after it that wasn't committed) is redelivered.
func WithConsumerWorkers(n int) KafkaConfigOption {
	return kafkaConfigOptionFunc(func(cfg *KafkaConfig) {
		cfg.consumerWorkers = n
	})
}

// consumeWithWorkers is ConsumeClaim for consumerWorkers > 1.
func (c *csmGrpHandler) consumeWithWorkers(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim, n int) error {
	tracker := newOffsetTracker(session)
	pool := newWorkerPool(n, func(msg *sarama.ConsumerMessage) error {
		if err := c.handle(msg); err != nil {
			return err
		}
		tracker.done(msg)
		return nil
	})

	var err error
	drain := false // true: finish queued messages before returning
	nextRoundRobin := 0

dispatch:
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				// all of this claim's messages are dispatched: let the queued ones finish, so they are committed
				drain = true
				break dispatch
			}
			if msg == nil {
				continue
			}

			worker := nextRoundRobin
			if key := consumerPartitioningKey(msg); key != nil {
				worker = int(xxhash.Sum64(key) % uint64(n)) // #nosec G115 -- n is a small positive int
			} else {
				nextRoundRobin = (nextRoundRobin + 1) % n
			}

			tracker.started(msg)
			if !pool.submit(worker, msg) {
				err = pool.firstError()
				break dispatch
			}
		case err = <-pool.failed:
			break dispatch
		case <-session.Context().Done():
			break dispatch
		}
	}

	if drain {
		pool.drain()
	} else {
		pool.stop() // queued messages are skipped (they are redelivered); running handlers finish
	}
	if err == nil {
		err = pool.firstError()
	}

	return err
}

// handle runs the handler for one message and records its metrics. The message is not marked here.
func (c *csmGrpHandler) handle(msg *sarama.ConsumerMessage) error {
	st := timeutil.Now()

	ctx := extractTraceContext(context.Background(), c.propagator, msg)
	if err := c.handler(ctx, msg); err != nil {
		c.logger.Error(ctx, "message handling failed, stopping session (message will be redelivered): topic=%s consumer_group=%s partition=%d offset=%d: %s",
			c.topic, c.consumerGroupName, msg.Partition, msg.Offset, err)
		return err
	}

	et := timeutil.Now()
	c.metrics.processDuration.Record(context.Background(), et.Sub(st).Milliseconds(), c.metrics.attrs)
	recordEndToEnd(ctx, c.cfg, c.logger, c.metrics, c.topic, c.consumerGroupName, et, msg)

	return nil
}

func consumerPartitioningKey(msg *sarama.ConsumerMessage) []byte {
	for _, h := range msg.Headers {
		if h != nil && string(h.Key) == RecordHeaderKeyPartitionKey {
			return h.Value
		}
	}
	return msg.Key
}

// workerPool runs one goroutine per worker, each handling its queue in order.
type workerPool struct {
	queues  []chan *sarama.ConsumerMessage
	wg      sync.WaitGroup
	failed  chan error // receives the first handler error
	stopped chan struct{}

	once     sync.Once
	errMu    sync.Mutex
	firstErr error
}

func newWorkerPool(n int, handle func(*sarama.ConsumerMessage) error) *workerPool {
	p := &workerPool{
		queues:  make([]chan *sarama.ConsumerMessage, n),
		failed:  make(chan error, 1),
		stopped: make(chan struct{}),
	}

	for i := range p.queues {
		queue := make(chan *sarama.ConsumerMessage, 16)
		p.queues[i] = queue

		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for msg := range queue {
				if p.isStopped() {
					continue // skip what is queued once stopping: it is redelivered later
				}
				if err := handle(msg); err != nil {
					p.fail(err)
				}
			}
		}()
	}

	return p
}

// submit queues msg for worker; it returns false when the pool stopped because a handler failed.
func (p *workerPool) submit(worker int, msg *sarama.ConsumerMessage) bool {
	select {
	case p.queues[worker] <- msg:
		return true
	case <-p.stopped:
		return false
	}
}

func (p *workerPool) fail(err error) {
	p.errMu.Lock()
	if p.firstErr == nil {
		p.firstErr = err
		p.failed <- err
	}
	p.errMu.Unlock()
	p.halt()
}

func (p *workerPool) firstError() error {
	p.errMu.Lock()
	defer p.errMu.Unlock()
	return p.firstErr
}

func (p *workerPool) halt() {
	p.once.Do(func() { close(p.stopped) })
}

func (p *workerPool) isStopped() bool {
	select {
	case <-p.stopped:
		return true
	default:
		return false
	}
}

// stop skips queued messages, waits for running handlers and releases the workers.
func (p *workerPool) stop() {
	p.halt()
	p.drain()
}

// drain handles every queued message (unless a handler fails meanwhile) and releases the workers.
func (p *workerPool) drain() {
	for _, q := range p.queues {
		close(q)
	}
	p.wg.Wait()
}

// offsetTracker marks messages as consumed in offset order, whatever order they finish in: only up to the oldest
// message that was started but hasn't finished. Used by one partition's claim.
type offsetTracker struct {
	session sarama.ConsumerGroupSession

	mu       sync.Mutex
	pending  []*sarama.ConsumerMessage // started and not yet marked, in offset order
	finished map[int64]bool
}

func newOffsetTracker(session sarama.ConsumerGroupSession) *offsetTracker {
	return &offsetTracker{session: session, finished: map[int64]bool{}}
}

func (t *offsetTracker) started(msg *sarama.ConsumerMessage) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.pending = append(t.pending, msg)
}

func (t *offsetTracker) done(msg *sarama.ConsumerMessage) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.finished[msg.Offset] = true

	marked := 0
	for _, m := range t.pending {
		if !t.finished[m.Offset] {
			break
		}
		t.session.MarkMessage(m, "")
		delete(t.finished, m.Offset)
		marked++
	}
	t.pending = t.pending[marked:]
}
