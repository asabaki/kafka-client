package kafkaclient

import (
	"sync"
	"time"

	"github.com/IBM/sarama"

	"github.com/asabaki/kafka-client/internal/timeutil"
)

// feedbackPartitioners remembers the partitioner sarama created for each topic, so producers can report send
// results back to it. sarama keeps its partitioners private.
type feedbackPartitioners struct {
	mu      sync.Mutex
	byTopic map[string]PartitionFeedback
}

// wrap returns a constructor that records every partitioner implementing PartitionFeedback.
// It returns nil when constructor is nil.
func (f *feedbackPartitioners) wrap(constructor sarama.PartitionerConstructor) sarama.PartitionerConstructor {
	return func(topic string) sarama.Partitioner {
		partitioner := constructor(topic)
		if fb, ok := partitioner.(PartitionFeedback); ok {
			f.mu.Lock()
			f.byTopic[topic] = fb
			f.mu.Unlock()
		}
		return partitioner
	}
}

func (f *feedbackPartitioners) get(topic string) PartitionFeedback {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.byTopic[topic]
}

func (f *feedbackPartitioners) success(msg *sarama.ProducerMessage) {
	if fb := f.get(msg.Topic); fb != nil {
		fb.OnSuccess(msg)
	}
}

func (f *feedbackPartitioners) failure(msg *sarama.ProducerMessage, err error) {
	if fb := f.get(msg.Topic); fb != nil {
		fb.OnError(msg, err)
	}
}

func newFeedbackPartitioners() *feedbackPartitioners {
	return &feedbackPartitioners{byTopic: map[string]PartitionFeedback{}}
}

// PartitionFeedback is implemented by partitioners that want to know the outcome of every send.
// The sync and async producers report each acknowledged message to OnSuccess and each failed one to OnError.
type PartitionFeedback interface {
	OnSuccess(msg *sarama.ProducerMessage)
	OnError(msg *sarama.ProducerMessage, err error)
}

// ActivePartitionConfig configures NewActivePartitionPartitioner.
type ActivePartitionConfig struct {
	// MaxFailures is the number of consecutive failed sends that opens a partition's circuit breaker. Default 3.
	MaxFailures int
	// OpenDuration is how long an open circuit breaker keeps a partition out of rotation before one probe
	// message is allowed through again (half-open). Default 30s.
	OpenDuration time.Duration
}

// NewActivePartitionPartitioner returns a partitioner that routes around partitions that keep failing, e.g. while a
// broker is down and its partitions wait for a new leader.
//
// It partitions exactly like the default ConsistentHashPartitioner until a partition fails MaxFailures times in a
// row. That partition's circuit breaker then opens for OpenDuration, and messages hashed to it are re-hashed onto the
// partitions whose breaker is closed. After OpenDuration the partition gets messages again; one success closes the
// breaker, one failure re-opens it. If every partition is open, messages keep their normal partition.
//
// Use it with WithPartitioner:
//
//	kafkaclient.NewSyncProducer(cfg, kafkaclient.WithPartitioner(
//		kafkaclient.NewActivePartitionPartitioner(kafkaclient.ActivePartitionConfig{})))
//
// Trade-off: while a breaker is open, messages with the same key can land on different partitions, so per-key
// ordering is not guaranteed. Only use it for topics whose consumers don't rely on that ordering.
//
// Messages without a key are spread randomly over the partitions whose breaker is closed.
//
// Rerouting protects the messages sent after the breaker opened. A message that already failed is not moved: it
// is returned as an error (sync) or passed to the async error handler.
func NewActivePartitionPartitioner(cfg ActivePartitionConfig) sarama.PartitionerConstructor {
	if cfg.MaxFailures <= 0 {
		cfg.MaxFailures = 3
	}
	if cfg.OpenDuration <= 0 {
		cfg.OpenDuration = 30 * time.Second
	}

	return func(topic string) sarama.Partitioner {
		return &activePartitionPartitioner{
			cfg:      cfg,
			random:   sarama.NewRandomPartitioner(topic),
			breakers: map[int32]*partitionBreaker{},
			assigned: map[*sarama.ProducerMessage]int32{},
			metrics:  newBreakerMetrics(topic),
		}
	}
}

var _ PartitionFeedback = (*activePartitionPartitioner)(nil)

// activePartitionPartitioner is created by sarama once per topic.
type activePartitionPartitioner struct {
	cfg    ActivePartitionConfig
	random sarama.Partitioner

	mu       sync.Mutex
	breakers map[int32]*partitionBreaker
	// assigned holds the partition chosen for each message still in flight. sarama fills msg.Partition only after
	// partitioning, and a message rejected earlier (e.g. too large) would otherwise be charged to partition 0.
	assigned map[*sarama.ProducerMessage]int32
	metrics  *breakerMetrics
}

type partitionBreaker struct {
	failures int
	openedAt time.Time // zero = closed
}

// RequiresConsistency is always true so sarama passes the full partition count: the returned index is then the
// partition id, which the circuit breakers are keyed by. Offline partitions are skipped by the breakers instead.
func (p *activePartitionPartitioner) RequiresConsistency() bool {
	return true
}

func (p *activePartitionPartitioner) Partition(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	partition, err := p.partition(msg, numPartitions)
	if err == nil {
		p.mu.Lock()
		p.assigned[msg] = partition
		p.mu.Unlock()
	}

	return partition, err
}

func (p *activePartitionPartitioner) partition(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	key := partitioningKey(msg)
	if key == nil {
		return p.randomHealthy(msg, numPartitions)
	}

	target, err := hashKey(key, numPartitions)
	if err != nil {
		return -1, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.isOpenLocked(target) {
		return target, nil
	}

	healthy := p.healthyLocked(numPartitions)
	if len(healthy) == 0 {
		return target, nil
	}
	// re-hash over the healthy partitions so a key still maps to one partition while the breaker stays open
	idx, err := hashKey(key, int32(len(healthy))) // #nosec G115 -- len <= numPartitions (int32)
	if err != nil {
		return -1, err
	}

	return healthy[idx], nil
}

func (p *activePartitionPartitioner) randomHealthy(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	p.mu.Lock()
	healthy := p.healthyLocked(numPartitions)
	p.mu.Unlock()

	if len(healthy) == 0 || len(healthy) == int(numPartitions) {
		return p.random.Partition(msg, numPartitions)
	}
	idx, err := p.random.Partition(msg, int32(len(healthy))) // #nosec G115 -- len <= numPartitions (int32)
	if err != nil {
		return -1, err
	}

	return healthy[idx], nil
}

func (p *activePartitionPartitioner) OnSuccess(msg *sarama.ProducerMessage) {
	p.mu.Lock()
	defer p.mu.Unlock()

	partition, ok := p.takeAssignedLocked(msg)
	if !ok {
		return
	}
	if b := p.breakers[partition]; b != nil && !b.openedAt.IsZero() {
		p.metrics.closed(partition)
	}
	delete(p.breakers, partition)
}

func (p *activePartitionPartitioner) OnError(msg *sarama.ProducerMessage, _ error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	partition, ok := p.takeAssignedLocked(msg)
	if !ok {
		return // rejected before partitioning (e.g. message too large): not the partition's fault
	}

	b := p.breakers[partition]
	if b == nil {
		b = &partitionBreaker{}
		p.breakers[partition] = b
	}

	b.failures++
	if b.failures >= p.cfg.MaxFailures {
		// also re-opens a half-open breaker whose probe failed
		p.metrics.opened(partition, !b.openedAt.IsZero())
		b.openedAt = timeutil.Now()
	}
}

func (p *activePartitionPartitioner) takeAssignedLocked(msg *sarama.ProducerMessage) (int32, bool) {
	partition, ok := p.assigned[msg]
	delete(p.assigned, msg)
	return partition, ok
}

func (p *activePartitionPartitioner) isOpenLocked(partition int32) bool {
	b := p.breakers[partition]
	if b == nil || b.openedAt.IsZero() {
		return false
	}
	// after OpenDuration the partition is half-open: it gets messages again, and the next result decides
	return timeutil.Since(b.openedAt) < p.cfg.OpenDuration
}

func (p *activePartitionPartitioner) healthyLocked(numPartitions int32) []int32 {
	healthy := make([]int32, 0, numPartitions)
	for i := int32(0); i < numPartitions; i++ {
		if !p.isOpenLocked(i) {
			healthy = append(healthy, i)
		}
	}
	return healthy
}
