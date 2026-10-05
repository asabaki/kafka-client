package kafkaclient

import (
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/asabaki/kafka-client/internal/timeutil"
)

func TestConsistentHashPartitioner_MessageRequiresConsistency(t *testing.T) {
	p := NewConsistentHashPartition("t").(*ConsistentHashPartitioner)

	assert.False(t, p.MessageRequiresConsistency(&sarama.ProducerMessage{}), "keyless messages may use writable partitions only")
	assert.True(t, p.MessageRequiresConsistency(&sarama.ProducerMessage{Key: sarama.StringEncoder("k")}))
	assert.True(t, p.MessageRequiresConsistency(&sarama.ProducerMessage{Headers: []sarama.RecordHeader{
		{Key: []byte(RecordHeaderKeyPartitionKey), Value: []byte("pk")},
	}}))
}

// newActiveForTest freezes the clock (restored when the test ends) so breaker timing is exact.
func newActiveForTest(t *testing.T, maxFailures int, open time.Duration) *activePartitionPartitioner {
	timeutil.Freeze(t, time.Unix(1_000_000, 0))
	return NewActivePartitionPartitioner(ActivePartitionConfig{MaxFailures: maxFailures, OpenDuration: open})("t").(*activePartitionPartitioner)
}

func keyed(key string) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{Topic: "t", Key: sarama.StringEncoder(key)}
}

// sent simulates sarama delivering a message to partition: it records the assignment like Partition does.
func sent(p *activePartitionPartitioner, partition int32) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{Topic: "t", Partition: partition}
	p.mu.Lock()
	p.assigned[msg] = partition
	p.mu.Unlock()
	return msg
}

func fail(p *activePartitionPartitioner, partition int32, times int) {
	for i := 0; i < times; i++ {
		p.OnError(sent(p, partition), errors.New("boom"))
	}
}

func succeed(p *activePartitionPartitioner, partition int32) {
	p.OnSuccess(sent(p, partition))
}

func TestActivePartition_SameAsConsistentHashWhileHealthy(t *testing.T) {
	active := newActiveForTest(t, 3, time.Minute)
	def := NewConsistentHashPartition("t")

	for i := 0; i < 200; i++ {
		msg := keyed(string(rune('a'+i%26)) + string(rune(i)))
		want, err := def.Partition(msg, 12)
		require.NoError(t, err)
		got, err := active.Partition(msg, 12)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
}

func TestActivePartition_ReroutesAfterMaxFailuresAndRecovers(t *testing.T) {
	p := newActiveForTest(t, 3, 30*time.Second)
	msg := keyed("user-1")
	target, err := p.Partition(msg, 6)
	require.NoError(t, err)

	fail(p, target, 2)
	got, _ := p.Partition(msg, 6)
	assert.Equal(t, target, got, "below MaxFailures the breaker stays closed")

	fail(p, target, 1)
	rerouted, _ := p.Partition(msg, 6)
	assert.NotEqual(t, target, rerouted, "breaker open: rerouted")
	again, _ := p.Partition(msg, 6)
	assert.Equal(t, rerouted, again, "a key maps to one healthy partition while the breaker is open")

	timeutil.Advance(30 * time.Second)
	probe, _ := p.Partition(msg, 6)
	assert.Equal(t, target, probe, "half-open after OpenDuration: back to the original partition")

	fail(p, target, 1)
	reopened, _ := p.Partition(msg, 6)
	assert.NotEqual(t, target, reopened, "a failed probe re-opens the breaker immediately")

	timeutil.Advance(30 * time.Second)
	succeed(p, target)
	fail(p, target, 1)
	closed, _ := p.Partition(msg, 6)
	assert.Equal(t, target, closed, "a success resets the failure count")
}

func TestActivePartition_SuccessResetsConsecutiveFailures(t *testing.T) {
	p := newActiveForTest(t, 3, time.Minute)
	msg := keyed("k")
	target, _ := p.Partition(msg, 4)

	fail(p, target, 2)
	succeed(p, target)
	fail(p, target, 2)

	got, _ := p.Partition(msg, 4)
	assert.Equal(t, target, got)
}

func TestActivePartition_AllOpenKeepsNormalPartition(t *testing.T) {
	p := newActiveForTest(t, 1, time.Minute)
	for i := int32(0); i < 3; i++ {
		fail(p, i, 1)
	}

	msg := keyed("k")
	want, _ := NewConsistentHashPartition("t").Partition(msg, 3)
	got, _ := p.Partition(msg, 3)
	assert.Equal(t, want, got)
}

func TestActivePartition_KeylessAvoidsOpenPartitions(t *testing.T) {
	p := newActiveForTest(t, 1, time.Minute)
	fail(p, 0, 1)
	fail(p, 2, 1)

	for i := 0; i < 500; i++ {
		got, err := p.Partition(&sarama.ProducerMessage{Topic: "t"}, 4)
		require.NoError(t, err)
		assert.Contains(t, []int32{1, 3}, got)
	}
}

func TestActivePartition_IgnoresFailuresBeforePartitioning(t *testing.T) {
	p := newActiveForTest(t, 1, time.Minute)
	p.OnError(&sarama.ProducerMessage{Topic: "t"}, errors.New("message too large")) // Partition never called
	assert.Empty(t, p.breakers)
}

func TestActivePartition_ForgetsAssignmentAfterResult(t *testing.T) {
	p := newActiveForTest(t, 3, time.Minute)
	msg := keyed("k")
	_, err := p.Partition(msg, 4)
	require.NoError(t, err)
	require.Len(t, p.assigned, 1)

	p.OnSuccess(msg)
	assert.Empty(t, p.assigned, "no leak once the result is reported")
}

func TestActivePartition_Defaults(t *testing.T) {
	p := NewActivePartitionPartitioner(ActivePartitionConfig{})("t").(*activePartitionPartitioner)
	assert.Equal(t, 3, p.cfg.MaxFailures)
	assert.Equal(t, 30*time.Second, p.cfg.OpenDuration)
}

func TestFeedbackPartitioners_RoutesResultsPerTopic(t *testing.T) {
	f := newFeedbackPartitioners()
	constructor := f.wrap(NewActivePartitionPartitioner(ActivePartitionConfig{MaxFailures: 1}))
	a := constructor("a").(*activePartitionPartitioner)
	b := constructor("b").(*activePartitionPartitioner)

	msg := &sarama.ProducerMessage{Topic: "a", Key: sarama.StringEncoder("k")}
	partition, err := a.Partition(msg, 4)
	require.NoError(t, err)
	f.failure(msg, errors.New("boom"))
	assert.Contains(t, a.breakers, partition)
	assert.Empty(t, b.breakers)

	msg2 := &sarama.ProducerMessage{Topic: "a", Key: sarama.StringEncoder("k2")}
	p2, _ := a.Partition(msg2, 4)
	f.success(msg2)
	assert.NotContains(t, a.breakers, p2)

	// partitioners without feedback (the default) are ignored
	plain := newFeedbackPartitioners()
	_ = plain.wrap(NewConsistentHashPartition)("c")
	plain.failure(&sarama.ProducerMessage{Topic: "c"}, errors.New("boom"))
}
