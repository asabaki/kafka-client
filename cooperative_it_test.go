package kafkaclient_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kafkaclient "github.com/asabaki/kafka-client"
)

// claimEnds counts the library's debug logs for a partition claim ending: a cooperative revoke closes the claim's
// channel, an eager rebalance ends the whole session. Either happens only when a rebalance takes the partition away
// (or the consumer stops).
type claimEnds struct {
	mu    sync.Mutex
	count int
}

func (c *claimEnds) Debug(_ context.Context, format string, _ ...any) {
	if strings.HasPrefix(format, "consumer claim channel has been closed") || strings.HasPrefix(format, "consumer session ended") {
		c.mu.Lock()
		c.count++
		c.mu.Unlock()
	}
}
func (*claimEnds) Info(context.Context, string, ...any)  {}
func (*claimEnds) Warn(context.Context, string, ...any)  {}
func (*claimEnds) Error(context.Context, string, ...any) {}

func (c *claimEnds) value() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

type partitionsSeen struct {
	mu   sync.Mutex
	seen map[int32]bool
}

func (p *partitionsSeen) handler() kafkaclient.MessageHandler[*sarama.ConsumerMessage] {
	return func(_ context.Context, msg *sarama.ConsumerMessage) error {
		p.mu.Lock()
		p.seen[msg.Partition] = true
		p.mu.Unlock()
		return nil
	}
}

func (p *partitionsSeen) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.seen)
}

// produceSteadily writes one message to each partition every interval until ctx ends.
func produceSteadily(ctx context.Context, t *testing.T, topic string, partitions int32, interval time.Duration) <-chan struct{} {
	t.Helper()

	cfg := createSaramaConfigForTest()
	cfg.Producer.Partitioner = sarama.NewManualPartitioner
	producer, err := sarama.NewSyncProducer(getKafkaBootstrapServers(), cfg)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = producer.Close() }()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			for p := int32(0); p < partitions; p++ {
				if _, _, err := producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Partition: p, Value: sarama.StringEncoder(fmt.Sprint(i))}); err != nil {
					t.Errorf("produce: %s", err)
					return
				}
			}
		}
	}()
	return done
}

// claimsRevokedFromFirstMember starts member a alone, lets member b join, and reports how many of a's claims ended,
// and how many partitions b took over.
func claimsRevokedFromFirstMember(t *testing.T, strategies []string) (revoked, movedToB int) {
	t.Helper()

	const partitions = 4
	admin := createClusterAdmin(t, getKafkaBootstrapServers())
	topic := createTestTopicWithPartitions(t, admin, "join", partitions)
	group := topic + "-g"

	cfg := makeKafkaConfigForTest()
	cfg.ConsumerBalanceStrategies = strategies

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	produceCtx, stopProducing := context.WithCancel(ctx)
	producing := produceSteadily(produceCtx, t, topic, partitions, 20*time.Millisecond)
	defer func() {
		stopProducing()
		<-producing
	}()

	aEnds := &claimEnds{}
	aSeen := &partitionsSeen{seen: map[int32]bool{}}
	a, err := kafkaclient.NewConsumerGroup(cfg, group, topic, false, aSeen.handler(), kafkaclient.WithLogger(aEnds))
	require.NoError(t, err)
	go func() { _ = a.Run(ctx) }()
	require.Eventually(t, func() bool { return aSeen.count() == partitions }, 30*time.Second, 100*time.Millisecond,
		"member a owns every partition")
	require.Zero(t, aEnds.value(), "no claim ends before b joins")

	bSeen := &partitionsSeen{seen: map[int32]bool{}}
	b, err := kafkaclient.NewConsumerGroup(cfg, group, topic, false, bSeen.handler())
	require.NoError(t, err)
	go func() { _ = b.Run(ctx) }()
	require.Eventually(t, func() bool { return bSeen.count() > 0 }, 30*time.Second, 100*time.Millisecond,
		"member b takes over partitions")
	time.Sleep(2 * time.Second)

	return aEnds.value(), bSeen.count()
}

func TestCooperativeSticky_JoiningMemberRevokesOnlyMovedPartitions(t *testing.T) {
	requireKafka(t)

	revoked, moved := claimsRevokedFromFirstMember(t, []string{kafkaclient.BalanceStrategyCooperativeSticky})
	assert.Equal(t, moved, revoked, "only the partitions handed to b are revoked from a")
}

// Guards the cooperative test: with an eager strategy the same join revokes every partition of member a, so the
// count must see more revocations than moved partitions.
func TestRange_JoiningMemberRevokesEveryPartition(t *testing.T) {
	requireKafka(t)

	revoked, moved := claimsRevokedFromFirstMember(t, []string{kafkaclient.BalanceStrategyRange})
	assert.Equal(t, 4, revoked, "an eager join revokes all of a's partitions")
	assert.Less(t, moved, revoked)
}
