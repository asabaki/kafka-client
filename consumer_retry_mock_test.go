package kafkaclient_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kafkaclient "github.com/asabaki/kafka-client"
)

// newMockGroupBroker answers everything a single-member group consuming topic/0 needs, with join responses from join.
func newMockGroupBroker(t *testing.T, topic, group string, join sarama.MockResponse, messages ...string) *sarama.MockBroker {
	t.Helper()

	broker := sarama.NewMockBroker(t, 1)
	t.Cleanup(broker.Close)

	fetch := sarama.NewMockFetchResponse(t, 1)
	for i, m := range messages {
		fetch.SetMessage(topic, 0, int64(i), sarama.StringEncoder(m))
	}
	assignment := &sarama.ConsumerGroupMemberAssignment{Topics: map[string][]int32{topic: {0}}}
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(topic, 0, broker.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, group, broker),
		"JoinGroupRequest":   join,
		"SyncGroupRequest":   sarama.NewMockSyncGroupResponse(t).SetMemberAssignment(assignment),
		"HeartbeatRequest":   sarama.NewMockHeartbeatResponse(t),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).SetOffset(group, topic, 0, 0, "", sarama.ErrNoError),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(topic, 0, sarama.OffsetOldest, 0).
			SetOffset(topic, 0, sarama.OffsetNewest, int64(len(messages))),
		"FetchRequest":        fetch,
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(t),
		"LeaveGroupRequest":   sarama.NewMockLeaveGroupResponse(t),
	})
	return broker
}

// mockGroupCompatible: sarama's MockBroker answers no ApiVersions, and a failed join must reach kafka-client's own
// retry loop instead of sarama's internal retries.
var mockGroupCompatible = kafkaclient.WithSaramaConfigHookForTest(func(c *sarama.Config) {
	c.ApiVersionsRequest = false
	c.Version = sarama.V2_8_0_0
	c.Consumer.Group.Rebalance.Retry.Max = 0
})

func mockGroupConfig(broker *sarama.MockBroker) kafkaclient.KafkaConfig {
	return kafkaclient.KafkaConfig{BootstrapServers: []string{broker.Addr()}, MetricsDisabled: true, ConsumerGroupMaxRetry: 3, ProducerTimeout: time.Second}
}

// A handler that panics must not take down the process: the panic becomes the handler error, the fallback parks
// the message and the partition moves on to the next one.
func TestConsumerGroup_HandlerPanicIsParkedByFallback(t *testing.T) {
	const topic, group = "orders", "orders-group"
	broker := newMockGroupBroker(t, topic, group,
		sarama.NewMockJoinGroupResponse(t).SetGroupProtocol(sarama.RangeBalanceStrategyName), "poison", "order-2")

	parked := make(chan error, 1)
	handled := make(chan string, 2)
	handler := kafkaclient.WrapWithFallbackHandler(
		func(_ context.Context, msg *sarama.ConsumerMessage) error {
			if string(msg.Value) == "poison" {
				panic("cannot decode order")
			}
			handled <- string(msg.Value)
			return nil
		},
		func(_ context.Context, _ *sarama.ConsumerMessage, err error) error {
			parked <- err
			return nil
		},
	)
	consumer, err := kafkaclient.NewConsumerGroup(mockGroupConfig(broker), group, topic, false, handler, mockGroupCompatible)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = consumer.Run(ctx) }()

	select {
	case err := <-parked:
		assert.ErrorIs(t, err, kafkaclient.ErrHandlerPanic)
	case <-time.After(15 * time.Second):
		t.Fatal("panicking message never reached the fallback")
	}
	select {
	case msg := <-handled:
		assert.Equal(t, "order-2", msg)
	case <-time.After(15 * time.Second):
		t.Fatal("partition did not move on after the panic")
	}
}

// A failed join (the group coordinator broker just died) must be retried: the consumer recovers once the group is
// reachable again, instead of counting the first error until ConsumerGroupMaxRetry and stopping.
func TestConsumerGroup_RetriesJoinAfterError(t *testing.T) {
	const topic, group = "orders", "orders-group"

	broker := newMockGroupBroker(t, topic, group, sarama.NewMockSequence(
		sarama.NewMockJoinGroupResponse(t).SetError(sarama.ErrConsumerCoordinatorNotAvailable),
		sarama.NewMockJoinGroupResponse(t).SetGroupProtocol(sarama.RangeBalanceStrategyName),
	), "order-1")

	handled := make(chan string, 1)
	consumer, err := kafkaclient.NewConsumerGroup(mockGroupConfig(broker), group, topic, false, func(_ context.Context, msg *sarama.ConsumerMessage) error {
		select {
		case handled <- string(msg.Value):
		default:
		}
		return nil
	}, mockGroupCompatible)
	require.NoError(t, err)

	var runErr atomic.Value
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := consumer.Run(ctx); err != nil {
			runErr.Store(err)
		}
	}()

	select {
	case msg := <-handled:
		assert.Equal(t, "order-1", msg)
	case <-time.After(15 * time.Second):
		t.Fatalf("consumer never recovered from a failed join (run error: %v)", runErr.Load())
	}
}

// A member with no partitions (more replicas than partitions) never gets a claim. Its failure count must still reset
// after each successful join, or a few unrelated broker hiccups over its lifetime add up to ConsumerGroupMaxRetry
// and stop it for good.
func TestConsumerGroup_IdleMemberRetryCountResetsAfterJoin(t *testing.T) {
	const topic, group = "orders", "orders-group"

	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()

	fail := sarama.NewMockJoinGroupResponse(t).SetError(sarama.ErrConsumerCoordinatorNotAvailable)
	join := sarama.NewMockJoinGroupResponse(t).SetGroupProtocol(sarama.RangeBalanceStrategyName)
	// each successful join is followed by a heartbeat failure, which ends the session: an idle member's whole life
	// is joins and session ends. With ConsumerGroupMaxRetry 2 the third failure in total stops it unless joins reset
	// the count.
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(topic, 0, broker.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, group, broker),
		"JoinGroupRequest":  sarama.NewMockSequence(fail, join, fail, join, fail, join, fail, join, join),
		"SyncGroupRequest":  sarama.NewMockSyncGroupResponse(t),
		"HeartbeatRequest":  sarama.NewMockHeartbeatResponse(t).SetError(sarama.ErrUnknownMemberId),
		"LeaveGroupRequest": sarama.NewMockLeaveGroupResponse(t),
	})

	cfg := mockGroupConfig(broker)
	cfg.ConsumerGroupMaxRetry = 2
	cfg.ConsumerHeartbeatInterval = 20 * time.Millisecond
	consumer, err := kafkaclient.NewConsumerGroup(cfg, group, topic, false,
		func(context.Context, *sarama.ConsumerMessage) error { return nil }, mockGroupCompatible)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- consumer.Run(ctx) }()

	select {
	case err := <-stopped:
		t.Fatalf("idle member stopped after failures spread over several sessions: %v", err)
	case <-time.After(12 * time.Second):
	}
	assert.True(t, consumer.IsRunning())
}

// Stopping a consumer while it waits to retry a failed join must not start another attempt: sarama refreshes
// metadata before it looks at the context, so that attempt talks to the brokers that just failed and holds the
// shutdown up.
func TestConsumerGroup_StopDuringRetryBackoffDoesNotRejoin(t *testing.T) {
	const topic, group = "orders", "orders-group"

	broker := newMockGroupBroker(t, topic, group,
		sarama.NewMockJoinGroupResponse(t).SetError(sarama.ErrConsumerCoordinatorNotAvailable), "order-1")

	consumer, err := kafkaclient.NewConsumerGroup(mockGroupConfig(broker), group, topic, false,
		func(context.Context, *sarama.ConsumerMessage) error { return nil }, mockGroupCompatible)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- consumer.Run(ctx) }()

	// the first retry waits 1s after the failed join: cancel inside that wait
	require.Eventually(t, func() bool { return countRequests(broker, "*sarama.JoinGroupRequest") == 1 },
		10*time.Second, 10*time.Millisecond, "first join attempted")
	time.Sleep(200 * time.Millisecond)
	metadataBefore := countRequests(broker, "*sarama.MetadataRequest")
	cancel()

	select {
	case err := <-stopped:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop")
	}
	assert.Equal(t, metadataBefore, countRequests(broker, "*sarama.MetadataRequest"), "a stopped consumer started another join attempt")
}

func countRequests(broker *sarama.MockBroker, requestType string) int {
	n := 0
	for _, rr := range broker.History() {
		if fmt.Sprintf("%T", rr.Request) == requestType {
			n++
		}
	}
	return n
}
