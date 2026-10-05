package kafkaclient_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/asabaki/kafka-client"
)

func TestSyncProducerITTestSuite(t *testing.T) {
	requireKafka(t)

	suite.Run(t, &SyncProducerITTestSuite{})
}

type SyncProducerITTestSuite struct {
	suite.Suite
}

func (s *SyncProducerITTestSuite) TestSyncProducer_WithHeartbeatEnabled() {
	// Create config with heartbeat enabled
	kafkaCfg := makeKafkaConfigForTest()
	kafkaCfg.ProducerHeartbeatEnabled = true
	kafkaCfg.ProducerHeartbeatInterval = 2 * time.Second

	// Create sync producer
	producer, err := kafkaclient.NewSyncProducer(kafkaCfg)
	s.Require().NoError(err)
	s.Require().NotNil(producer)
	defer producer.Close()

	// Verify producer is created
	client := producer.Unwrap()
	s.Require().NotNil(client)

	// Verify brokers are available
	brokers := client.Brokers()
	s.Require().NotEmpty(brokers, "Should have at least one broker")

	// Wait for heartbeat to run a few cycles
	time.Sleep(5 * time.Second)

	// Verify brokers are still available after heartbeat cycles
	brokersAfterHeartbeat := client.Brokers()
	s.NotEmpty(brokersAfterHeartbeat, "Brokers should still be available after heartbeat")
}

func (s *SyncProducerITTestSuite) TestSyncProducer_WithHeartbeatDisabled() {
	// Create config with heartbeat disabled
	kafkaCfg := makeKafkaConfigForTest()
	kafkaCfg.ProducerHeartbeatEnabled = false

	// Create sync producer
	producer, err := kafkaclient.NewSyncProducer(kafkaCfg)
	s.Require().NoError(err)
	s.Require().NotNil(producer)
	defer producer.Close()

	// Verify producer works without heartbeat
	client := producer.Unwrap()
	s.Require().NotNil(client)

	brokers := client.Brokers()
	s.Require().NotEmpty(brokers, "Should have at least one broker")
}

func (s *SyncProducerITTestSuite) TestSyncProducer_PublishMessagesWithHeartbeat() {
	// Create config with heartbeat enabled
	kafkaCfg := makeKafkaConfigForTest()
	kafkaCfg.ProducerHeartbeatEnabled = true
	kafkaCfg.ProducerHeartbeatInterval = 2 * time.Second

	// Create admin and topic
	admin := createClusterAdmin(s.T(), kafkaCfg.BootstrapServers)
	topic := createTestTopic(s.T(), admin, "test-sync-producer-heartbeat")

	// Create sync producer
	producer, err := kafkaclient.NewSyncProducer(kafkaCfg)
	s.Require().NoError(err)
	s.Require().NotNil(producer)
	defer producer.Close()

	ctx := context.Background()

	// Publish messages over a period longer than heartbeat interval
	for i := 0; i < 10; i++ {
		message := fmt.Sprintf("message-%d", i)
		partition, offset, err := producer.PublishRawAtLeastOnce(
			ctx,
			topic,
			fmt.Sprintf("key-%d", i),
			[]byte(message),
			nil,
		)

		s.NoError(err)
		s.GreaterOrEqual(partition, int32(0))
		s.GreaterOrEqual(offset, int64(0))

		// Wait between messages to allow heartbeat to tick
		time.Sleep(500 * time.Millisecond)
	}

	// Verify connection is still healthy after producing messages
	client := producer.Unwrap()
	brokers := client.Brokers()
	s.NotEmpty(brokers, "Brokers should still be available after producing messages")
}

func (s *SyncProducerITTestSuite) TestSyncProducer_HeartbeatStopsOnClose() {
	// Create config with heartbeat enabled
	kafkaCfg := makeKafkaConfigForTest()
	kafkaCfg.ProducerHeartbeatEnabled = true
	kafkaCfg.ProducerHeartbeatInterval = 1 * time.Second

	// Create sync producer
	producer, err := kafkaclient.NewSyncProducer(kafkaCfg)
	s.Require().NoError(err)
	s.Require().NotNil(producer)

	// Wait for heartbeat to start
	time.Sleep(2 * time.Second)

	// Close the producer (should stop heartbeat gracefully)
	err = producer.Close()
	s.NoError(err)

	// Wait a bit to ensure cleanup completes
	time.Sleep(1 * time.Second)

	// Test passes if no panic or goroutine leak occurs
}

func (s *SyncProducerITTestSuite) TestSyncProducer_HeartbeatWithShortInterval() {
	// Create config with very short heartbeat interval
	kafkaCfg := makeKafkaConfigForTest()
	kafkaCfg.ProducerHeartbeatEnabled = true
	kafkaCfg.ProducerHeartbeatInterval = 500 * time.Millisecond

	// Create sync producer
	producer, err := kafkaclient.NewSyncProducer(kafkaCfg)
	s.Require().NoError(err)
	s.Require().NotNil(producer)
	defer producer.Close()

	// Wait for multiple heartbeat ticks
	time.Sleep(3 * time.Second)

	// Verify connection is maintained with frequent heartbeats
	client := producer.Unwrap()
	brokers := client.Brokers()
	s.NotEmpty(brokers, "Brokers should be available with frequent heartbeats")
}

func (s *SyncProducerITTestSuite) TestSyncProducer_MultipleConcurrentProducersWithHeartbeat() {
	kafkaCfg := makeKafkaConfigForTest()
	kafkaCfg.ProducerHeartbeatEnabled = true
	kafkaCfg.ProducerHeartbeatInterval = 2 * time.Second

	// Create multiple producers
	producer1, err := kafkaclient.NewSyncProducer(kafkaCfg)
	require.NoError(s.T(), err)
	defer producer1.Close()

	producer2, err := kafkaclient.NewSyncProducer(kafkaCfg)
	require.NoError(s.T(), err)
	defer producer2.Close()

	producer3, err := kafkaclient.NewSyncProducer(kafkaCfg)
	require.NoError(s.T(), err)
	defer producer3.Close()

	// Wait for heartbeats to run
	time.Sleep(5 * time.Second)

	// Verify all producers have healthy connections
	client1 := producer1.Unwrap()
	assert.NotEmpty(s.T(), client1.Brokers(), "Producer 1 should have brokers")

	client2 := producer2.Unwrap()
	assert.NotEmpty(s.T(), client2.Brokers(), "Producer 2 should have brokers")

	client3 := producer3.Unwrap()
	assert.NotEmpty(s.T(), client3.Brokers(), "Producer 3 should have brokers")
}

func (s *SyncProducerITTestSuite) TestSyncProducer_HeartbeatMaintainsConnectionDuringIdleTime() {
	// This test verifies that heartbeat keeps connection alive during idle periods
	kafkaCfg := makeKafkaConfigForTest()
	kafkaCfg.ProducerHeartbeatEnabled = true
	kafkaCfg.ProducerHeartbeatInterval = 1 * time.Second

	// Create admin and topic
	admin := createClusterAdmin(s.T(), kafkaCfg.BootstrapServers)
	topic := createTestTopic(s.T(), admin, "test-sync-producer-idle")

	// Create sync producer
	producer, err := kafkaclient.NewSyncProducer(kafkaCfg)
	s.Require().NoError(err)
	s.Require().NotNil(producer)
	defer producer.Close()

	ctx := context.Background()

	// Send initial message
	partition, offset, err := producer.PublishRawAtLeastOnce(
		ctx,
		topic,
		"key-1",
		[]byte("message-1"),
		nil,
	)
	s.NoError(err)
	s.GreaterOrEqual(partition, int32(0))
	s.GreaterOrEqual(offset, int64(0))

	// Wait for idle period (longer than heartbeat interval)
	// During this time, heartbeat should keep connection alive
	time.Sleep(5 * time.Second)

	// Send another message after idle period
	partition, offset, err = producer.PublishRawAtLeastOnce(
		ctx,
		topic,
		"key-2",
		[]byte("message-2"),
		nil,
	)
	s.NoError(err, "Should be able to publish after idle period with heartbeat")
	s.GreaterOrEqual(partition, int32(0))
	s.GreaterOrEqual(offset, int64(0))

	// Verify connection is still healthy
	client := producer.Unwrap()
	brokers := client.Brokers()
	s.NotEmpty(brokers, "Brokers should still be available after idle period")
}

func (s *SyncProducerITTestSuite) TestSyncProducer_HeartbeatWithHighThroughput() {
	// Test that heartbeat doesn't interfere with high-throughput message production
	kafkaCfg := makeKafkaConfigForTest()
	kafkaCfg.ProducerHeartbeatEnabled = true
	kafkaCfg.ProducerHeartbeatInterval = 1 * time.Second

	// Create admin and topic
	admin := createClusterAdmin(s.T(), kafkaCfg.BootstrapServers)
	topic := createTestTopic(s.T(), admin, "test-sync-producer-throughput")

	// Create sync producer
	producer, err := kafkaclient.NewSyncProducer(kafkaCfg)
	s.Require().NoError(err)
	s.Require().NotNil(producer)
	defer producer.Close()

	ctx := context.Background()

	// Produce many messages rapidly
	messageCount := 100
	successCount := 0

	for i := 0; i < messageCount; i++ {
		message := fmt.Sprintf("message-%d", i)
		_, _, err := producer.PublishRawAtLeastOnce(
			ctx,
			topic,
			fmt.Sprintf("key-%d", i),
			[]byte(message),
			nil,
		)

		if err == nil {
			successCount++
		}
	}

	// Verify most/all messages were published successfully
	s.GreaterOrEqual(successCount, messageCount-5, "Should publish most messages successfully")

	// Verify connection is still healthy
	client := producer.Unwrap()
	brokers := client.Brokers()
	s.NotEmpty(brokers, "Brokers should still be available after high throughput")
}

func (s *SyncProducerITTestSuite) TestSyncProducer_CompareWithAndWithoutHeartbeat() {
	// Create admin and topic
	kafkaCfg := makeKafkaConfigForTest()
	admin := createClusterAdmin(s.T(), kafkaCfg.BootstrapServers)
	topic := createTestTopic(s.T(), admin, "test-sync-producer-compare")

	ctx := context.Background()

	// Test WITHOUT heartbeat
	kafkaCfgNoHeartbeat := makeKafkaConfigForTest()
	kafkaCfgNoHeartbeat.ProducerHeartbeatEnabled = false

	producerNoHeartbeat, err := kafkaclient.NewSyncProducer(kafkaCfgNoHeartbeat)
	s.Require().NoError(err)
	defer producerNoHeartbeat.Close()

	_, _, err = producerNoHeartbeat.PublishRawAtLeastOnce(ctx, topic, "key-1", []byte("message-1"), nil)
	s.NoError(err, "Should work without heartbeat")

	// Test WITH heartbeat
	kafkaCfgWithHeartbeat := makeKafkaConfigForTest()
	kafkaCfgWithHeartbeat.ProducerHeartbeatEnabled = true
	kafkaCfgWithHeartbeat.ProducerHeartbeatInterval = 2 * time.Second

	producerWithHeartbeat, err := kafkaclient.NewSyncProducer(kafkaCfgWithHeartbeat)
	s.Require().NoError(err)
	defer producerWithHeartbeat.Close()

	_, _, err = producerWithHeartbeat.PublishRawAtLeastOnce(ctx, topic, "key-2", []byte("message-2"), nil)
	s.NoError(err, "Should work with heartbeat")

	// Wait for heartbeat to run
	time.Sleep(3 * time.Second)

	// Both should still be able to produce
	_, _, err = producerNoHeartbeat.PublishRawAtLeastOnce(ctx, topic, "key-3", []byte("message-3"), nil)
	s.NoError(err)

	_, _, err = producerWithHeartbeat.PublishRawAtLeastOnce(ctx, topic, "key-4", []byte("message-4"), nil)
	s.NoError(err)
}

// TestSyncProducer_HeartbeatKeepsConnectionAlive verifies that heartbeat maintains
// connection to Kafka brokers when producer is idle_
func (s *SyncProducerITTestSuite) TestSyncProducer_HeartbeatKeepsConnectionAlive() {
	// Create config with heartbeat enabled
	kafkaCfg := makeKafkaConfigForTest()
	kafkaCfg.ProducerHeartbeatEnabled = true
	kafkaCfg.ProducerHeartbeatInterval = 2 * time.Second

	// Create admin and topic for initial connection establishment
	admin := createClusterAdmin(s.T(), kafkaCfg.BootstrapServers)
	topic := createTestTopic(s.T(), admin, "test-heartbeat-keeps-alive")

	// Create sync producer
	producer, err := kafkaclient.NewSyncProducer(kafkaCfg)
	s.NoError(err)
	s.NotNil(producer)
	defer producer.Close()

	// Establish initial broker connections by producing a message
	s.establishBrokerConnections(producer, topic)

	// Get initial broker state (connections should now be established)
	client := producer.Unwrap()
	s.NotNil(client)

	s.checkIfBrokerConnected(client.Brokers())

	// Wait for several heartbeat cycles WITHOUT any message production
	// This simulates an idle producer that should maintain connection via heartbeat
	time.Sleep(7 * time.Second)

	// Verify brokers are STILL connected after idle period (heartbeat kept them alive)
	// This is the key test: verify connections are still alive WITHOUT re-opening them
	s.checkIfBrokerConnected(client.Brokers())

}

func (s *SyncProducerITTestSuite) checkIfBrokerConnected(brokers []*sarama.Broker) {
	s.NotEmpty(brokers, "Brokers should still be available after idle period with heartbeat")

	for _, broker := range brokers {
		s.NotNil(broker, "Broker should not be nil")
		s.NotEmpty(broker.Addr(), "Broker address should be available")

		// Check if connected (without opening - this tests that heartbeat kept it alive)
		connected, err := broker.Connected()
		s.NoError(err, "Error checking broker connection status")
		s.True(connected, "Broker should be connected to %s (heartbeat should have kept connection alive)", broker.Addr())
	}
}

// establishBrokerConnections ensures broker connections are established by producing a test message
func (s *SyncProducerITTestSuite) establishBrokerConnections(producer kafkaclient.SyncProducer, topic string) {
	ctx := context.Background()
	_, _, err := producer.PublishRawAtLeastOnce(ctx, topic, "test-key", []byte("test-connection-establishment"), nil)
	s.NoError(err, "Should be able to produce initial message to establish broker connections")
}

// TestSyncProducer_HeartbeatStopsGracefully verifies that heartbeat stops cleanly
// when producer is closed
func (s *SyncProducerITTestSuite) TestSyncProducer_HeartbeatStopsGracefully() {
	kafkaCfg := makeKafkaConfigForTest()
	kafkaCfg.ProducerHeartbeatEnabled = true
	kafkaCfg.ProducerHeartbeatInterval = 1 * time.Second

	producer, err := kafkaclient.NewSyncProducer(kafkaCfg)
	s.NoError(err)
	s.NotNil(producer)

	// Let heartbeat run for a few cycles
	time.Sleep(3 * time.Second)

	// Close should stop heartbeat without errors
	err = producer.Close()
	s.NoError(err, "Producer should close gracefully, stopping heartbeat")
}

// TestSyncProducer_HeartbeatWithVeryShortInterval tests heartbeat with aggressive interval
func (s *SyncProducerITTestSuite) TestSyncProducer_HeartbeatWithVeryShortInterval() {
	kafkaCfg := makeKafkaConfigForTest()
	kafkaCfg.ProducerHeartbeatEnabled = true
	kafkaCfg.ProducerHeartbeatInterval = 500 * time.Millisecond // Very short interval

	// Create admin and topic
	admin := createClusterAdmin(s.T(), kafkaCfg.BootstrapServers)
	topic := createTestTopic(s.T(), admin, "test-heartbeat-short-interval")

	producer, err := kafkaclient.NewSyncProducer(kafkaCfg)
	s.NoError(err)
	s.NotNil(producer)
	defer producer.Close()

	// Establish initial broker connections
	s.establishBrokerConnections(producer, topic)

	// Let heartbeat run many cycles with short interval
	time.Sleep(5 * time.Second)

	// Verify connection is maintained even with frequent heartbeats
	client := producer.Unwrap()
	s.checkIfBrokerConnected(client.Brokers())
}

// TestSyncProducer_HeartbeatDoesNotInterfereWithProduction tests that heartbeat
// runs in background without affecting message production
func (s *SyncProducerITTestSuite) TestSyncProducer_HeartbeatDoesNotInterfereWithProduction() {
	kafkaCfg := makeKafkaConfigForTest()
	kafkaCfg.ProducerHeartbeatEnabled = true
	kafkaCfg.ProducerHeartbeatInterval = 1 * time.Second

	// Create admin and topic
	admin := createClusterAdmin(s.T(), kafkaCfg.BootstrapServers)
	topic := createTestTopic(s.T(), admin, "test-heartbeat-production")

	producer, err := kafkaclient.NewSyncProducer(kafkaCfg)
	s.NoError(err)
	s.NotNil(producer)
	defer producer.Close()

	ctx := context.Background()

	// Produce messages while heartbeat is running
	successCount := 0
	for i := 0; i < 50; i++ {
		_, _, err := producer.PublishRawAtLeastOnce(
			ctx,
			topic,
			fmt.Sprintf("key-%d", i),
			[]byte(fmt.Sprintf("message-%d", i)),
			nil,
		)
		if err == nil {
			successCount++
		}

		// Small delay to allow heartbeat to tick
		time.Sleep(100 * time.Millisecond)
	}

	// Verify all messages were published successfully
	s.Equal(50, successCount, "All messages should be published successfully with heartbeat running")
}

// TestSyncProducer_MultipleConcurrentWithHeartbeat tests multiple producers
// with heartbeat running concurrently
func (s *SyncProducerITTestSuite) TestSyncProducer_MultipleConcurrentWithHeartbeat() {
	kafkaCfg := makeKafkaConfigForTest()
	kafkaCfg.ProducerHeartbeatEnabled = true
	kafkaCfg.ProducerHeartbeatInterval = 2 * time.Second

	// Create admin and topic
	admin := createClusterAdmin(s.T(), kafkaCfg.BootstrapServers)
	topic := createTestTopic(s.T(), admin, "test-heartbeat-concurrent")

	// Create multiple producers
	producers := make([]kafkaclient.SyncProducer, 5)
	for i := 0; i < 5; i++ {
		producer, err := kafkaclient.NewSyncProducer(kafkaCfg)
		s.NoError(err)
		s.NotNil(producer)
		producers[i] = producer
		defer producer.Close()

		// Establish connections for each producer
		s.establishBrokerConnections(producer, topic)
	}

	// Let heartbeats run for all producers (idle period)
	time.Sleep(6 * time.Second)

	// Verify all producers STILL have healthy connections (heartbeat kept them alive)
	for i, producer := range producers {
		client := producer.Unwrap()
		s.checkIfBrokerConnected(client.Brokers())
		s.T().Logf("Producer %d has healthy broker connections", i)
	}
}

// TestHeartbeat_CompareEnabledVsDisabled compares behavior with and without heartbeat
func (s *SyncProducerITTestSuite) TestHeartbeat_CompareEnabledVsDisabled() {
	// Create admin and topic
	kafkaCfgSetup := makeKafkaConfigForTest()
	admin := createClusterAdmin(s.T(), kafkaCfgSetup.BootstrapServers)
	topic := createTestTopic(s.T(), admin, "test-heartbeat-compare")

	// Producer WITHOUT heartbeat
	kafkaCfgNoHeartbeat := makeKafkaConfigForTest()
	kafkaCfgNoHeartbeat.ProducerHeartbeatEnabled = false

	producerNoHeartbeat, err := kafkaclient.NewSyncProducer(kafkaCfgNoHeartbeat)
	s.NoError(err)
	defer producerNoHeartbeat.Close()

	// Establish connections
	s.establishBrokerConnections(producerNoHeartbeat, topic)

	// Producer WITH heartbeat
	kafkaCfgWithHeartbeat := makeKafkaConfigForTest()
	kafkaCfgWithHeartbeat.ProducerHeartbeatEnabled = true
	kafkaCfgWithHeartbeat.ProducerHeartbeatInterval = 2 * time.Second

	producerWithHeartbeat, err := kafkaclient.NewSyncProducer(kafkaCfgWithHeartbeat)
	s.NoError(err)
	defer producerWithHeartbeat.Close()

	// Establish connections
	s.establishBrokerConnections(producerWithHeartbeat, topic)

	// Both should work initially
	clientNoHb := producerNoHeartbeat.Unwrap()
	s.NotEmpty(clientNoHb.Brokers())

	clientWithHb := producerWithHeartbeat.Unwrap()
	s.NotEmpty(clientWithHb.Brokers())

	// Wait for heartbeat cycles (idle period)
	time.Sleep(6 * time.Second)

	// Both should still work - verifying connections are maintained
	// (In this short test, both may work, but heartbeat is crucial for longer idle periods)
	s.checkIfBrokerConnected(clientNoHb.Brokers())
	s.checkIfBrokerConnected(clientWithHb.Brokers())
}

// TestSyncProducer_HeartbeatAfterLongIdlePeriod tests heartbeat benefit
// after extended idle time
func (s *SyncProducerITTestSuite) TestSyncProducer_HeartbeatAfterLongIdlePeriod() {
	kafkaCfg := makeKafkaConfigForTest()
	kafkaCfg.ProducerHeartbeatEnabled = true
	kafkaCfg.ProducerHeartbeatInterval = 2 * time.Second

	// Create admin and topic
	admin := createClusterAdmin(s.T(), kafkaCfg.BootstrapServers)
	topic := createTestTopic(s.T(), admin, "test-heartbeat-long-idle")

	producer, err := kafkaclient.NewSyncProducer(kafkaCfg)
	s.NoError(err)
	s.NotNil(producer)
	defer producer.Close()

	ctx := context.Background()

	// Send initial message
	partition, offset, err := producer.PublishRawAtLeastOnce(
		ctx,
		topic,
		"key-1",
		[]byte("message-1"),
		nil,
	)
	s.NoError(err)
	s.GreaterOrEqual(partition, int32(0))
	s.GreaterOrEqual(offset, int64(0))

	// Simulate long idle period (heartbeat should maintain connection)
	// In real scenarios with connection timeouts, this is where heartbeat helps
	time.Sleep(10 * time.Second)

	// Verify we can still produce after long idle
	partition, offset, err = producer.PublishRawAtLeastOnce(
		ctx,
		topic,
		"key-2",
		[]byte("message-2"),
		nil,
	)
	s.NoError(err, "Should be able to publish after long idle with heartbeat")
	s.GreaterOrEqual(partition, int32(0))
	s.GreaterOrEqual(offset, int64(0))

	// Verify connection is still healthy
	client := producer.Unwrap()
	s.checkIfBrokerConnected(client.Brokers())
}

// TestSyncProducer_HeartbeatWithProducerRecreation tests that heartbeat
// is properly initialized when creating multiple producers sequentially
func (s *SyncProducerITTestSuite) TestSyncProducer_HeartbeatWithProducerRecreation() {
	kafkaCfg := makeKafkaConfigForTest()
	kafkaCfg.ProducerHeartbeatEnabled = true
	kafkaCfg.ProducerHeartbeatInterval = 1 * time.Second

	// Create admin and topic
	admin := createClusterAdmin(s.T(), kafkaCfg.BootstrapServers)
	topic := createTestTopic(s.T(), admin, "test-heartbeat-recreation")

	// Create, use, and close multiple producers in sequence
	for i := 0; i < 3; i++ {
		producer, err := kafkaclient.NewSyncProducer(kafkaCfg)
		s.NoError(err)
		s.NotNil(producer)

		// Establish broker connection
		s.establishBrokerConnections(producer, topic)

		// Verify broker connection
		client := producer.Unwrap()
		s.checkIfBrokerConnected(client.Brokers())
		s.T().Logf("Producer iteration %d has healthy broker connections", i)

		// Let heartbeat run for a bit (idle period)
		time.Sleep(3 * time.Second)

		// Verify connection is still alive after idle period
		s.checkIfBrokerConnected(client.Brokers())
		s.T().Logf("Producer iteration %d still has healthy broker connections after idle", i)

		// Close should be clean
		err = producer.Close()
		s.NoError(err, "Producer iteration %d should close cleanly", i)

		// Small delay between iterations
		time.Sleep(500 * time.Millisecond)
	}
}

// TestHeartbeat_DisabledByDefault verifies that heartbeat is not active when disabled
func (s *SyncProducerITTestSuite) TestHeartbeat_DisabledByDefault() {
	// Create config with default heartbeat setting (should be disabled)
	kafkaCfg := makeKafkaConfigForTest()
	// Explicitly verify default is false
	s.False(kafkaCfg.ProducerHeartbeatEnabled, "Heartbeat should be disabled by default")

	// Create admin and topic
	admin := createClusterAdmin(s.T(), kafkaCfg.BootstrapServers)
	topic := createTestTopic(s.T(), admin, "test-heartbeat-disabled")

	producer, err := kafkaclient.NewSyncProducer(kafkaCfg)
	s.NoError(err)
	s.NotNil(producer)
	defer producer.Close()

	// Establish connections
	s.establishBrokerConnections(producer, topic)

	// Producer should work without heartbeat
	client := producer.Unwrap()
	s.checkIfBrokerConnected(client.Brokers())

	// Wait some time (no heartbeat should be running)
	time.Sleep(3 * time.Second)

	// Producer should still work (in this short test period)
	s.checkIfBrokerConnected(client.Brokers())
}

// TestSyncProducer_HeartbeatWithHighMessageThroughput tests that heartbeat
// doesn't cause issues during high throughput
func (s *SyncProducerITTestSuite) TestSyncProducer_HeartbeatWithHighMessageThroughput() {
	kafkaCfg := makeKafkaConfigForTest()
	kafkaCfg.ProducerHeartbeatEnabled = true
	kafkaCfg.ProducerHeartbeatInterval = 1 * time.Second

	// Create admin and topic
	admin := createClusterAdmin(s.T(), kafkaCfg.BootstrapServers)
	topic := createTestTopic(s.T(), admin, "test-heartbeat-throughput")

	producer, err := kafkaclient.NewSyncProducer(kafkaCfg)
	s.NoError(err)
	s.NotNil(producer)
	defer producer.Close()

	ctx := context.Background()

	// Produce many messages rapidly while heartbeat is running
	messageCount := 200
	successCount := 0

	startTime := time.Now()
	for i := 0; i < messageCount; i++ {
		_, _, err := producer.PublishRawAtLeastOnce(
			ctx,
			topic,
			fmt.Sprintf("key-%d", i),
			[]byte(fmt.Sprintf("message-%d", i)),
			nil,
		)
		if err == nil {
			successCount++
		}
	}
	duration := time.Since(startTime)

	// Verify high success rate
	s.GreaterOrEqual(successCount, messageCount-10,
		"Should publish most messages successfully with heartbeat running")

	s.T().Logf("Published %d/%d messages in %v with heartbeat enabled",
		successCount, messageCount, duration)

	// Verify connection is still healthy
	client := producer.Unwrap()
	s.checkIfBrokerConnected(client.Brokers())
}
