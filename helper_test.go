package kafkaclient_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"

	"github.com/asabaki/kafka-client"
)

var processID = fmt.Sprintf("%d", os.Getpid())
var num syncNumber

type syncNumber struct {
	num  int
	lock sync.Mutex
}

func (r *syncNumber) GetNextNumber() int {
	r.lock.Lock()
	r.num++
	r.lock.Unlock()
	return r.num
}

func getNextSequence() string {
	return getNextSequenceWithFormat("%s%05d")
}

func getNextSequenceWithFormat(format string) string {
	return fmt.Sprintf(format, processID, num.GetNextNumber())
}

// requireKafka skips an integration test when no broker is reachable (or with -short), so `go test ./...` works
// without Kafka. Set KAFKA_REQUIRED=1 (as CI does) to fail instead of skipping.
func requireKafka(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("integration test: skipped with -short")
	}

	addr := getKafkaBootstrapServers()[0]
	conn, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(context.Background(), "tcp", addr)
	if err == nil {
		_ = conn.Close()
		return
	}
	if os.Getenv("KAFKA_REQUIRED") != "" {
		t.Fatalf("integration test: Kafka not reachable at %s: %s", addr, err)
	}
	t.Skipf("integration test: Kafka not reachable at %s (start it with `docker compose up -d`)", addr)
}

func getKafkaBootstrapServers() []string {
	host := "localhost:9092"
	if bs := os.Getenv("KAFKA_BOOTSTRAP_SERVERS"); bs != "" {
		host = bs
	}

	return []string{host}
}

func createSaramaConfigForTest() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Flush.Messages = 1
	cfg.Producer.Flush.MaxMessages = 1

	return cfg
}

func makeKafkaConfigForTest() kafkaclient.KafkaConfig {
	return kafkaclient.KafkaConfig{
		BootstrapServers:      getKafkaBootstrapServers(),
		MetricsDisabled:       true,
		ConsumerGroupMaxRetry: 3,
		ProducerTimeout:       5 * time.Second,
	}
}
func createProducerForTest(t *testing.T, bootstrapServers []string) sarama.SyncProducer {
	p, err := sarama.NewSyncProducer(bootstrapServers, createSaramaConfigForTest())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = p.Close()
	})

	return p
}

func createClusterAdmin(t *testing.T, bootstrapServers []string) sarama.ClusterAdmin {
	admin, err := sarama.NewClusterAdmin(bootstrapServers, sarama.NewConfig())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = admin.Close
	})

	return admin
}

func createTestTopic(t *testing.T, admin sarama.ClusterAdmin, topicPrefix string) string {
	return createTestTopicWithPartitions(t, admin, topicPrefix, 1)
}

func createTestTopicWithPartitions(t *testing.T, admin sarama.ClusterAdmin, topicPrefix string, partitions int32) string {
	topic := topicPrefix + getNextSequence()
	_ = admin.DeleteTopic(topic)

	err := admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}, false)
	require.NoError(t, err)

	return topic
}
