package kafkagoso_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/justtrackio/gosoline/pkg/application"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/kernel"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/justtrackio/gosoline/pkg/tracing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kafkaclient "github.com/asabaki/kafka-client"
	"github.com/asabaki/kafka-client/kafkagoso"
)

func bootstrapServer() string {
	if bs := os.Getenv("KAFKA_BOOTSTRAP_SERVERS"); bs != "" {
		return bs
	}
	return "localhost:9092"
}

type rawMsg struct{ topic, key, value string }

func (m rawMsg) KafkaTopic() string                   { return m.topic }
func (m rawMsg) KafkaMessageKey() string              { return m.key }
func (m rawMsg) KafkaMessagePayload() ([]byte, error) { return []byte(m.value), nil }

// publisherModule publishes one message through the shared sync producer and then idles until shutdown.
type publisherModule struct {
	kernel.BackgroundModule
	kernel.ApplicationStage

	producer kafkaclient.SyncProducer
	topic    string
}

const publishedTraceId = "goso:0b6e8b3c-1d5b-4a55-9b1f-2e8f2c3a4d5e"

func (m *publisherModule) Run(ctx context.Context) error {
	ctx = tracing.ContextWithTrace(ctx, &tracing.Trace{TraceId: publishedTraceId, Id: "00000000-0000-0000-0000-000000000000"})
	if _, _, err := m.producer.PublishAtLeastOnce(ctx, rawMsg{topic: m.topic, key: "k1", value: "hello"}); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

// requireKafka skips when no broker is reachable (or with -short); KAFKA_REQUIRED=1 fails instead.
func requireKafka(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("integration test: skipped with -short")
	}
	conn, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(context.Background(), "tcp", bootstrapServer())
	if err == nil {
		_ = conn.Close()
		return
	}
	if os.Getenv("KAFKA_REQUIRED") != "" {
		t.Fatalf("integration test: Kafka not reachable at %s: %s", bootstrapServer(), err)
	}
	t.Skipf("integration test: Kafka not reachable at %s (start it with `docker compose up -d`)", bootstrapServer())
}

func TestGosolineApplication(t *testing.T) {
	requireKafka(t)

	topic := fmt.Sprintf("kafkagoso-test-%d", time.Now().UnixNano())
	admin, err := sarama.NewClusterAdmin([]string{bootstrapServer()}, sarama.NewConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })
	require.NoError(t, admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false))

	received := make(chan string, 1)
	receivedTrace := make(chan string, 1)
	receivedHeader := make(chan string, 1)
	var consumerProducer, publisherProducer kafkaclient.SyncProducer
	exitCode := make(chan int, 1)

	ker := application.New(
		application.WithConfigMap(map[string]any{
			"env":         "test",
			"app_project": "kafka-client",
			"app_family":  "test",
			"app_group":   "test",
			"app_name":    "kafkagoso",
			"kafka_client": map[string]any{
				"connection": map[string]any{
					"default": map[string]any{
						"bootstrap_servers":   []string{bootstrapServer()},
						"sasl_enabled":        false,
						"tls_enabled":         false,
						"metrics_disabled":    true,
						"producer_idempotent": false,
						"logging_enabled":     true,
					},
				},
				"consumer": map[string]any{
					"events": map[string]any{
						"topic":    topic,
						"group_id": topic + "-group",
					},
				},
				"producer": map[string]any{
					"events": map[string]any{},
				},
			},
		}),
		application.WithLoggerHandlers(log.NewCliHandler()),
		application.WithKernelExitHandler(func(code int) { exitCode <- code }),
		application.WithModuleFactory("consumer", kafkagoso.NewConsumerModuleFactory("events",
			func(ctx context.Context, config cfg.Config, logger log.Logger) (kafkaclient.MessageHandler[*sarama.ConsumerMessage], error) {
				p, err := kafkagoso.ProvideSyncProducer(ctx, config, logger, "events")
				if err != nil {
					return nil, err
				}
				consumerProducer = p

				return func(ctx context.Context, msg *sarama.ConsumerMessage) error {
					if id := tracing.GetTraceIdFromContext(ctx); id != nil {
						receivedTrace <- *id
					} else {
						receivedTrace <- ""
					}
					for _, h := range msg.Headers {
						if string(h.Key) == kafkagoso.TraceIdHeader {
							receivedHeader <- string(h.Value)
						}
					}
					received <- string(msg.Value)
					return nil
				}, nil
			})),
		application.WithModuleFactory("publisher", func(ctx context.Context, config cfg.Config, logger log.Logger) (kernel.Module, error) {
			p, err := kafkagoso.ProvideSyncProducer(ctx, config, logger, "events")
			if err != nil {
				return nil, err
			}
			publisherProducer = p

			return &publisherModule{producer: p, topic: topic}, nil
		}),
		application.WithModuleFactory("kafka_producer_closer", kafkagoso.NewProducerCloserModuleFactory()),
	)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		ker.Run()
	}()

	select {
	case v := <-received:
		assert.Equal(t, "hello", v)
		assert.Equal(t, "Root="+publishedTraceId+";Parent=00000000-0000-0000-0000-000000000000;Sampled=0", <-receivedHeader)
		assert.Contains(t, <-receivedTrace, "Root="+publishedTraceId+";", "handler ctx must continue the producer's trace")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for message")
	}

	require.NotNil(t, consumerProducer)
	assert.Same(t, consumerProducer, publisherProducer, "producer must be shared per name")

	ker.Stop("test done")

	select {
	case <-runDone:
	case <-time.After(30 * time.Second):
		t.Fatal("kernel did not stop")
	}
	assert.Equal(t, kernel.ExitCodeOk, <-exitCode)
	assert.True(t, publisherProducer.Unwrap().Closed(), "closer module must close shared producers")
}
