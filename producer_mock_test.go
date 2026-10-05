package kafkaclient_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	kafkaclient "github.com/asabaki/kafka-client"
)

// newMockCluster starts a sarama MockBroker leading `partitions` partitions of topic. produce configures produce
// responses (nil = always succeed).
func newMockCluster(t *testing.T, topic string, partitions int32, produce *sarama.MockProduceResponse) (*sarama.MockBroker, kafkaclient.KafkaConfig) {
	t.Helper()

	broker := sarama.NewMockBroker(t, 1)
	t.Cleanup(broker.Close)

	metadata := sarama.NewMockMetadataResponse(t).SetBroker(broker.Addr(), broker.BrokerID())
	for p := int32(0); p < partitions; p++ {
		metadata.SetLeader(topic, p, broker.BrokerID())
	}
	if produce == nil {
		produce = sarama.NewMockProduceResponse(t)
	}
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": metadata,
		"ProduceRequest":  produce,
	})

	return broker, kafkaclient.KafkaConfig{
		BootstrapServers:     []string{broker.Addr()},
		ProducerRequiredAcks: -1,
		ProducerTimeout:      time.Second,
		MetricsDisabled:      true,
	}
}

func TestSyncProducer_ContextDeadline(t *testing.T) {
	broker, cfg := newMockCluster(t, "orders", 1, nil)

	producer, err := kafkaclient.NewSyncProducer(cfg, mockBrokerCompatible)
	require.NoError(t, err)
	defer producer.Close()

	// warm up the connection, then make the broker slow
	_, _, err = producer.PublishRawAtLeastOnce(context.Background(), "orders", "k", []byte("v"), nil)
	require.NoError(t, err)
	broker.SetLatency(2 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, _, err = producer.PublishRawAtLeastOnce(ctx, "orders", "k", []byte("v"), nil)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), time.Second, "returns at the deadline, not when the broker answers")
}

func TestSyncProducer_CanceledContextNeverSends(t *testing.T) {
	broker, cfg := newMockCluster(t, "orders", 1, nil)

	producer, err := kafkaclient.NewSyncProducer(cfg, mockBrokerCompatible)
	require.NoError(t, err)
	defer producer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = producer.PublishRawAtLeastOnce(ctx, "orders", "k", []byte("v"), nil)
	assert.ErrorIs(t, err, context.Canceled)

	for _, rr := range broker.History() {
		_, isProduce := rr.Request.(*sarama.ProduceRequest)
		assert.False(t, isProduce, "a canceled publish must not reach the broker")
	}
}

func TestSyncProducer_CloseWaitsForInFlight(t *testing.T) {
	broker, cfg := newMockCluster(t, "orders", 1, nil)
	broker.SetLatency(300 * time.Millisecond)

	producer, err := kafkaclient.NewSyncProducer(cfg, mockBrokerCompatible)
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make([]error, 5)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, errs[i] = producer.PublishRawAtLeastOnce(context.Background(), "orders", "k", []byte("v"), nil)
		}(i)
	}
	time.Sleep(50 * time.Millisecond) // all five are in flight
	require.NoError(t, producer.Close())
	wg.Wait()

	for _, err := range errs {
		assert.NoError(t, err, "in-flight publishes complete before Close returns")
	}
}

func TestAsyncProducer_ContextEndsWhileQueueFullReportsError(t *testing.T) {
	broker, cfg := newMockCluster(t, "orders", 1, nil)
	broker.SetLatency(time.Second)

	var mu sync.Mutex
	var reported []error
	producer, err := kafkaclient.NewAsyncProducer(cfg, mockBrokerCompatible,
		kafkaclient.WithSaramaConfigHookForTest(func(c *sarama.Config) {
			c.ApiVersionsRequest = false
			c.Version = sarama.V2_8_0_0
			c.ChannelBufferSize = 1 // tiny input queue, so publishing blocks
			c.Producer.Flush.MaxMessages = 1
		}),
		kafkaclient.WithAsyncErrorHandler(func(pe *sarama.ProducerError) {
			mu.Lock()
			reported = append(reported, pe.Err)
			mu.Unlock()
		}),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	for i := 0; i < 200; i++ {
		producer.PublishRawAtMostOnce(ctx, "orders", "k", []byte("v"), nil)
	}
	assert.Less(t, time.Since(start), 2*time.Second, "publishing stops blocking once ctx ends")
	require.NoError(t, producer.Close())

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, reported)
	for _, err := range reported {
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "unexpected error %v", err)
	}
}

func TestProducerMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	_, cfg := newMockCluster(t, "orders", 1, sarama.NewMockProduceResponse(t).SetError("orders", 0, sarama.ErrNotEnoughReplicas))
	cfg.ProducerRetryMax = 0

	sync, err := kafkaclient.NewSyncProducer(cfg, mockBrokerCompatible)
	require.NoError(t, err)
	_, _, err = sync.PublishRawAtLeastOnce(context.Background(), "orders", "k", []byte("v"), nil)
	require.ErrorIs(t, err, sarama.ErrNotEnoughReplicas)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _ = sync.PublishRawAtLeastOnce(canceled, "orders", "k", []byte("v"), nil)
	require.NoError(t, sync.Close())

	async, err := kafkaclient.NewAsyncProducer(cfg, mockBrokerCompatible)
	require.NoError(t, err)
	async.PublishRawAtMostOnce(context.Background(), "orders", "k", []byte("v"), nil)
	require.NoError(t, async.Close())

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	counts := map[string]int64{}
	var durations uint64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				if m.Name != "kafka_client_producer_messages" {
					continue
				}
				for _, dp := range data.DataPoints {
					producer, _ := dp.Attributes.Value("producer")
					result, _ := dp.Attributes.Value("result")
					topic, _ := dp.Attributes.Value("topic")
					assert.Equal(t, "orders", topic.AsString())
					counts[producer.AsString()+"/"+result.AsString()] += dp.Value
				}
			case metricdata.Histogram[int64]:
				if m.Name == "kafka_client_producer_publish_duration" {
					for _, dp := range data.DataPoints {
						durations += dp.Count
					}
				}
			}
		}
	}

	assert.Equal(t, map[string]int64{"sync/error": 1, "sync/canceled": 1, "async/error": 1}, counts)
	assert.Equal(t, uint64(2), durations, "one duration per sync publish")
}
