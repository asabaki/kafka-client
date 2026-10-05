package kafkaclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
)

var _ ConsumerGroup = (*csmGroup)(nil)

const instrumentationName = "kafka-client"

type csmGroup struct {
	cfg                        KafkaConfig
	logger                     Logger
	topic                      string
	consumerGroupName          string
	saramaConsumerGroupHandler sarama.ConsumerGroupHandler

	client sarama.ConsumerGroup

	mu         sync.Mutex
	running    atomic.Bool
	wg         sync.WaitGroup
	cancelFunc context.CancelFunc
	done       chan struct{}
	runErr     error
}

func createConsumerGroup(
	cfg KafkaConfig,
	topic string,
	consumerGroupName string,
	saramaConsumerGroupHandler sarama.ConsumerGroupHandler,
	ignoreOldMessage bool,
) (*csmGroup, error) {
	saramaCfg := cfg.ToSaramaConfig()
	if ignoreOldMessage {
		saramaCfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	} else {
		saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	// Make unique client id by adding the consumer group id as a suffix.
	saramaCfg.ClientID = fmt.Sprintf("%s-%s", saramaCfg.ClientID, consumerGroupName)

	client, err := sarama.NewConsumerGroup(cfg.BootstrapServers, consumerGroupName, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("error creating consumer group: %w", err)
	}

	logger := cfg.log()
	// Prevent deadlock caused by enabling the return channel
	go receiveUntilClose(client.Errors(), func(e error) {
		logger.Error(context.Background(), "consume message error: topic=%s consumer_group=%s: %s", topic, consumerGroupName, e)
	})

	return &csmGroup{
		cfg:                        cfg,
		logger:                     logger,
		topic:                      topic,
		consumerGroupName:          consumerGroupName,
		saramaConsumerGroupHandler: saramaConsumerGroupHandler,
		client:                     client,
	}, nil
}

func (c *csmGroup) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.IsRunning() {
		return fmt.Errorf("consumer %s %s have already been running", c.topic, c.consumerGroupName)
	}

	ctxWithCancel, cancelFunc := context.WithCancel(context.Background())
	c.cancelFunc = cancelFunc
	c.done = make(chan struct{})
	c.runErr = nil

	c.running.Store(true)
	c.wg.Add(1)
	go func() {
		defer func() {
			cancelFunc()
			c.running.Store(false)
			close(c.done)
			c.wg.Done()
		}()
		c.runErr = c.runSaramaConsumer(ctxWithCancel)
	}()

	return nil
}

// Run starts the consumer and blocks until ctx is canceled or the consumer terminates on its own.
// The consumer is closed before Run returns. The returned error is nil for a ctx-triggered shutdown and
// non-nil if the consumer stopped because it could not (re)connect to Kafka.
//
// Run matches the signature of a gosoline kernel.Module.
func (c *csmGroup) Run(ctx context.Context) error {
	if err := c.Start(); err != nil {
		return err
	}

	c.mu.Lock()
	done := c.done
	c.mu.Unlock()

	select {
	case <-ctx.Done():
	case <-done:
	}

	c.Close()

	return c.runErr
}

func (c *csmGroup) runSaramaConsumer(ctx context.Context) error {
	var retryCounter counter

	handler := saramaConsumerWithRetryCounter{
		handler:      c.saramaConsumerGroupHandler,
		retryCounter: &retryCounter,
	}

	var cgErr error
	for {
		// always check for context.Canceled first
		if err := ctx.Err(); err != nil {
			if !errors.Is(err, context.Canceled) {
				cgErr = err
			} else {
				cgErr = nil
			}
			break
		}
		if cgErr != nil {
			if errors.Is(cgErr, sarama.ErrClosedConsumerGroup) {
				cgErr = nil
				break
			}

			retryCounter.Inc()
			if retryCounter.Count() > c.cfg.ConsumerGroupMaxRetry {
				break
			}
			c.logger.Warn(ctx, "retry connecting to kafka: topic=%s consumer_group=%s retry_count=%d: %s", c.topic, c.consumerGroupName, retryCounter.Count(), cgErr)

			select {
			case <-ctx.Done():
			case <-time.After(time.Duration(retryCounter.Count()) * time.Second):
			}
			continue
		}
		cgErr = c.client.Consume(ctx, []string{c.topic}, &handler)
	}

	if cgErr != nil {
		c.logger.Error(ctx, "consumer stopped: topic=%s consumer_group=%s: %s", c.topic, c.consumerGroupName, cgErr)
		return fmt.Errorf("consumer %s %s stopped: %w", c.topic, c.consumerGroupName, cgErr)
	}

	return nil
}

func (c *csmGroup) Close() {
	c.mu.Lock()
	cancel := c.cancelFunc
	c.mu.Unlock()

	if cancel != nil {
		cancel()
		c.wg.Wait()
	}
	if err := c.client.Close(); err != nil && !errors.Is(err, sarama.ErrClosedConsumerGroup) {
		c.logger.Error(context.Background(), "error closing consumer: topic=%s consumer_group=%s: %s", c.topic, c.consumerGroupName, err)
	}
}

func (c *csmGroup) IsRunning() bool {
	return c.running.Load()
}

func (c *csmGroup) Health() error {
	if !c.IsRunning() {
		return fmt.Errorf("consumer %s %s is not running", c.topic, c.consumerGroupName)
	}
	return nil
}

type consumerMetrics struct {
	processDuration    metric.Int64Histogram
	e2eProcessDuration metric.Int64Histogram
	attrs              metric.MeasurementOption
}

func newConsumerMetrics(processMetricName, processMetricDescription, topic, consumerGroupName string) (consumerMetrics, error) {
	meter := otel.Meter(instrumentationName)
	processDuration, err := meter.Int64Histogram(processMetricName,
		metric.WithDescription(processMetricDescription),
		metric.WithUnit("ms"))
	if err != nil {
		return consumerMetrics{}, err
	}
	e2eProcessDuration, err := meter.Int64Histogram("kafka_client_e2e_process_duration",
		metric.WithDescription("end-to-end processing time from publish to complete in milliseconds"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(10, 50, 100, 300, 500, 700, 1_000, 5_000, 10_000, 30_000, 60_000, 120_000, 240_000, 300_000, 600_000, 1_800_000),
	)
	if err != nil {
		return consumerMetrics{}, err
	}

	return consumerMetrics{
		processDuration:    processDuration,
		e2eProcessDuration: e2eProcessDuration,
		attrs: metric.WithAttributes(
			attribute.String("consumer_group", consumerGroupName),
			attribute.String("topic", topic),
		),
	}, nil
}

type csmGrpHandler struct {
	cfg               KafkaConfig
	logger            Logger
	topic             string
	consumerGroupName string
	handler           MessageHandler[*sarama.ConsumerMessage]
	metrics           consumerMetrics
	propagator        propagation.TextMapPropagator
}

func createSaramaConsumerGroupHandler(
	cfg KafkaConfig,
	topic string,
	consumerGroupName string,
	handler MessageHandler[*sarama.ConsumerMessage],
) (*csmGrpHandler, error) {
	if handler == nil {
		return nil, errors.New("missing handler")
	}
	if consumerGroupName == "" {
		return nil, errors.New("missing consumer group name")
	}
	if topic == "" {
		return nil, errors.New("missing topic to consume")
	}

	metrics, err := newConsumerMetrics("kafka_client_consumer_process_duration", "consumer process duration in milliseconds", topic, consumerGroupName)
	if err != nil {
		return nil, err
	}

	return &csmGrpHandler{
		cfg:               cfg,
		logger:            cfg.log(),
		topic:             topic,
		consumerGroupName: consumerGroupName,
		handler:           handler,
		metrics:           metrics,
		propagator:        cfg.tracePropagator(),
	}, nil
}

func (c *csmGrpHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *csmGrpHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *csmGrpHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	if c.cfg.consumerWorkers > 1 {
		return c.consumeWithWorkers(session, claim, c.cfg.consumerWorkers)
	}

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				c.logger.Debug(session.Context(), "consumer claim channel has been closed: topic=%s consumer_group=%s", c.topic, c.consumerGroupName)
				return nil
			}
			if msg == nil {
				c.logger.Warn(session.Context(), "consumer claim got nil message, skipping: topic=%s consumer_group=%s", c.topic, c.consumerGroupName)
				continue
			}

			if err := c.handle(msg); err != nil {
				return err
			}
			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

func recordEndToEnd(ctx context.Context, cfg KafkaConfig, logger Logger, metrics consumerMetrics, topic, consumerGroupName string, now time.Time, msg *sarama.ConsumerMessage) {
	if msg.Timestamp.IsZero() {
		return
	}

	endToEndProcessTime := now.Sub(msg.Timestamp).Milliseconds()
	metrics.e2eProcessDuration.Record(context.Background(), endToEndProcessTime, metrics.attrs)

	if endToEndProcessTime >= cfg.SlowMessageThreshold.Milliseconds() {
		logger.Warn(ctx, "slow message: topic=%s consumer_group=%s message_key=%s duration_ms=%d",
			topic, consumerGroupName, convertBytesToString(msg.Key), endToEndProcessTime)
	}
}

type saramaConsumerWithRetryCounter struct {
	handler      sarama.ConsumerGroupHandler
	retryCounter *counter
}

func (s saramaConsumerWithRetryCounter) Setup(session sarama.ConsumerGroupSession) error {
	return s.handler.Setup(session)
}

func (s saramaConsumerWithRetryCounter) Cleanup(session sarama.ConsumerGroupSession) error {
	return s.handler.Cleanup(session)
}

func (s saramaConsumerWithRetryCounter) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	s.retryCounter.Clear()
	return s.handler.ConsumeClaim(session, claim)
}

type counter struct {
	count atomic.Int64
}

func (c *counter) Count() int {
	return int(c.count.Load())
}

func (c *counter) Inc() {
	c.count.Add(1)
}

func (c *counter) Clear() {
	c.count.Store(0)
}
