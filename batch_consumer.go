package kafkaclient

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel/propagation"

	"github.com/asabaki/kafka-client/internal/timeutil"
)

var _ sarama.ConsumerGroupHandler = (*batchConsumerHandler)(nil)

type batchConsumerHandler struct {
	cfg               KafkaConfig
	logger            Logger
	topic             string
	consumerGroupName string
	handler           MessagesHandler[*sarama.ConsumerMessage]
	batchSize         uint16
	batchTimeout      time.Duration
	metrics           consumerMetrics
	propagator        propagation.TextMapPropagator

	mu     sync.Mutex
	buffer []*sarama.ConsumerMessage
	timer  *time.Timer
}

func createBatchSaramaConsumerGroupHandler(
	cfg KafkaConfig,
	topic string,
	consumerGroupName string,
	handler MessagesHandler[*sarama.ConsumerMessage],
	batchSize uint16,
	batchTimeout time.Duration,
) (*batchConsumerHandler, error) {
	if topic == "" {
		return nil, errors.New("missing topic to consume")
	}
	if consumerGroupName == "" {
		return nil, errors.New("missing consumer group name")
	}
	if batchSize == 0 {
		return nil, errors.New("batch size must be greater than zero")
	}
	if batchTimeout <= 0 {
		return nil, errors.New("batch timeout must be greater than zero")
	}
	if handler == nil {
		return nil, errors.New("missing messages handler")
	}

	metrics, err := newConsumerMetrics("kafka_client_batch_consumer_process_duration", "batch consumer process duration in milliseconds", topic, consumerGroupName)
	if err != nil {
		return nil, err
	}

	return &batchConsumerHandler{
		cfg:               cfg,
		logger:            cfg.log(),
		topic:             topic,
		consumerGroupName: consumerGroupName,
		handler:           handler,
		batchSize:         batchSize,
		batchTimeout:      batchTimeout,
		metrics:           metrics,
		propagator:        cfg.tracePropagator(),
	}, nil
}

func (h *batchConsumerHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Messages buffered in a previous session may belong to partitions revoked by the rebalance.
	// They were never marked, so they will be redelivered to whichever member owns them now.
	h.buffer = nil
	h.timer = time.NewTimer(h.batchTimeout)
	h.logger.Debug(session.Context(), "batch consumer session started: topic=%s consumer_group=%s batch_timeout=%s", h.topic, h.consumerGroupName, h.batchTimeout)
	return nil
}

func (h *batchConsumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.timer != nil {
		h.timer.Stop()
	}
	return nil
}

func (h *batchConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				h.logger.Debug(session.Context(), "consumer claim channel has been closed: topic=%s consumer_group=%s", h.topic, h.consumerGroupName)
				return nil
			}
			if msg == nil {
				h.logger.Warn(session.Context(), "consumer claim got nil message, skipping: topic=%s consumer_group=%s", h.topic, h.consumerGroupName)
				continue
			}

			h.mu.Lock()
			h.buffer = append(h.buffer, msg)
			full := len(h.buffer) >= int(h.batchSize)
			h.mu.Unlock()

			if full {
				if err := h.flushBatch(session); err != nil {
					return err
				}
			}

		case <-h.timer.C:
			if err := h.flushBatch(session); err != nil {
				return err
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

func (h *batchConsumerHandler) flushBatch(session sarama.ConsumerGroupSession) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.buffer) == 0 {
		h.timer.Reset(h.batchTimeout)
		return nil
	}

	h.logger.Debug(session.Context(), "flushing batch: topic=%s consumer_group=%s batch_size=%d", h.topic, h.consumerGroupName, len(h.buffer))

	st := timeutil.Now()

	messagesWithContext := make([]*MessageWithContext[*sarama.ConsumerMessage], 0, len(h.buffer))
	for _, msg := range h.buffer {
		ctx := extractTraceContext(context.Background(), h.propagator, msg)
		messagesWithContext = append(messagesWithContext, &MessageWithContext[*sarama.ConsumerMessage]{ctx: ctx, msg: msg})
	}

	if err := h.handler(messagesWithContext); err != nil {
		h.logger.Error(session.Context(), "batch processing error: topic=%s consumer_group=%s: %s", h.topic, h.consumerGroupName, err)
		return err
	}
	for _, msg := range h.buffer {
		session.MarkMessage(msg, "")
	}

	et := timeutil.Now()
	h.metrics.processDuration.Record(context.Background(), et.Sub(st).Milliseconds(), h.metrics.attrs)
	for _, m := range messagesWithContext {
		recordEndToEnd(m.ctx, h.cfg, h.logger, h.metrics, h.topic, h.consumerGroupName, et, m.msg)
	}

	h.buffer = nil
	h.timer.Reset(h.batchTimeout)

	return nil
}
