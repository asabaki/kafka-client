package kafkaclient

import (
	"time"

	"github.com/IBM/sarama"
)

// NewAsyncProducer creates an asynchronous (at-most-once) producer.
func NewAsyncProducer(config KafkaConfig, opts ...KafkaConfigOption) (AsyncProducer, error) {
	config = prepareConfig(config, opts...)
	saramaCfg := config.ToSaramaConfig()
	feedback := newFeedbackPartitioners()
	saramaCfg.Producer.Partitioner = feedback.wrap(saramaCfg.Producer.Partitioner)

	kafkaClient, err := sarama.NewClient(config.BootstrapServers, saramaCfg)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewAsyncProducerFromClient(kafkaClient)
	if err != nil {
		_ = kafkaClient.Close()
		return nil, err
	}

	metrics, err := newProducerMetrics("async")
	if err != nil {
		_ = producer.Close()
		_ = kafkaClient.Close()
		return nil, err
	}

	p := &asyncProducer{
		metrics:    metrics,
		producer:   producer,
		client:     kafkaClient,
		propagator: config.tracePropagator(),
		logger:     config.log(),
		onSuccess:  config.asyncSuccessHandler,
		onError:    config.asyncErrorHandler,
	}
	if p.onError == nil {
		p.onError = p.logProducerError
	}

	p.wg.Add(2)
	go func() {
		defer p.wg.Done()
		receiveUntilClose(producer.Successes(), func(msg *sarama.ProducerMessage) {
			feedback.success(msg)
			metrics.delivered(msg.Topic, nil)
			if p.onSuccess != nil {
				p.onSuccess(msg)
			}
		})
	}()
	go func() {
		defer p.wg.Done()
		receiveUntilClose(producer.Errors(), func(pe *sarama.ProducerError) {
			feedback.failure(pe.Msg, pe.Err)
			metrics.delivered(pe.Msg.Topic, pe.Err)
			p.onError(pe)
		})
	}()

	return p, nil
}

// NewSyncProducer creates a synchronous (acknowledged) producer.
func NewSyncProducer(config KafkaConfig, opts ...KafkaConfigOption) (SyncProducer, error) {
	config = prepareConfig(config, opts...)
	saramaCfg := config.ToSaramaConfig()
	feedback := newFeedbackPartitioners()
	saramaCfg.Producer.Partitioner = feedback.wrap(saramaCfg.Producer.Partitioner)

	kafkaClient, err := sarama.NewClient(config.BootstrapServers, saramaCfg)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewAsyncProducerFromClient(kafkaClient)
	if err != nil {
		_ = kafkaClient.Close()
		return nil, err
	}

	metrics, err := newProducerMetrics("sync")
	if err != nil {
		_ = producer.Close()
		_ = kafkaClient.Close()
		return nil, err
	}

	p := newSyncProducer(producer, kafkaClient, config, feedback, metrics)
	if config.ProducerHeartbeatEnabled {
		heartbeat := newProducerHeartbeat(config, kafkaClient)
		p.heartbeat = &heartbeat
		heartbeat.startProcess()
	}

	return p, nil
}

// NewConsumerGroup creates a consumer group that handles messages one at a time.
func NewConsumerGroup(
	cfg KafkaConfig,
	consumerGroup string,
	topic string,
	ignoreOldMessage bool,
	handler MessageHandler[*sarama.ConsumerMessage],
	opts ...KafkaConfigOption,
) (ConsumerGroup, error) {
	cfg = prepareConfig(cfg, opts...)

	h, err := createSaramaConsumerGroupHandler(cfg, topic, consumerGroup, handler)
	if err != nil {
		return nil, err
	}

	return createConsumerGroup(cfg, topic, consumerGroup, h, ignoreOldMessage)
}

// NewBatchConsumerGroup creates a consumer group that handles messages in batches of up to batchSize,
// flushing at least every batchTimeout.
func NewBatchConsumerGroup(
	cfg KafkaConfig,
	groupName string,
	topic string,
	batchSize uint16,
	batchTimeout time.Duration,
	ignoreOldMessage bool,
	handler MessagesHandler[*sarama.ConsumerMessage],
	opts ...KafkaConfigOption,
) (ConsumerGroup, error) {
	cfg = prepareConfig(cfg, opts...)

	h, err := createBatchSaramaConsumerGroupHandler(cfg, topic, groupName, handler, batchSize, batchTimeout)
	if err != nil {
		return nil, err
	}

	return createConsumerGroup(cfg, topic, groupName, h, ignoreOldMessage)
}

// prepareConfig applies options and process-wide side effects (sarama debug logger, go-metrics switch).
func prepareConfig(cfg KafkaConfig, opts ...KafkaConfigOption) KafkaConfig {
	cfg = cfg.withOptions(opts...)

	if cfg.Debug && cfg.logger != nil {
		// sarama.Logger is a package-level global in sarama; the last configured logger wins.
		sarama.Logger = saramaLogger{logger: cfg.logger}
	}
	if cfg.MetricsDisabled {
		disableMetric()
	}

	return cfg
}
