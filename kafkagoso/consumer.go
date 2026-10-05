package kafkagoso

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/kernel"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/justtrackio/gosoline/pkg/tracing"

	kafkaclient "github.com/asabaki/kafka-client"
)

type (
	// HandlerFactory builds the message handler of a single-message consumer. It runs during module creation,
	// so dependencies (repositories, producers, ...) can be booted from config here.
	HandlerFactory func(ctx context.Context, config cfg.Config, logger log.Logger) (kafkaclient.MessageHandler[*sarama.ConsumerMessage], error)
	// BatchHandlerFactory builds the handler of a batch consumer (kafka_client.consumer.<name>.batch_size > 0).
	BatchHandlerFactory func(ctx context.Context, config cfg.Config, logger log.Logger) (kafkaclient.MessagesHandler[*sarama.ConsumerMessage], error)
)

var (
	_ kernel.FullModule = (*consumerModule)(nil)
)

// consumerModule runs a kafka-client consumer group as a gosoline kernel module.
// It is essential (the app stops if the consumer dies) and runs in the application stage,
// so it is stopped before service-stage modules such as the producer closer.
type consumerModule struct {
	kernel.EssentialModule
	kernel.ApplicationStage

	consumer kafkaclient.ConsumerGroup
}

// NewConsumerModuleFactory returns a module factory for a single-message consumer configured under
// kafka_client.consumer.<name>.
func NewConsumerModuleFactory(name string, handlerFactory HandlerFactory) kernel.ModuleFactory {
	return func(ctx context.Context, config cfg.Config, logger log.Logger) (kernel.Module, error) {
		settings, kafkaCfg, opts, err := readConsumer(config, logger, name)
		if err != nil {
			return nil, err
		}
		if settings.BatchSize > 0 {
			return nil, fmt.Errorf("kafka consumer %q has batch_size=%d: use NewBatchConsumerModuleFactory", name, settings.BatchSize)
		}

		handler, err := handlerFactory(ctx, config, logger)
		if err != nil {
			return nil, fmt.Errorf("can not create handler for kafka consumer %q: %w", name, err)
		}

		if settings.SpanEnabled {
			tracer, err := tracing.ProvideTracer(ctx, config, logger)
			if err != nil {
				return nil, fmt.Errorf("can not create tracer for kafka consumer %q: %w", name, err)
			}
			handler = withSpan(tracer, settings, handler)
		}

		opts = append(opts, kafkaclient.WithConsumerWorkers(settings.WorkerCount))
		consumer, err := kafkaclient.NewConsumerGroup(kafkaCfg, settings.GroupID, settings.Topic, settings.IgnoreOldMessages, handler, opts...)
		if err != nil {
			return nil, fmt.Errorf("can not create kafka consumer %q: %w", name, err)
		}

		return &consumerModule{consumer: consumer}, nil
	}
}

// NewBatchConsumerModuleFactory returns a module factory for a batch consumer configured under
// kafka_client.consumer.<name>; batch_size must be > 0.
func NewBatchConsumerModuleFactory(name string, handlerFactory BatchHandlerFactory) kernel.ModuleFactory {
	return func(ctx context.Context, config cfg.Config, logger log.Logger) (kernel.Module, error) {
		settings, kafkaCfg, opts, err := readConsumer(config, logger, name)
		if err != nil {
			return nil, err
		}
		if settings.BatchSize == 0 {
			return nil, fmt.Errorf("kafka consumer %q: batch_size must be > 0 for a batch consumer", name)
		}

		handler, err := handlerFactory(ctx, config, logger)
		if err != nil {
			return nil, fmt.Errorf("can not create handler for kafka consumer %q: %w", name, err)
		}

		if settings.SpanEnabled {
			tracer, err := tracing.ProvideTracer(ctx, config, logger)
			if err != nil {
				return nil, fmt.Errorf("can not create tracer for kafka consumer %q: %w", name, err)
			}
			handler = withBatchSpans(tracer, settings, handler)
		}

		consumer, err := kafkaclient.NewBatchConsumerGroup(kafkaCfg, settings.GroupID, settings.Topic, settings.BatchSize, settings.BatchTimeout, settings.IgnoreOldMessages, handler, opts...)
		if err != nil {
			return nil, fmt.Errorf("can not create kafka batch consumer %q: %w", name, err)
		}

		return &consumerModule{consumer: consumer}, nil
	}
}

// withSpan starts one span per message, continuing the trace extracted from the message headers.
func withSpan(tracer tracing.Tracer, settings *ConsumerSettings, handler kafkaclient.MessageHandler[*sarama.ConsumerMessage]) kafkaclient.MessageHandler[*sarama.ConsumerMessage] {
	return func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		ctx, span := tracer.StartSpanFromContext(ctx, settings.SpanName)
		defer span.Finish()
		annotate(span, settings, msg)

		err := handler(ctx, msg)
		if err != nil {
			span.AddError(err)
		}

		return err
	}
}

// withBatchSpans starts one span per message of the batch (each continuing its own trace) and finishes them
// once the batch handler returns. The span context is available through MessageWithContext.Get().
func withBatchSpans(tracer tracing.Tracer, settings *ConsumerSettings, handler kafkaclient.MessagesHandler[*sarama.ConsumerMessage]) kafkaclient.MessagesHandler[*sarama.ConsumerMessage] {
	return func(messages []*kafkaclient.MessageWithContext[*sarama.ConsumerMessage]) error {
		spans := make([]tracing.Span, 0, len(messages))
		wrapped := make([]*kafkaclient.MessageWithContext[*sarama.ConsumerMessage], 0, len(messages))

		for _, m := range messages {
			msgCtx, msg := m.Get()
			spanCtx, span := tracer.StartSpanFromContext(msgCtx, settings.SpanName)
			annotate(span, settings, msg)
			spans = append(spans, span)
			wrapped = append(wrapped, kafkaclient.NewMessageWithContext(spanCtx, msg))
		}

		err := handler(wrapped)
		for _, span := range spans {
			if err != nil {
				span.AddError(err)
			}
			span.Finish()
		}

		return err
	}
}

func annotate(span tracing.Span, settings *ConsumerSettings, msg *sarama.ConsumerMessage) {
	span.AddAnnotation("kafka_topic", msg.Topic)
	span.AddAnnotation("kafka_group_id", settings.GroupID)
	span.AddMetadata("kafka_partition", msg.Partition)
	span.AddMetadata("kafka_offset", msg.Offset)
}

func readConsumer(config cfg.Config, logger log.Logger, name string) (*ConsumerSettings, kafkaclient.KafkaConfig, []kafkaclient.KafkaConfigOption, error) {
	settings, err := ReadConsumerSettings(config, name)
	if err != nil {
		return nil, kafkaclient.KafkaConfig{}, nil, err
	}

	kafkaCfg, opts, err := ReadConnection(config, logger, settings.Connection)
	if err != nil {
		return nil, kafkaclient.KafkaConfig{}, nil, err
	}

	return settings, kafkaCfg, opts, nil
}

func (m *consumerModule) Run(ctx context.Context) error {
	return m.consumer.Run(ctx)
}

func (m *consumerModule) IsHealthy(context.Context) (bool, error) {
	return m.consumer.IsRunning(), nil
}
