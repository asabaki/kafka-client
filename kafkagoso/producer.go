package kafkagoso

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/justtrackio/gosoline/pkg/appctx"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/kernel"
	"github.com/justtrackio/gosoline/pkg/log"

	kafkaclient "github.com/asabaki/kafka-client"
)

type (
	syncProducerKey  string
	asyncProducerKey string
	registryKey      struct{}
)

// producerRegistry tracks every producer provided in an application so the closer module can close them.
type producerRegistry struct {
	mu      sync.Mutex
	closers map[string]func() error
	order   []string
}

func provideRegistry(ctx context.Context) (*producerRegistry, error) {
	return appctx.Provide(ctx, registryKey{}, func() (*producerRegistry, error) {
		return &producerRegistry{closers: map[string]func() error{}}, nil
	})
}

func (r *producerRegistry) add(id string, closeFn func() error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.closers[id]; !ok {
		r.order = append(r.order, id)
	}
	r.closers[id] = closeFn
}

func (r *producerRegistry) closeAll(ctx context.Context, logger log.Logger) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var errs []error
	for _, id := range r.order {
		if err := r.closers[id](); err != nil {
			logger.Error(ctx, "can not close kafka producer %s: %s", id, err)
			errs = append(errs, fmt.Errorf("close %s: %w", id, err))
		}
	}
	r.closers = map[string]func() error{}
	r.order = nil

	return errors.Join(errs...)
}

// ProvideSyncProducer returns the application-wide SyncProducer configured under kafka_client.producer.<name>,
// creating it on first use. Every module asking for the same name gets the same instance.
// Add NewProducerCloserModuleFactory to the application so it is closed on shutdown.
func ProvideSyncProducer(ctx context.Context, config cfg.Config, logger log.Logger, name string) (kafkaclient.SyncProducer, error) {
	return appctx.Provide(ctx, syncProducerKey(name), func() (kafkaclient.SyncProducer, error) {
		kafkaCfg, opts, err := readProducer(config, logger, name)
		if err != nil {
			return nil, err
		}

		producer, err := kafkaclient.NewSyncProducer(kafkaCfg, opts...)
		if err != nil {
			return nil, fmt.Errorf("can not create kafka sync producer %q: %w", name, err)
		}

		if err := register(ctx, "sync/"+name, producer.Close); err != nil {
			_ = producer.Close()
			return nil, err
		}

		return producer, nil
	})
}

// ProvideAsyncProducer returns the application-wide AsyncProducer configured under kafka_client.producer.<name>,
// creating it on first use. Every module asking for the same name gets the same instance.
// Add NewProducerCloserModuleFactory to the application so buffered messages are flushed on shutdown.
func ProvideAsyncProducer(ctx context.Context, config cfg.Config, logger log.Logger, name string) (kafkaclient.AsyncProducer, error) {
	return appctx.Provide(ctx, asyncProducerKey(name), func() (kafkaclient.AsyncProducer, error) {
		kafkaCfg, opts, err := readProducer(config, logger, name)
		if err != nil {
			return nil, err
		}

		producer, err := kafkaclient.NewAsyncProducer(kafkaCfg, opts...)
		if err != nil {
			return nil, fmt.Errorf("can not create kafka async producer %q: %w", name, err)
		}

		if err := register(ctx, "async/"+name, producer.Close); err != nil {
			_ = producer.Close()
			return nil, err
		}

		return producer, nil
	})
}

func readProducer(config cfg.Config, logger log.Logger, name string) (kafkaclient.KafkaConfig, []kafkaclient.KafkaConfigOption, error) {
	settings, err := ReadProducerSettings(config, name)
	if err != nil {
		return kafkaclient.KafkaConfig{}, nil, err
	}

	kafkaCfg, opts, err := ReadConnection(config, logger, settings.Connection)
	if err != nil {
		return kafkaclient.KafkaConfig{}, nil, err
	}

	partitioner, err := settings.partitionerOption()
	if err != nil {
		return kafkaclient.KafkaConfig{}, nil, fmt.Errorf("kafka producer %q: %w", name, err)
	}

	return kafkaCfg, append(opts, partitioner), nil
}

func register(ctx context.Context, id string, closeFn func() error) error {
	registry, err := provideRegistry(ctx)
	if err != nil {
		return fmt.Errorf("can not register kafka producer %s: %w", id, err)
	}
	registry.add(id, closeFn)

	return nil
}

var _ kernel.FullModule = (*producerCloserModule)(nil)

// producerCloserModule closes all provided producers once the kernel shuts down.
//
// It is a background module in the service stage: the kernel stops stages highest-first, so consumers and
// HTTP handlers (application stage) have stopped producing before the producers are flushed and closed.
type producerCloserModule struct {
	kernel.BackgroundModule
	kernel.ServiceStage

	appCtx context.Context
	logger log.Logger
}

// NewProducerCloserModuleFactory returns a module that flushes and closes every producer obtained through
// ProvideSyncProducer / ProvideAsyncProducer when the application shuts down.
func NewProducerCloserModuleFactory() kernel.ModuleFactory {
	return func(ctx context.Context, _ cfg.Config, logger log.Logger) (kernel.Module, error) {
		// Producers may be provided by factories that run after this one, so the registry is resolved at shutdown.
		return &producerCloserModule{appCtx: ctx, logger: logger.WithChannel("kafka-client")}, nil
	}
}

func (m *producerCloserModule) Run(ctx context.Context) error {
	<-ctx.Done()

	registry, err := provideRegistry(m.appCtx)
	if err != nil {
		return err
	}

	return registry.closeAll(context.WithoutCancel(ctx), m.logger)
}

func (m *producerCloserModule) IsHealthy(context.Context) (bool, error) {
	return true, nil
}
