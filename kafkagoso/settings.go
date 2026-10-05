// Package kafkagoso wires kafka-client into gosoline applications.
//
// Configuration layout:
//
//	kafka_client:
//	  connection:
//	    default:                     # any KafkaConfig field, snake_case (see KafkaConfig cfg tags)
//	      bootstrap_servers: [localhost:9092]
//	      sasl_enabled: false
//	      tls_enabled: false
//	      logging_enabled: true      # opt-in: route kafka-client logs to the gosoline logger
//	  consumer:
//	    my_consumer:
//	      connection: default        # optional, defaults to "default"
//	      topic: my.topic
//	      group_id: my-group
//	      ignore_old_messages: false
//	      batch_size: 0              # 0 = single-message consumer, > 0 = batch consumer
//	      batch_timeout: 1s
//	  producer:
//	    my_producer:
//	      connection: default
//	      partitioner: consistent_hash  # consistent_hash | active_partition | sarama_hash
//	      circuit_breaker:             # only for active_partition
//	        max_failures: 3
//	        open_duration: 30s
package kafkagoso

import (
	"fmt"
	"time"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
	"go.opentelemetry.io/otel/propagation"

	kafkaclient "github.com/asabaki/kafka-client"
)

const configPrefix = "kafka_client"

// ConnectionSettings is the gosoline config shape of a named connection.
type ConnectionSettings struct {
	kafkaclient.KafkaConfig
	// LoggingEnabled opts in to kafka-client logging through the gosoline logger. Default: false.
	LoggingEnabled bool `cfg:"logging_enabled" default:"false"`
	// TracePropagation selects which message headers carry the trace: gosoline | otel | all | none.
	TracePropagation string `cfg:"trace_propagation" default:"gosoline" validate:"oneof=gosoline otel all none"`
}

type ConsumerSettings struct {
	Connection        string        `cfg:"connection" default:"default"`
	Topic             string        `cfg:"topic"`
	GroupID           string        `cfg:"group_id"`
	IgnoreOldMessages bool          `cfg:"ignore_old_messages" default:"false"`
	BatchSize         uint16        `cfg:"batch_size" default:"0"`
	BatchTimeout      time.Duration `cfg:"batch_timeout" default:"1s"`
	// WorkerCount handles each partition with this many goroutines, routed by key (single-message consumers only).
	// 1 = one goroutine per partition.
	WorkerCount int `cfg:"worker_count" default:"1"`
	// SpanEnabled wraps every handled message in a gosoline tracing span (tracing.provider decides what that does).
	SpanEnabled bool `cfg:"span_enabled" default:"true"`
	// SpanName is the span name; defaults to "kafka-consumer-<consumer name>".
	SpanName string `cfg:"span_name"`
}

type ProducerSettings struct {
	Connection string `cfg:"connection" default:"default"`
	// Partitioner selects how messages are assigned to partitions: consistent_hash | active_partition | sarama_hash.
	Partitioner    string                         `cfg:"partitioner" default:"consistent_hash"`
	CircuitBreaker ProducerCircuitBreakerSettings `cfg:"circuit_breaker"`
}

// ProducerCircuitBreakerSettings tune partitioner: active_partition.
type ProducerCircuitBreakerSettings struct {
	MaxFailures  int           `cfg:"max_failures" default:"3"`
	OpenDuration time.Duration `cfg:"open_duration" default:"30s"`
}

const (
	PartitionerConsistentHash  = "consistent_hash"
	PartitionerActivePartition = "active_partition"
	PartitionerSaramaHash      = "sarama_hash"
)

// partitionerOption turns the producer settings into a KafkaConfigOption.
func (s *ProducerSettings) partitionerOption() (kafkaclient.KafkaConfigOption, error) {
	switch s.Partitioner {
	case PartitionerConsistentHash:
		return kafkaclient.WithPartitioner(kafkaclient.NewConsistentHashPartition), nil
	case PartitionerActivePartition:
		if s.CircuitBreaker.MaxFailures < 1 {
			return nil, fmt.Errorf("circuit_breaker.max_failures must be >= 1, got %d", s.CircuitBreaker.MaxFailures)
		}
		if s.CircuitBreaker.OpenDuration <= 0 {
			return nil, fmt.Errorf("circuit_breaker.open_duration must be > 0, got %s", s.CircuitBreaker.OpenDuration)
		}
		return kafkaclient.WithPartitioner(kafkaclient.NewActivePartitionPartitioner(kafkaclient.ActivePartitionConfig{
			MaxFailures:  s.CircuitBreaker.MaxFailures,
			OpenDuration: s.CircuitBreaker.OpenDuration,
		})), nil
	case PartitionerSaramaHash:
		return kafkaclient.WithDefaultPartitioner(), nil
	default:
		return nil, fmt.Errorf("unknown partitioner %q, expected one of: %s, %s, %s",
			s.Partitioner, PartitionerConsistentHash, PartitionerActivePartition, PartitionerSaramaHash)
	}
}

// ReadConnection reads kafka_client.connection.<name> and returns the KafkaConfig plus the options
// (e.g. the logger) implied by the settings.
func ReadConnection(config cfg.Config, logger log.Logger, name string) (kafkaclient.KafkaConfig, []kafkaclient.KafkaConfigOption, error) {
	key := fmt.Sprintf("%s.connection.%s", configPrefix, name)
	if !config.IsSet(key) {
		return kafkaclient.KafkaConfig{}, nil, fmt.Errorf("kafka connection %q is not configured (missing %s)", name, key)
	}

	settings := &ConnectionSettings{}
	if err := config.UnmarshalKey(key, settings); err != nil {
		return kafkaclient.KafkaConfig{}, nil, fmt.Errorf("can not read kafka connection %q: %w", name, err)
	}
	if len(settings.BootstrapServers) == 0 {
		return kafkaclient.KafkaConfig{}, nil, fmt.Errorf("kafka connection %q: %s.bootstrap_servers must not be empty", name, key)
	}

	propagator, err := tracePropagator(settings.TracePropagation)
	if err != nil {
		return kafkaclient.KafkaConfig{}, nil, fmt.Errorf("kafka connection %q: %w", name, err)
	}

	opts := []kafkaclient.KafkaConfigOption{kafkaclient.WithTracePropagator(propagator)}
	if settings.LoggingEnabled {
		opts = append(opts, kafkaclient.WithLogger(logger.WithChannel("kafka-client").WithFields(log.Fields{
			"kafka_connection": name,
		})))
	}

	return settings.KafkaConfig, opts, nil
}

func tracePropagator(mode string) (propagation.TextMapPropagator, error) {
	switch mode {
	case "gosoline":
		return GosolinePropagator{}, nil
	case "otel":
		return kafkaclient.OTelGlobalPropagator(), nil
	case "all":
		return propagation.NewCompositeTextMapPropagator(GosolinePropagator{}, kafkaclient.OTelGlobalPropagator()), nil
	case "none":
		return propagation.NewCompositeTextMapPropagator(), nil
	default:
		return nil, fmt.Errorf("unknown trace_propagation %q, expected one of: gosoline, otel, all, none", mode)
	}
}

// ReadConsumerSettings reads kafka_client.consumer.<name>.
func ReadConsumerSettings(config cfg.Config, name string) (*ConsumerSettings, error) {
	key := fmt.Sprintf("%s.consumer.%s", configPrefix, name)
	settings := &ConsumerSettings{}
	if err := config.UnmarshalKey(key, settings); err != nil {
		return nil, fmt.Errorf("can not read kafka consumer %q: %w", name, err)
	}
	if settings.Topic == "" {
		return nil, fmt.Errorf("kafka consumer %q: %s.topic must not be empty", name, key)
	}
	if settings.GroupID == "" {
		return nil, fmt.Errorf("kafka consumer %q: %s.group_id must not be empty", name, key)
	}
	if settings.WorkerCount < 1 {
		return nil, fmt.Errorf("kafka consumer %q: %s.worker_count must be >= 1, got %d", name, key, settings.WorkerCount)
	}
	if settings.WorkerCount > 1 && settings.BatchSize > 0 {
		return nil, fmt.Errorf("kafka consumer %q: %s.worker_count is not supported for batch consumers (batch_size > 0)", name, key)
	}
	if settings.SpanName == "" {
		settings.SpanName = "kafka-consumer-" + name
	}

	return settings, nil
}

// ReadProducerSettings reads kafka_client.producer.<name>.
func ReadProducerSettings(config cfg.Config, name string) (*ProducerSettings, error) {
	key := fmt.Sprintf("%s.producer.%s", configPrefix, name)
	settings := &ProducerSettings{}
	if err := config.UnmarshalKey(key, settings); err != nil {
		return nil, fmt.Errorf("can not read kafka producer %q: %w", name, err)
	}
	if _, err := settings.partitionerOption(); err != nil {
		return nil, fmt.Errorf("kafka producer %q: %s.%w", name, key, err)
	}

	return settings, nil
}
