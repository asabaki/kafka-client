package kafkaclient

import (
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
	gometrics "github.com/rcrowley/go-metrics"
	"go.opentelemetry.io/otel/propagation"

	"github.com/asabaki/kafka-client/internal/scram"
)

var hostname string
var once sync.Once

func init() {
	hostname, _ = os.Hostname()
}

func disableMetric() {
	// disable go-metrics just once to prevent race condition
	once.Do(func() {
		gometrics.UseNilMetrics = true
	})
}

type IsolationLevel string

const (
	IsolationLevelReadCommitted   IsolationLevel = "READ_COMMITTED"
	IsolationLevelReadUncommitted IsolationLevel = "READ_UNCOMMITTED"
)

type KafkaConfig struct {
	// A user-provided string sent with every request to the brokers for logging,
	// debugging, and auditing purposes. Defaults to current host name, but you should
	// probably set it to something specific to your application.
	ClientID                 string         `env:"KAFKA_CLIENT_ID" envconfig:"KAFKA_CLIENT_ID" cfg:"client_id"`
	BootstrapServers         []string       `env:"KAFKA_BOOTSTRAP_SERVERS" envconfig:"KAFKA_BOOTSTRAP_SERVERS" cfg:"bootstrap_servers"`
	SaslEnabled              bool           `env:"KAFKA_SASL_ENABLED" envconfig:"KAFKA_SASL_ENABLED" cfg:"sasl_enabled" default:"true"`
	SaslMechanisms           string         `env:"KAFKA_SASL_MECHANISMS" envconfig:"KAFKA_SASL_MECHANISMS" cfg:"sasl_mechanisms" default:"PLAIN"`
	SaslUsername             string         `env:"KAFKA_SASL_USERNAME" envconfig:"KAFKA_SASL_USERNAME" cfg:"sasl_username"`
	SaslPassword             string         `env:"KAFKA_SASL_PASSWORD" envconfig:"KAFKA_SASL_PASSWORD" cfg:"sasl_password"`
	TlsEnabled               bool           `env:"KAFKA_TLS_ENABLED" envconfig:"KAFKA_TLS_ENABLED" cfg:"tls_enabled" default:"true"` // nolint ST1003
	IsolationLevel           IsolationLevel `env:"KAFKA_ISOLATION_LEVEL" envconfig:"KAFKA_ISOLATION_LEVEL" cfg:"isolation_level" default:"READ_COMMITTED"`
	AutoOffsetReset          string         `env:"KAFKA_AUTO_OFFSET_RESET" envconfig:"KAFKA_AUTO_OFFSET_RESET" cfg:"auto_offset_reset"`
	EnableAutoCommit         bool           `env:"KAFKA_ENABLE_AUTO_COMMIT" envconfig:"KAFKA_ENABLE_AUTO_COMMIT" cfg:"enable_auto_commit"`
	ProducerRetryMax         int            `env:"KAFKA_PRODUCER_RETRY_MAX" envconfig:"KAFKA_PRODUCER_RETRY_MAX" cfg:"producer_retry_max" default:"3"`
	ProducerRetryBackoff     time.Duration  `env:"KAFKA_PRODUCER_RETRY_BACKOFF" envconfig:"KAFKA_PRODUCER_RETRY_BACKOFF" cfg:"producer_retry_backoff" default:"100ms"`
	ProducerFlushMessages    int            `env:"KAFKA_PRODUCER_FLUSH_MESSAGES" envconfig:"KAFKA_PRODUCER_FLUSH_MESSAGES" cfg:"producer_flush_messages" default:"0"`
	ProducerFlushMaxMessages int            `env:"KAFKA_PRODUCER_FLUSH_MAX_MESSAGES" envconfig:"KAFKA_PRODUCER_FLUSH_MAX_MESSAGES" cfg:"producer_flush_max_messages" default:"0"`
	ProducerFlushBytes       int            `env:"KAFKA_PRODUCER_FLUSH_BYTES" envconfig:"KAFKA_PRODUCER_FLUSH_BYTES" cfg:"producer_flush_bytes" default:"1048576"`
	ProducerFlushFrequency   time.Duration  `env:"KAFKA_PRODUCER_FLUSH_FREQUENCY" envconfig:"KAFKA_PRODUCER_FLUSH_FREQUENCY" cfg:"producer_flush_frequency" default:"100ms"`
	ProducerRequiredAcks     int16          `env:"KAFKA_PRODUCER_REQUIRED_ACKS" envconfig:"KAFKA_PRODUCER_REQUIRED_ACKS" cfg:"producer_required_acks" default:"-1"`
	ProducerTimeout          time.Duration  `env:"KAFKA_PRODUCER_TIMEOUT" envconfig:"KAFKA_PRODUCER_TIMEOUT" cfg:"producer_timeout" default:"10s"`
	ProducerIdempotent       bool           `env:"KAFKA_PRODUCER_IDEMPOTENT" envconfig:"KAFKA_PRODUCER_IDEMPOTENT" cfg:"producer_idempotent" default:"true"`
	// ProducerHeartbeatEnabled If set to true, It will start the go routine heartbeat to keep the connection
	// between producer and brokers alive which help reduce the latency of reconnection during the producer idle time.
	ProducerHeartbeatEnabled bool `env:"KAFKA_PRODUCER_HEARTBEAT_ENABLED" envconfig:"KAFKA_PRODUCER_HEARTBEAT_ENABLED" cfg:"producer_heartbeat_enabled" default:"false"`
	// ProducerHeartbeatInterval control the interval time that the heartbeat sent the keepalive request to all available brokers.
	ProducerHeartbeatInterval time.Duration `env:"KAFKA_PRODUCER_HEARTBEAT_INTERVAL" envconfig:"KAFKA_PRODUCER_HEARTBEAT_INTERVAL" cfg:"producer_heartbeat_interval" default:"5m"`
	CompressionType           string        `env:"KAFKA_COMPRESSION_TYPE" envconfig:"KAFKA_COMPRESSION_TYPE" cfg:"compression_type" default:"lz4"`
	Debug                     bool          `env:"SARAMA_DEBUG" envconfig:"SARAMA_DEBUG" cfg:"debug" default:"false"`
	InsecureSkipVerify        bool          `env:"KAFKA_INSECURE_SKIP_VERIFY" envconfig:"KAFKA_INSECURE_SKIP_VERIFY" cfg:"insecure_skip_verify" default:"false"`
	TLSConfigServerName       string        `env:"KAFKA_TLS_CONFIG_SERVER_NAME" envconfig:"KAFKA_TLS_CONFIG_SERVER_NAME" cfg:"tls_config_server_name"`
	ConsumerGroupMaxRetry     int           `env:"KAFKA_CONSUMER_GROUP_MAX_RETRY" envconfig:"KAFKA_CONSUMER_GROUP_MAX_RETRY" cfg:"consumer_group_max_retry" default:"10"`
	MetricsDisabled           bool          `env:"KAFKA_METRICS_DISABLED" envconfig:"KAFKA_METRICS_DISABLED" cfg:"metrics_disabled" default:"false"`
	KeepAlive                 time.Duration `env:"KAFKA_KEEPALIVE" envconfig:"KAFKA_KEEPALIVE" cfg:"keepalive"`
	SlowMessageThreshold      time.Duration `env:"KAFKA_SLOW_MESSAGE_THRESHOLD" envconfig:"KAFKA_SLOW_MESSAGE_THRESHOLD" cfg:"slow_message_threshold" default:"5s"`
	// NetDialTimeout, NetReadTimeout and NetWriteTimeout bound each broker connection attempt and request.
	// A broker that vanished without closing its connections (e.g. a reclaimed node) is only detected when they
	// expire. 0 = sarama's default of 30s.
	NetDialTimeout  time.Duration `env:"KAFKA_NET_DIAL_TIMEOUT" envconfig:"KAFKA_NET_DIAL_TIMEOUT" cfg:"net_dial_timeout"`
	NetReadTimeout  time.Duration `env:"KAFKA_NET_READ_TIMEOUT" envconfig:"KAFKA_NET_READ_TIMEOUT" cfg:"net_read_timeout"`
	NetWriteTimeout time.Duration `env:"KAFKA_NET_WRITE_TIMEOUT" envconfig:"KAFKA_NET_WRITE_TIMEOUT" cfg:"net_write_timeout"`
	// ProducerRetryMaxBufferLength and ProducerRetryMaxBufferBytes cap the messages a producer holds in memory for
	// retry, e.g. while partitions have no leader. When full, the oldest are failed with
	// sarama.ErrProducerRetryBufferOverflow (reported like any delivery failure) instead of growing without bound.
	// 0 = the library default (10000 messages / 64 MiB); negative = unlimited. sarama raises values below
	// 4096 messages / 32 MiB to those minimums.
	ProducerRetryMaxBufferLength int   `env:"KAFKA_PRODUCER_RETRY_MAX_BUFFER_LENGTH" envconfig:"KAFKA_PRODUCER_RETRY_MAX_BUFFER_LENGTH" cfg:"producer_retry_max_buffer_length"`
	ProducerRetryMaxBufferBytes  int64 `env:"KAFKA_PRODUCER_RETRY_MAX_BUFFER_BYTES" envconfig:"KAFKA_PRODUCER_RETRY_MAX_BUFFER_BYTES" cfg:"producer_retry_max_buffer_bytes"`

	producerPartitioner sarama.PartitionerConstructor
	tlsConfig           *tls.Config
	logger              Logger
	asyncSuccessHandler func(*sarama.ProducerMessage)
	asyncErrorHandler   func(*sarama.ProducerError)
	propagator          propagation.TextMapPropagator
	consumerWorkers     int
	// saramaConfigHook lets tests adjust the final sarama config (e.g. for sarama's MockBroker).
	saramaConfigHook func(*sarama.Config)
}

type KafkaConfigOption interface {
	apply(cfg *KafkaConfig)
}

type kafkaConfigOptionFunc func(*KafkaConfig)

func (fn kafkaConfigOptionFunc) apply(cfg *KafkaConfig) {
	fn(cfg)
}

func WithDefaultPartitioner() KafkaConfigOption {
	return kafkaConfigOptionFunc(func(cfg *KafkaConfig) {
		cfg.producerPartitioner = sarama.NewHashPartitioner
	})
}

func WithPartitioner(partitioner sarama.PartitionerConstructor) KafkaConfigOption {
	return kafkaConfigOptionFunc(func(cfg *KafkaConfig) {
		cfg.producerPartitioner = partitioner
	})
}

func WithTLSConfig(tlsConfig *tls.Config) KafkaConfigOption {
	return kafkaConfigOptionFunc(func(cfg *KafkaConfig) {
		cfg.tlsConfig = tlsConfig
	})
}

// WithLogger opts in to logging. Without it, kafka-client logs nothing.
// When KafkaConfig.Debug is also true, sarama's internal logs are forwarded to this logger at debug level.
func WithLogger(logger Logger) KafkaConfigOption {
	return kafkaConfigOptionFunc(func(cfg *KafkaConfig) {
		cfg.logger = logger
	})
}

// WithAsyncSuccessHandler sets a callback for every message acknowledged by an AsyncProducer.
func WithAsyncSuccessHandler(fn func(*sarama.ProducerMessage)) KafkaConfigOption {
	return kafkaConfigOptionFunc(func(cfg *KafkaConfig) {
		cfg.asyncSuccessHandler = fn
	})
}

// WithAsyncErrorHandler sets a callback for every message an AsyncProducer failed to deliver.
// Defaults to logging the error through the configured Logger (a no-op if no logger is set).
func WithAsyncErrorHandler(fn func(*sarama.ProducerError)) KafkaConfigOption {
	return kafkaConfigOptionFunc(func(cfg *KafkaConfig) {
		cfg.asyncErrorHandler = fn
	})
}

// WithTracePropagator sets how trace context is written to (producer) and read from (consumer) message headers.
// Default: OpenTelemetry's global propagator (a no-op until an OpenTelemetry SDK registers one).
func WithTracePropagator(propagator propagation.TextMapPropagator) KafkaConfigOption {
	return kafkaConfigOptionFunc(func(cfg *KafkaConfig) {
		cfg.propagator = propagator
	})
}

func (k KafkaConfig) tracePropagator() propagation.TextMapPropagator {
	if k.propagator == nil {
		return globalPropagator{}
	}
	return k.propagator
}

func WithProducerIdempotent(enable bool) KafkaConfigOption {
	return kafkaConfigOptionFunc(func(cfg *KafkaConfig) {
		cfg.ProducerIdempotent = enable
	})
}

func (k KafkaConfig) withOptions(opts ...KafkaConfigOption) KafkaConfig {
	for _, opt := range opts {
		opt.apply(&k)
	}
	return k
}

func (k KafkaConfig) log() Logger {
	return loggerOrNop(k.logger)
}

func (k KafkaConfig) ToSaramaConfig(opts ...KafkaConfigOption) *sarama.Config {
	k = k.withOptions(opts...)

	c := sarama.NewConfig()

	c.ClientID = k.ClientID
	if k.ClientID == "" {
		c.ClientID = hostname
	}

	c.Net.SASL.Enable = k.SaslEnabled
	if k.SaslEnabled {
		c.Net.SASL.Mechanism = sarama.SASLMechanism(k.SaslMechanisms)
		c.Net.SASL.User = k.SaslUsername
		c.Net.SASL.Password = k.SaslPassword
		c.Net.SASL.Handshake = true
		switch c.Net.SASL.Mechanism {
		case sarama.SASLTypeSCRAMSHA256:
			c.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &scram.Client{HashGeneratorFcn: sha256.New} }
		case sarama.SASLTypeSCRAMSHA512:
			c.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &scram.Client{HashGeneratorFcn: sha512.New} }
		}
	}
	c.Net.KeepAlive = k.KeepAlive
	if k.NetDialTimeout > 0 {
		c.Net.DialTimeout = k.NetDialTimeout
	}
	if k.NetReadTimeout > 0 {
		c.Net.ReadTimeout = k.NetReadTimeout
	}
	if k.NetWriteTimeout > 0 {
		c.Net.WriteTimeout = k.NetWriteTimeout
	}

	c.Net.TLS.Enable = k.TlsEnabled
	if k.TlsEnabled {
		if k.tlsConfig == nil {
			// #nosec G402
			c.Net.TLS.Config = &tls.Config{InsecureSkipVerify: k.InsecureSkipVerify}
			if k.TLSConfigServerName != "" {
				c.Net.TLS.Config.ServerName = k.TLSConfigServerName
			}
		} else {
			c.Net.TLS.Config = k.tlsConfig
		}
	}
	c.Producer.Retry.Max = k.ProducerRetryMax
	c.Producer.Retry.Backoff = k.ProducerRetryBackoff
	c.Producer.Retry.MaxBufferLength = defaultIfZero(k.ProducerRetryMaxBufferLength, defaultRetryMaxBufferLength)
	c.Producer.Retry.MaxBufferBytes = defaultIfZero(k.ProducerRetryMaxBufferBytes, defaultRetryMaxBufferBytes)
	c.Producer.Flush.Messages = k.ProducerFlushMessages
	c.Producer.Flush.Bytes = k.ProducerFlushBytes
	c.Producer.Flush.Frequency = k.ProducerFlushFrequency
	c.Producer.Flush.MaxMessages = k.ProducerFlushMaxMessages

	c.Producer.RequiredAcks = sarama.RequiredAcks(k.ProducerRequiredAcks)
	c.Producer.Compression = k.producerCompressionCodec()
	c.Producer.Timeout = k.ProducerTimeout
	if k.ProducerIdempotent {
		c.Producer.Idempotent = true
		c.Net.MaxOpenRequests = 1 // idempotent required MaxOpenRequests=1 and it's an implicit config - cannot set by user
	}

	// Enable producer to return successes/errors on the channel for instrumentation
	c.Producer.Return.Errors = true
	c.Producer.Return.Successes = true

	if k.producerPartitioner != nil {
		c.Producer.Partitioner = k.producerPartitioner
	} else {
		c.Producer.Partitioner = NewConsistentHashPartition
	}

	// Enable consumer to return errors on the channel for instrumentation
	c.Consumer.Return.Errors = true

	c.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}

	c.Consumer.IsolationLevel = parseIsolationLevel(k.IsolationLevel)

	if k.saramaConfigHook != nil {
		k.saramaConfigHook(c)
	}

	return c
}

const (
	defaultRetryMaxBufferLength = 10_000
	defaultRetryMaxBufferBytes  = 64 << 20
)

func defaultIfZero[T int | int64](v, def T) T {
	if v == 0 {
		return def
	}
	return v
}

func (k KafkaConfig) producerCompressionCodec() sarama.CompressionCodec {
	if k.CompressionType == "" {
		return sarama.CompressionNone
	}

	codec := sarama.CompressionNone
	if err := codec.UnmarshalText([]byte(k.CompressionType)); err != nil {
		panic(fmt.Errorf("invalid kafka producer codec. input=%s err=%w", k.CompressionType, err))
	}
	return codec
}

func parseIsolationLevel(level IsolationLevel) sarama.IsolationLevel {
	switch level {
	case IsolationLevelReadCommitted:
		return sarama.ReadCommitted
	case IsolationLevelReadUncommitted:
		return sarama.ReadUncommitted
	default:
		return sarama.ReadCommitted
	}
}
