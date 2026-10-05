kafka-client
===

[![CI](https://github.com/asabaki/kafka-client/actions/workflows/ci.yml/badge.svg)](https://github.com/asabaki/kafka-client/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/asabaki/kafka-client.svg)](https://pkg.go.dev/github.com/asabaki/kafka-client)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A small, use-case oriented Kafka client for Go, built on [IBM/sarama](https://github.com/IBM/sarama).
It covers the common cases (publish a message, consume a topic in a consumer group) and leaves the rest of Kafka
to sarama, which stays reachable through `Unwrap()` and `ToSaramaConfig()`.

Using [gosoline](https://github.com/justtrackio/gosoline)? See **[kafkagoso](kafkagoso/README.md)** for consumers as
kernel modules, shared producers, gosoline trace propagation and the `config.dist.yml` reference.

## Table of contents

- [Features](#features)
- [Install](#install)
- [Quick start](#quick-start)
  - [Sync producer: wait for the broker](#sync-producer-wait-for-the-broker)
  - [Async producer: fire and forget](#async-producer-fire-and-forget)
  - [Consumer](#consumer)
- [API](#api)
  - [Configuration: `KafkaConfig`](#configuration-kafkaconfig)
  - [Options: `KafkaConfigOption`](#options-kafkaconfigoption)
  - [Sync producer](#sync-producer)
  - [Async producer](#async-producer)
  - [Messages](#messages)
  - [Consumer group](#consumer-group)
  - [Consumer workers](#consumer-workers)
  - [Batch consumer group](#batch-consumer-group)
  - [Handlers and converters](#handlers-and-converters)
  - [Retry with backoff](#retry-with-backoff)
  - [Fallback and dead-letter topic](#fallback-and-dead-letter-topic)
  - [Partitioning](#partitioning)
  - [Producer heartbeat](#producer-heartbeat)
- [Logging](#logging)
- [Tracing and metrics](#tracing-and-metrics)
- [Delivery guarantees](#delivery-guarantees)
- [Examples](#examples)
- [Development](#development)
- [License](#license)

## Features

- **Sync producer** (waits for the broker's acknowledgement, bounded by `ctx`) and **async producer** (fire and forget,
  bounded retry buffer, flushed on `Close`).
- **Consumer group** handling one message at a time, and **batch consumer group** handling up to N messages at once.
- Typed handlers through a converter, **retry with exponential backoff**, and a **fallback / dead-letter topic** so a
  bad message never blocks its partition.
- Optional **worker pool** per partition that keeps per-key ordering.
- **Consistent-hash partitioner** with a partition key that can differ from the message key, and an opt-in
  **active-partition partitioner** that keeps publishing while partitions are down.
- **No logging unless you opt in** with `WithLogger(logger)`.
- **Trace context in message headers** (OpenTelemetry by default, pluggable), plus `kafka_client_*` producer, consumer
  and partitioner metrics.
- **Producer heartbeat** keeps idle broker connections alive.
- SASL (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`) and TLS.

## Install

```sh
go get github.com/asabaki/kafka-client
go get github.com/asabaki/kafka-client/kafkagoso   # optional: gosoline integration (separate module)
```

Requires Go 1.26+. The gosoline integration is a separate module, so the library itself doesn't depend on gosoline.

```go
import kafkaclient "github.com/asabaki/kafka-client"
```

## Quick start

Shared config for all examples below:

```go
cfg := kafkaclient.KafkaConfig{
	BootstrapServers: []string{"localhost:9092"},
	SaslEnabled:      true,
	SaslMechanisms:   "SCRAM-SHA-512",
	SaslUsername:     os.Getenv("KAFKA_USERNAME"),
	SaslPassword:     os.Getenv("KAFKA_PASSWORD"),
	TlsEnabled:       true,
	// A zero-value field means zero, not the documented default: set at least these for producers.
	ProducerRequiredAcks: -1,
	ProducerRetryMax:     3,
	ProducerTimeout:      10 * time.Second,
	ProducerIdempotent:   true,
}
```

### Sync producer: wait for the broker

```go
producer, err := kafkaclient.NewSyncProducer(cfg)
if err != nil {
	return err
}
defer producer.Close()

partition, offset, err := producer.PublishRawAtLeastOnce(ctx, "orders", "order-42", []byte(`{"id":42}`), nil)
if err != nil {
	return err // not stored: retry, or report upstream
}
fmt.Printf("stored at partition %d, offset %d\n", partition, offset)
```

### Async producer: fire and forget

```go
producer, err := kafkaclient.NewAsyncProducer(cfg,
	kafkaclient.WithAsyncErrorHandler(func(pe *sarama.ProducerError) {
		fmt.Printf("failed to deliver to %s: %s\n", pe.Msg.Topic, pe.Err)
	}),
)
if err != nil {
	return err
}
defer producer.Close() // required: flushes buffered messages and waits for the error handler

producer.PublishRawAtMostOnce(ctx, "page-views", "user-7", []byte(`{"path":"/"}`), nil) // returns immediately
```

### Consumer

```go
consumer, err := kafkaclient.NewConsumerGroup(cfg, "order-service", "orders", false,
	func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		fmt.Printf("got %s\n", msg.Value)
		return nil // nil = mark as consumed; an error redelivers the message
	},
)
if err != nil {
	return err
}
return consumer.Run(ctx) // blocks until ctx is done, then closes the consumer
```

For typed messages, batches and retry, see [Messages](#messages), [Batch consumer group](#batch-consumer-group) and
[Retry with backoff](#retry-with-backoff).

## API

### Configuration: `KafkaConfig`

`KafkaConfig` is a plain struct, used by every constructor. Each field has `env` / `envconfig` tags (for
[caarlos0/env](https://github.com/caarlos0/env) and [kelseyhightower/envconfig](https://github.com/kelseyhightower/envconfig))
and a `cfg` tag (for gosoline). The `default` tags are applied **only** by those loaders. If you build the struct in
code, an unset field is its Go zero value.

| Field                                               | env                                   | default          | Meaning                                                                                                              |
|-----------------------------------------------------|---------------------------------------|------------------|----------------------------------------------------------------------------------------------------------------------|
| `BootstrapServers []string`                         | `KAFKA_BOOTSTRAP_SERVERS`             | — (required)     | brokers for the initial connection                                                                                   |
| `ClientID string`                                   | `KAFKA_CLIENT_ID`                     | hostname         | sent with every request; consumers append `-<group>`                                                                 |
| `SaslEnabled bool`                                  | `KAFKA_SASL_ENABLED`                  | `true`           | authenticate with SASL                                                                                               |
| `SaslMechanisms string`                             | `KAFKA_SASL_MECHANISMS`               | `PLAIN`          | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`                                                                            |
| `SaslUsername`, `SaslPassword string`               | `KAFKA_SASL_USERNAME` / `_PASSWORD`   | `""`             | credentials                                                                                                          |
| `TlsEnabled bool`                                   | `KAFKA_TLS_ENABLED`                   | `true`           | TLS to the brokers                                                                                                   |
| `InsecureSkipVerify bool`                           | `KAFKA_INSECURE_SKIP_VERIFY`          | `false`          | skip certificate verification                                                                                        |
| `TLSConfigServerName string`                        | `KAFKA_TLS_CONFIG_SERVER_NAME`        | `""`             | expected certificate host name (SNI)                                                                                 |
| `KeepAlive time.Duration`                           | `KAFKA_KEEPALIVE`                     | `0` (OS default) | TCP keep-alive period                                                                                                |
| `NetDialTimeout`, `NetReadTimeout`, `NetWriteTimeout time.Duration` | `KAFKA_NET_DIAL_TIMEOUT` / `_READ_` / `_WRITE_` | `0` (30s) | per connection attempt / request. A broker that vanished without closing connections is noticed only when these expire, so lower them (e.g. 5s) where brokers can disappear |
| `ProducerRequiredAcks int16`                        | `KAFKA_PRODUCER_REQUIRED_ACKS`        | `-1`             | `-1` all in-sync replicas, `1` leader only, `0` none                                                                 |
| `ProducerIdempotent bool`                           | `KAFKA_PRODUCER_IDEMPOTENT`           | `true`           | broker de-duplicates retries; needs acks `-1` and retry max `>= 1`, and forces 1 in-flight request                   |
| `ProducerRetryMax int`                              | `KAFKA_PRODUCER_RETRY_MAX`            | `3`              | send retries                                                                                                         |
| `ProducerRetryBackoff time.Duration`                | `KAFKA_PRODUCER_RETRY_BACKOFF`        | `100ms`          | wait between retries                                                                                                 |
| `ProducerRetryMaxBufferLength int`, `ProducerRetryMaxBufferBytes int64` | `KAFKA_PRODUCER_RETRY_MAX_BUFFER_LENGTH` / `_BYTES` | `0` (10000 / 64 MiB) | messages held in memory for retry; oldest fail with `ErrProducerRetryBufferOverflow` when full. Negative = unlimited; sarama raises values below 4096 / 32 MiB |
| `ProducerTimeout time.Duration`                     | `KAFKA_PRODUCER_TIMEOUT`              | `10s`            | broker-side wait for acks; **must be > 0**                                                                           |
| `CompressionType string`                            | `KAFKA_COMPRESSION_TYPE`              | `lz4`            | `none`, `gzip`, `snappy`, `lz4`, `zstd` (`""` = none; unknown panics)                                                |
| `ProducerFlushFrequency time.Duration`              | `KAFKA_PRODUCER_FLUSH_FREQUENCY`      | `100ms`          | send buffered messages at least this often                                                                           |
| `ProducerFlushMessages int`                         | `KAFKA_PRODUCER_FLUSH_MESSAGES`       | `0`              | send once this many are buffered (`0` = off)                                                                         |
| `ProducerFlushBytes int`                            | `KAFKA_PRODUCER_FLUSH_BYTES`          | `1048576`        | send once this many bytes are buffered                                                                               |
| `ProducerFlushMaxMessages int`                      | `KAFKA_PRODUCER_FLUSH_MAX_MESSAGES`   | `0`              | max messages per request (`0` = unlimited)                                                                           |
| `ProducerHeartbeatEnabled bool`                     | `KAFKA_PRODUCER_HEARTBEAT_ENABLED`    | `false`          | see [Producer heartbeat](#producer-heartbeat)                                                                        |
| `ProducerHeartbeatInterval time.Duration`           | `KAFKA_PRODUCER_HEARTBEAT_INTERVAL`   | `5m`             | heartbeat interval; must be > 0 when enabled                                                                         |
| `IsolationLevel IsolationLevel`                     | `KAFKA_ISOLATION_LEVEL`               | `READ_COMMITTED` | `IsolationLevelReadCommitted` / `IsolationLevelReadUncommitted`                                                      |
| `ConsumerGroupMaxRetry int`                         | `KAFKA_CONSUMER_GROUP_MAX_RETRY`      | `10`             | consecutive rejoin attempts before a consumer gives up                                                               |
| `SlowMessageThreshold time.Duration`                | `KAFKA_SLOW_MESSAGE_THRESHOLD`        | `5s`             | publish-to-processed time that gets a `slow message` warning (with a logger)                                         |
| `Debug bool`                                        | `SARAMA_DEBUG`                        | `false`          | forward sarama's internal logs to the logger (needs `WithLogger`)                                                    |
| `MetricsDisabled bool`                              | `KAFKA_METRICS_DISABLED`              | `false`          | turn off sarama's go-metrics for the whole process                                                                   |
| `AutoOffsetReset`, `EnableAutoCommit`               | —                                     | —                | accepted, **no effect** (see [Consumer group](#consumer-group))                                                      |

`func (k KafkaConfig) ToSaramaConfig(opts ...KafkaConfigOption) *sarama.Config` returns the sarama config the
library would use, e.g. to create a `sarama.ClusterAdmin` with identical connection settings.

### Options: `KafkaConfigOption`

Every constructor accepts options after its required arguments.

| Option                                                  | Applies to      | Effect                                                                                                    |
|---------------------------------------------------------|-----------------|-----------------------------------------------------------------------------------------------------------|
| `WithLogger(Logger)`                                    | all             | opt in to logging (see [Logging](#logging))                                                               |
| `WithTracePropagator(propagation.TextMapPropagator)`    | all             | how trace context is written to / read from headers (default `OTelGlobalPropagator()`)                    |
| `WithTLSConfig(*tls.Config)`                            | all             | use this TLS config instead of the one built from `InsecureSkipVerify` / `TLSConfigServerName`            |
| `WithPartitioner(sarama.PartitionerConstructor)`        | producers       | custom partitioner (default: [consistent hash](#partitioning))                                            |
| `WithDefaultPartitioner()`                              | producers       | sarama's hash partitioner (FNV-1a on the key, same as the Java client's legacy behaviour)                 |
| `WithProducerIdempotent(bool)`                          | producers       | override `ProducerIdempotent`                                                                             |
| `WithAsyncSuccessHandler(func(*sarama.ProducerMessage))`| async producer  | called for every acknowledged message                                                                     |
| `WithAsyncErrorHandler(func(*sarama.ProducerError))`    | async producer  | called for every failed message (default: log it, if a logger is set)                                     |
| `WithConsumerWorkers(n)`                                | consumer group  | handle each partition with `n` goroutines, routed by key (see [Consumer workers](#consumer-workers))       |

### Sync producer

```go
func NewSyncProducer(config KafkaConfig, opts ...KafkaConfigOption) (SyncProducer, error)

type SyncProducer interface {
	PublishRawAtLeastOnce(ctx context.Context, topic, messageKey string, payload []byte, headers map[string]string) (partition int32, offset int64, err error)
	PublishAtLeastOnce(ctx context.Context, msg Message, opts ...Option) (partition int32, offset int64, err error)
	Close() error
	Unwrap() sarama.Client
}
```

- Each call blocks until the broker acknowledges the message according to `ProducerRequiredAcks`, then returns the
  partition and offset it was written to. On failure (after `ProducerRetryMax` retries) it returns the error.
- **`ctx` bounds the call.** When `ctx` is canceled or its deadline passes, the call returns `ctx.Err()` right away.
  - Canceled before the message was queued: it was not sent.
  - Canceled after: the outcome is unknown. The message may still be delivered, and a retry can create a duplicate.
  - Use a deadline to fail fast during a broker outage, e.g. in an HTTP handler, instead of waiting for
    `NetReadTimeout` × retries.
- The trace context in `ctx` is written into the message headers (see [Tracing](#tracing-and-metrics)).
- Concurrent calls are safe, and are batched together on the wire. A single call may wait up to
  `ProducerFlushFrequency` before its batch is sent. Set `ProducerFlushMessages: 1` if latency matters more than throughput.
- `Close()` stops the heartbeat, waits for in-flight messages (including ones whose caller stopped waiting), then
  closes the underlying client.
- `Unwrap()` returns the underlying `sarama.Client` (metadata, brokers, partitions).

```go
ctx, cancel := context.WithTimeout(r.Context(), time.Second)
defer cancel()

_, _, err := producer.PublishAtLeastOnce(ctx, order, kafkaclient.WithTimestamp(order.CreatedAt))
if errors.Is(err, context.DeadlineExceeded) {
	// outcome unknown: store it in a fallback, or tell the caller to retry idempotently
}
```

### Async producer

```go
func NewAsyncProducer(config KafkaConfig, opts ...KafkaConfigOption) (AsyncProducer, error)

type AsyncProducer interface {
	PublishRawAtMostOnce(ctx context.Context, topic, messageKey string, payload []byte, headers map[string]string)
	PublishAtMostOnce(ctx context.Context, msg Message, opts ...Option)
	Close() error
	Unwrap() sarama.Client
}
```

- Publishing puts the message in sarama's input queue and returns. It blocks only while that queue is full
  (backpressure), and at most until `ctx` ends.
- There is no return value. Every message ends in exactly one delivery report:
  - `WithAsyncSuccessHandler` when it's acknowledged;
  - `WithAsyncErrorHandler` when delivery failed, when `KafkaMessagePayload()` failed, or when `ctx` ended before it
    could be queued (the error is `ctx.Err()`).
  The default error handler logs, if a logger is set. Put a fallback (e.g. a durable queue) in your error handler if
  messages must not be lost.
- While partitions are unavailable, messages waiting for a retry are held in memory, up to
  `ProducerRetryMaxBufferLength` / `ProducerRetryMaxBufferBytes` (10000 messages / 64 MiB by default). Past that, the
  oldest fail with `sarama.ErrProducerRetryBufferOverflow` through the error handler, instead of growing until the
  process runs out of memory.
- `Close()` **must** be called on shutdown. It flushes every buffered message, waits until all success and error
  callbacks have run, then closes the client. Messages still buffered when the process exits without `Close()` are lost.
- Do not publish after (or concurrently with) `Close()`.

```go
producer, err := kafkaclient.NewAsyncProducer(cfg,
	kafkaclient.WithAsyncErrorHandler(func(pe *sarama.ProducerError) {
		failedSends.Inc()
	}),
)
```

### Messages

Publish your own types by implementing `Message`. The optional interfaces add headers and a partition key.

```go
type Message interface {
	KafkaTopic() string
	KafkaMessageKey() string              // "" = no key (random partition)
	KafkaMessagePayload() ([]byte, error) // message value
}

type MessageWithHeader interface {
	KafkaMessageHeaders() map[string]string
}

type MessageWithPartitionKey interface {
	KafkaPartitionKey() string // sent as header "partition_key", see Partitioning
}
```

`Option` values tune a single publish:

| Option                          | Effect                                              |
|---------------------------------|-----------------------------------------------------|
| `WithTimestamp(time.Time)`      | message timestamp (default: now)                    |

```go
type OrderCreated struct {
	ID       string `json:"id"`
	Customer string `json:"customer"`
}

func (o *OrderCreated) KafkaTopic() string                   { return "orders" }
func (o *OrderCreated) KafkaMessageKey() string              { return o.ID }
func (o *OrderCreated) KafkaMessagePayload() ([]byte, error) { return json.Marshal(o) }
func (o *OrderCreated) KafkaPartitionKey() string            { return o.Customer } // keep one customer's orders in order
```

### Consumer group

```go
func NewConsumerGroup(
	cfg KafkaConfig,
	consumerGroup string,
	topic string,
	ignoreOldMessage bool,
	handler MessageHandler[*sarama.ConsumerMessage],
	opts ...KafkaConfigOption,
) (ConsumerGroup, error)

type MessageHandler[T any] func(ctx context.Context, msg T) error
```

Arguments:

- `consumerGroup`: the group id. Kafka stores committed offsets per group; members of the same group share the topic's
  partitions. You can run at most as many useful members as there are partitions.
- `topic`: one topic per consumer group.
- `ignoreOldMessage`: only used when the group has **no committed offset yet**. `false` starts from the oldest retained
  message, `true` starts from new messages only. Once offsets exist, consumption always resumes from them.
- `handler`: called once per message, sequentially per partition (different partitions run concurrently). `ctx`
  carries the trace context from the message headers.
  - returns `nil`: the message is marked as consumed. Offsets are committed automatically every second.
  - returns an error: the session ends, the consumer rejoins the group, and the message is redelivered. See
    [Delivery guarantees](#delivery-guarantees).

```go
type ConsumerGroup interface {
	Start() error                  // start consuming in the background; error if already running
	Run(ctx context.Context) error // Start, block until ctx is done or the consumer stops, then Close
	Close()                        // stop consuming and release the connection; safe without Start
	IsRunning() bool
	Health() error                 // non-nil while not running, for health endpoints
}
```

- Use `Run(ctx)` when you have a context to tie the lifetime to. It returns `nil` on `ctx` shutdown, or an error if the
  consumer gave up.
- Use `Start()` + `Close()` otherwise.
- Connection errors are retried up to `ConsumerGroupMaxRetry` times in a row, waiting 1s, 2s, 3s, …. After that the
  consumer stops, `IsRunning()` turns false and `Run` returns the error.
- A consumer cannot be restarted after `Close()`; create a new one.

### Consumer workers

By default each partition is handled by one goroutine, one message at a time. For slow handlers (remote calls, database
writes), `WithConsumerWorkers(n)` runs `n` handlers per partition:

```go
consumer, err := kafkaclient.NewConsumerGroup(cfg, "order-service", "orders", false, handler,
	kafkaclient.WithConsumerWorkers(8),
)
```

- Messages are routed by key (the partition key header, else the message key). All messages with the same key go to
  the same worker, in order, so **per-key ordering is kept**. Messages without a key are spread over the workers.
- Offsets are committed only up to the oldest message that hasn't finished, so a crash never skips a message. After
  a crash, messages that finished after that point are delivered again.
- `n` is per partition: a member that owns 6 partitions with `n: 8` runs up to 48 handlers. Handlers must be safe for
  concurrent use.
- When a handler fails, no new messages start, the running ones finish, and the session ends: the failed message and
  everything after it that wasn't committed is redelivered.
- Not available for batch consumers.

### Batch consumer group

```go
func NewBatchConsumerGroup(
	cfg KafkaConfig,
	groupName string,
	topic string,
	batchSize uint16,
	batchTimeout time.Duration,
	ignoreOldMessage bool,
	handler MessagesHandler[*sarama.ConsumerMessage],
	opts ...KafkaConfigOption,
) (ConsumerGroup, error)

type MessagesHandler[T any] func(messages []*MessageWithContext[T]) error

func (m *MessageWithContext[T]) Get() (context.Context, T)
```

- The handler gets a batch as soon as `batchSize` messages are buffered, or after `batchTimeout` with whatever has
  arrived (never an empty batch). Both must be > 0.
- Each element carries its own `ctx` (trace context from that message's headers): `ctx, msg := m.Get()`.
- Returning `nil` marks the **whole batch** as consumed. Returning an error redelivers the whole batch, so handlers
  should be idempotent.
- A batch can mix messages from several partitions of the topic. Order is kept within a partition.
- The returned value is the same `ConsumerGroup` as above.

```go
consumer, err := kafkaclient.NewBatchConsumerGroup(cfg, "order-archiver", "orders", 500, 2*time.Second, false,
	func(messages []*kafkaclient.MessageWithContext[*sarama.ConsumerMessage]) error {
		rows := make([]Row, 0, len(messages))
		for _, m := range messages {
			_, msg := m.Get()
			rows = append(rows, toRow(msg))
		}
		return db.BulkInsert(rows)
	},
)
```

### Handlers and converters

Decode once, and write handlers against your own type:

```go
type ConsumerMessageConverter[T any] interface {
	Convert(*sarama.ConsumerMessage) (T, error)
}
type ConsumerMessageConverterFunc[T any] func(*sarama.ConsumerMessage) (T, error)

func WrapWithSaramaMessageHandler[T any](handler MessageHandler[T], converter ConsumerMessageConverter[T]) MessageHandler[*sarama.ConsumerMessage]
func WrapWithSaramaMessagesHandler[T any](handler MessagesHandler[T], converter ConsumerMessageConverter[T]) MessagesHandler[*sarama.ConsumerMessage]
```

```go
decodeOrder := kafkaclient.ConsumerMessageConverterFunc[OrderCreated](func(m *sarama.ConsumerMessage) (OrderCreated, error) {
	var o OrderCreated
	err := json.Unmarshal(m.Value, &o)
	return o, err
})

consumer, err := kafkaclient.NewConsumerGroup(cfg, "order-service", "orders", false,
	kafkaclient.WrapWithSaramaMessageHandler(func(ctx context.Context, o OrderCreated) error {
		return handle(ctx, o)
	}, decodeOrder),
)
```

A conversion error is returned like a handler error, so the message is redelivered. Add
[retry](#retry-with-backoff) around it if you'd rather skip undecodable messages.

`NewMessageWithContext(ctx, msg)` builds a `MessageWithContext` yourself, e.g. in batch-handler middleware.

### Retry with backoff

```go
type RetryConfig struct {
	MaxRetries     int
	BaseDelay      time.Duration
	MaxDelay       time.Duration
	RetryCondition func(error) bool // nil = retry every error
	ReturnError    bool             // return the last error instead of nil when giving up
	Logger         Logger           // optional
}

func WrapWithRetryBackoffHandler(handler MessageHandler[*sarama.ConsumerMessage], cfg RetryConfig) MessageHandler[*sarama.ConsumerMessage]
func WrapWithRetryBackoffBatchHandler(handler MessagesHandler[*sarama.ConsumerMessage], cfg RetryConfig) MessagesHandler[*sarama.ConsumerMessage]
func RetryWithBackoff(cfg RetryConfig, fn func() error) error
```

- Calls the handler up to `1 + MaxRetries` times, waiting `BaseDelay × 2^attempt` (capped at `MaxDelay`) in between.
  The wait ends early when the handler's `ctx` is canceled.
- If `RetryCondition` returns false for an error, it stops retrying immediately.
- When it gives up it returns **nil** by default: the message (or batch) is marked as consumed and lost. With
  `ReturnError: true` it returns the last error instead, which you normally hand to a
  [fallback](#fallback-and-dead-letter-topic).
- The wait blocks the partition (or the worker). Keep `MaxRetries × MaxDelay` well below the group's session and
  rebalance timeouts.

### Fallback and dead-letter topic

A handler that keeps failing redelivers its message forever and blocks the partition. A fallback parks the message
somewhere else and moves on:

```go
type FallbackFunc func(ctx context.Context, msg *sarama.ConsumerMessage, handlerErr error) error

func WrapWithFallbackHandler(handler MessageHandler[*sarama.ConsumerMessage], fallback FallbackFunc) MessageHandler[*sarama.ConsumerMessage]
func WrapWithFallbackBatchHandler(handler MessagesHandler[*sarama.ConsumerMessage], fallback FallbackFunc) MessagesHandler[*sarama.ConsumerMessage]
func DeadLetterTopic(producer SyncProducer, topic string) FallbackFunc
```

- When the handler fails, `fallback` is called. If it returns nil, the message counts as consumed.
- If the fallback fails too, both errors are returned and the message is redelivered: a message is never marked
  consumed without being stored somewhere.
- `DeadLetterTopic` publishes the message unchanged (key, value, headers) to another topic and adds
  `dead-letter-error`, `dead-letter-source-topic`, `-partition` and `-offset` headers. It keeps publishing even if
  the handler's `ctx` was canceled by a shutdown.
- A `FallbackFunc` can store anywhere else: a queue, a database table, object storage.

Retry temporary errors, then dead-letter what still fails:

```go
handler := kafkaclient.WrapWithFallbackHandler(
	kafkaclient.WrapWithRetryBackoffHandler(
		kafkaclient.WrapWithSaramaMessageHandler(handleOrder, decodeOrder),
		kafkaclient.RetryConfig{
			MaxRetries: 3, BaseDelay: 100 * time.Millisecond, MaxDelay: 5 * time.Second,
			RetryCondition: isTemporary,
			ReturnError:    true,
		},
	),
	kafkaclient.DeadLetterTopic(dlqProducer, "orders.dead-letter"),
)
```

### Partitioning

Three partitioners are built in:

| Partitioner                                        | Keyed messages                                        | Keyless messages                  | Per-key ordering |
|----------------------------------------------------|-------------------------------------------------------|-----------------------------------|------------------|
| `NewConsistentHashPartition` (default)             | always the key's partition                            | random partition **with a leader** | kept             |
| `NewActivePartitionPartitioner(cfg)` (opt-in)      | the key's partition, rerouted while it keeps failing  | random partition that isn't failing | **not** kept during failures |
| sarama's hash (`WithDefaultPartitioner()`)         | FNV-1a hash of the key                                | random partition with a leader    | kept             |

**Consistent hash (default)**

- It hashes the message's **partition key** if present: the `partition_key` header, set automatically from
  `MessageWithPartitionKey`. Otherwise it hashes the **message key**.
- Messages with neither go to a random partition, chosen among partitions that currently have a leader (from the
  client's cached metadata). They skip offline partitions without any setup.
- The hash is xxhash + [jump consistent hash](https://arxiv.org/abs/1406.2294): when partitions are added, only a
  minimal share of keys moves to another partition.
- It is **not** compatible with other Kafka clients' default partitioners. If other producers write to the same topic
  and you need the same key → partition mapping, use `WithDefaultPartitioner()` or `WithPartitioner(...)`.

**Active partition (opt-in): keep publishing while partitions are down**

When a broker disappears (a reclaimed spot instance, a crashed node), the partitions it led can't be written until
Kafka elects a new leader. With the default partitioner every message for those keys fails meanwhile. The
active-partition partitioner routes around them:

```go
producer, err := kafkaclient.NewSyncProducer(cfg, kafkaclient.WithPartitioner(
	kafkaclient.NewActivePartitionPartitioner(kafkaclient.ActivePartitionConfig{
		MaxFailures:  3,                // consecutive failed sends that take a partition out of rotation
		OpenDuration: 30 * time.Second, // how long it stays out before it gets traffic again
	}),
))
```

- It keeps a circuit breaker per partition. Both producers report every send result to it.
- While a partition is healthy, it partitions exactly like the default.
- After `MaxFailures` failures in a row, the partition's breaker opens. For `OpenDuration`, messages hashed to it are
  re-hashed onto the partitions whose breaker is closed, so a key still maps to one partition in the meantime.
- After `OpenDuration`, the partition gets traffic again: one success closes the breaker, one failure re-opens it.
- If every partition is open, messages keep their normal partition.
- Failures that happen before a partition is chosen (e.g. a message larger than the client's limit) don't count.

Know before enabling it:

- **Per-key ordering breaks** while a breaker is open: one key's messages sit on two partitions and can be consumed
  out of order, or concurrently by two consumers. Use it only for topics whose consumers don't depend on that.
- **The failed messages are not moved.** sarama fixes a message's partition on the first attempt and retries there.
  Rerouting protects the messages sent after the breaker opened. The failures themselves come back as errors (sync) or
  go to `WithAsyncErrorHandler` (async); store them elsewhere, e.g. a fallback queue, if they must not be lost.
- Each producer has its own breakers. A breaker only learns from its own producer's sends.

**Custom partitioners**

`WithPartitioner(constructor)` takes any `sarama.PartitionerConstructor`. If the partitioner also implements
`PartitionFeedback`, the producers report every send result to it:

```go
type PartitionFeedback interface {
	OnSuccess(msg *sarama.ProducerMessage)
	OnError(msg *sarama.ProducerMessage, err error)
}
```

`RecordHeaderKeyPartitionKey` (`"partition_key"`) is the header name.

### Producer heartbeat

Brokers close connections that are idle longer than `connections.max.idle.ms` (default 10 minutes). The next message
then pays for a reconnect, which can take seconds. With `ProducerHeartbeatEnabled`, a **sync** producer sends a
lightweight `ApiVersions` request to every connected broker every `ProducerHeartbeatInterval`, so connections stay open.

Enable it for services that publish rarely but need low latency when they do (e.g. quiet at night). It is unnecessary
when you publish continuously.

## Logging

The library logs nothing by default. Pass a logger to opt in:

```go
type Logger interface {
	Debug(ctx context.Context, format string, args ...any)
	Info(ctx context.Context, format string, args ...any)
	Warn(ctx context.Context, format string, args ...any)
	Error(ctx context.Context, format string, args ...any)
}

producer, err := kafkaclient.NewSyncProducer(cfg, kafkaclient.WithLogger(myLogger))
```

What gets logged: consumer (re)connect retries and fatal stops, handler errors, slow messages, async producer
delivery errors, and heartbeat activity. Consumer-path logs use the message's `ctx`, so a logger that reads trace ids
from `ctx` can correlate them. With `Debug: true`, sarama's internal logs are forwarded at debug level. sarama has
a single process-wide logger, so the last client that enables it wins.

Adapting `log/slog`:

```go
type slogAdapter struct{ l *slog.Logger }

func (a slogAdapter) Debug(ctx context.Context, f string, args ...any) { a.l.DebugContext(ctx, fmt.Sprintf(f, args...)) }
func (a slogAdapter) Info(ctx context.Context, f string, args ...any)  { a.l.InfoContext(ctx, fmt.Sprintf(f, args...)) }
func (a slogAdapter) Warn(ctx context.Context, f string, args ...any)  { a.l.WarnContext(ctx, fmt.Sprintf(f, args...)) }
func (a slogAdapter) Error(ctx context.Context, f string, args ...any) { a.l.ErrorContext(ctx, fmt.Sprintf(f, args...)) }
```

## Tracing and metrics

**Trace propagation.** Producers inject the trace context from `ctx` into the message headers; consumers extract it
into the handler's `ctx`. The default is `OTelGlobalPropagator()`: it uses whatever propagator the OpenTelemetry SDK
registered, looked up on each call, so an SDK set up after the client was created still applies. Before any SDK is
set up it is a no-op. Use `WithTracePropagator(...)` for a fixed propagator, e.g.
`propagation.TraceContext{}` for W3C `traceparent`, or your own header format (see
[kafkagoso's gosoline propagator](kafkagoso/propagator.go)). The library creates no spans itself. Start one in your
handler from the provided `ctx`.

**Metrics.** Recorded on OpenTelemetry's global meter provider. Consumer metrics have the attributes `consumer_group`
and `topic`:

| Metric                                         | Type             | Meaning                                                    |
|------------------------------------------------|------------------|------------------------------------------------------------|
| `kafka_client_consumer_process_duration`       | histogram (ms)   | handler time per message                                   |
| `kafka_client_batch_consumer_process_duration` | histogram (ms)   | handler time per batch                                     |
| `kafka_client_e2e_process_duration`            | histogram (ms)   | message timestamp → handled (includes producer and queue time) |
| `kafka_client_producer_messages`               | counter          | messages handed to a producer; attributes `producer` (`sync`/`async`), `topic`, `result` (`success`/`error`/`canceled`) |
| `kafka_client_producer_publish_duration`       | histogram (ms)   | sync publish call → acknowledged or failed; same attributes |
| `kafka_client_partition_breaker_trips`         | counter          | active-partition partitioner: a partition taken out of rotation; attributes `topic`, `partition` |
| `kafka_client_partition_breakers_open`         | up/down counter  | partitions currently out of rotation; attributes `topic`, `partition` |

sarama's own go-metrics are separate; disable them with `MetricsDisabled`.

## Delivery guarantees

- **Sync producer:** at-least-once. An acknowledged message is stored; a retried send can be written twice unless
  `ProducerIdempotent` is on (which also keeps per-partition order during retries).
- **Async producer:** at-most-once from the caller's point of view. You get no error back, and anything still
  buffered when the process dies is lost.
- **Consumers:** at-least-once. A message is marked only after the handler returns `nil`, and offsets are committed in
  the background every second. After a crash or rebalance, the messages since the last commit are delivered again.
  Make handlers idempotent.
- **Handler errors block.** A handler that keeps failing on one message redelivers it forever and blocks that
  partition. Use a [fallback](#fallback-and-dead-letter-topic) to park it and move on, with
  [retry](#retry-with-backoff) in front for temporary errors.
- **Ordering** is kept per partition. Messages with the same key (or partition key) go to the same partition.

## Examples

[examples/](examples/README.md) has one runnable program per scenario: sync and async producers, consumer, batch
consumer, typed handler with retry and dead-letter topic, `log/slog` logging, OpenTelemetry tracing, the
active-partition partitioner, and a complete gosoline service.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md). In short:

```sh
docker compose up -d                       # single-node Kafka on localhost:9092
go test -race ./...                        # unit + integration tests (integration tests skip without Kafka)
(cd kafkagoso && go test -race ./...)
(cd examples && go build ./...)
golangci-lint run ./...
```

Tests that need a fixed time freeze the library's clock with `internal/timeutil`:

```go
timeutil.Freeze(t, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) // restored when the test ends
timeutil.Advance(30 * time.Second)
```

## License

[MIT](LICENSE)
