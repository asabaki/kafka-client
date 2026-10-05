kafkagoso
===

[gosoline](https://github.com/justtrackio/gosoline) integration for [kafka-client](../README.md): consumers as kernel
modules, app-wide shared producers, and trace propagation through gosoline's `tracing` package, all configured from
`config.dist.yml`.

```sh
go get github.com/asabaki/kafka-client/kafkagoso
```

```go
import "github.com/asabaki/kafka-client/kafkagoso"
```

`kafkagoso` is its own Go module, so only applications that use gosoline pull in gosoline.

## Table of contents

- [Quick start](#quick-start)
- [What it provides](#what-it-provides)
- [Sharing producers](#sharing-producers)
- [Tracing](#tracing)
- [`config.dist.yml` reference](#configdistyml-reference)
- [Examples](#examples)

## Quick start

A service that consumes `orders`, logs each one and publishes an `order-events` message for it.

`config.dist.yml`:

```yaml
app:
  env: dev
  name: order-service

kafka_client:
  connection:
    default:
      bootstrap_servers: [ "{KAFKA_BOOTSTRAP}" ]  # e.g. KAFKA_BOOTSTRAP=localhost:9092
      sasl_enabled: false                        # local broker; see the reference for SASL/TLS
      tls_enabled: false
      logging_enabled: true
  consumer:
    orders:
      topic: orders
      group_id: order-service
  producer:
    order_events: {}                             # uses connection "default"
```

`main.go`:

```go
package main

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/justtrackio/gosoline/pkg/application"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"

	kafkaclient "github.com/asabaki/kafka-client"
	"github.com/asabaki/kafka-client/kafkagoso"
)

func main() {
	application.Run(
		application.WithConfigFile("config.dist.yml", "yml"),
		application.WithTracing, // trace_id in logs, gosoline tracer for consumer spans
		application.WithModuleFactory("orders_consumer", kafkagoso.NewConsumerModuleFactory("orders", newOrderHandler)),
		application.WithModuleFactory("kafka_producer_closer", kafkagoso.NewProducerCloserModuleFactory()),
	)
}

func newOrderHandler(ctx context.Context, config cfg.Config, logger log.Logger) (kafkaclient.MessageHandler[*sarama.ConsumerMessage], error) {
	events, err := kafkagoso.ProvideSyncProducer(ctx, config, logger, "order_events")
	if err != nil {
		return nil, fmt.Errorf("can not create order_events producer: %w", err)
	}

	return func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		logger.Info(ctx, "received order %s: %s", msg.Key, msg.Value)

		_, _, err := events.PublishRawAtLeastOnce(ctx, "order-events", string(msg.Key), []byte(`{"status":"accepted"}`), nil)

		return err // nil = mark as consumed; an error redelivers the message
	}, nil
}
```

Run it:

```sh
KAFKA_BOOTSTRAP=localhost:9092 go run .
```

What happens:

- `orders_consumer` joins group `order-service` and calls the handler for every message on `orders`.
- The handler logs with the message's `ctx`, so the log line carries `trace_id`. The published `order-events` message
  gets a `traceId` header with the **same** trace id: the trace continues across services.
- On SIGTERM the consumer finishes the message in progress, commits its offsets and leaves the group. Then
  `kafka_producer_closer` closes the producer, and the app exits with code 0.

Notes:

- gosoline's `application.Run` / `application.Default` do **not** load `config.dist.yml` or enable tracing by
  themselves. Add `WithConfigFile` and `WithTracing` as shown.
- Always register `NewProducerCloserModuleFactory()` when you use `Provide*Producer`. Without it, producers are never
  closed, and an async producer loses its buffered messages on exit.
- For a batch consumer, set `batch_size` on the consumer and use `NewBatchConsumerModuleFactory` with a
  `kafkaclient.MessagesHandler`. Publishing without consuming works the same way: call `Provide*Producer` from any
  module factory or HTTP router definer.

## What it provides

- `NewConsumerModuleFactory(name, handlerFactory)` / `NewBatchConsumerModuleFactory(name, handlerFactory)` return a
  `kernel.ModuleFactory` that runs the consumer configured under `kafka_client.consumer.<name>`. The module is
  *essential* (if the consumer gives up reconnecting, the app stops), runs in the application stage, and reports
  unhealthy while the consumer is not running.
- `ProvideSyncProducer(ctx, config, logger, name)` / `ProvideAsyncProducer(...)` return the producer configured under
  `kafka_client.producer.<name>`. It is created on first use and shared by every caller in the app (via `appctx`).
- `NewProducerCloserModuleFactory()` is a background module in the service stage. On shutdown it flushes and closes
  every provided producer, after the application-stage modules (consumers, HTTP handlers) have stopped.

## Sharing producers

Inside `WithModuleMultiFactory`, every factory receives the same app `ctx`. That means
`ProvideSyncProducer(ctx, ..., "x")` returns one shared instance, which you can hand to an HTTP router and to a
consumer handler.

## Tracing

Tracing uses gosoline's `tracing` package:

- **Producers** write the trace in `ctx` to the `traceId` message header as `Root=<trace-id>;Parent=<span-id>;Sampled=0|1`.
  This is the same attribute gosoline's stream encoder uses, so traces also flow to and from other gosoline services
  that carry `traceId` in their message attributes.
- **Consumers** read `traceId` and put the trace into the handler `ctx`. Log calls with that `ctx` get a `trace_id`
  field (enabled by `application.WithTracing`).
- **Spans:** each message is handled inside `tracer.StartSpanFromContext(ctx, span_name)`, annotated with
  `kafka_topic` / `kafka_group_id` and with `kafka_partition` / `kafka_offset` as metadata. Handler errors are recorded
  on the span. What that span does depends on the app's `tracing.provider`:

| `tracing.provider` | effect                                                                                       |
|--------------------|----------------------------------------------------------------------------------------------|
| `local` (default)  | nothing recorded or exported; only the trace id is passed along                               |
| `xray`             | an X-Ray segment per message, continuing the producer's trace                                |
| `otel`             | an OpenTelemetry span per message, exported by gosoline's OTel setup                         |
| `noop`             | tracing disabled                                                                             |

For batch consumers, each message gets its own span (continuing its own trace), available through
`MessageWithContext.Get()`. All spans of a batch finish when the batch handler returns.

## `config.dist.yml` reference

Everything lives under `kafka_client`. Three maps are keyed by a name you choose: `connection`, `consumer` and `producer`.
Consumers and producers pick a connection by name. The values below are the defaults: omit a key to use them.
gosoline's `{KEY}` placeholders and env overrides work as usual. Without an env key prefix, `{KAFKA_BOOTSTRAP}` reads
the env var `KAFKA_BOOTSTRAP`, and any key can be overridden, e.g. `KAFKA_CLIENT_CONNECTION_DEFAULT_SASL_PASSWORD`.
With `application.WithConfigEnvKeyPrefix("order-service")` every env var gets the `ORDER_SERVICE_` prefix, including
the ones behind placeholders: `ORDER_SERVICE_KAFKA_BOOTSTRAP`,
`ORDER_SERVICE_KAFKA_CLIENT_CONNECTION_DEFAULT_SASL_PASSWORD`.

```yaml
kafka_client:
  connection:
    default:
      bootstrap_servers: [ "{KAFKA_BOOTSTRAP}" ]  # required
      sasl_mechanisms: SCRAM-SHA-512
      sasl_username: "{KAFKA_USERNAME}"
      sasl_password: "{KAFKA_PASSWORD}"
      logging_enabled: true
  consumer:
    orders:
      topic: shop.orders                         # required
      group_id: order-service                    # required
  producer:
    notifications: {}                            # uses connection "default"
```

### `kafka_client.connection.<name>`

**Connection & security**

| key                      | default          | values / meaning                                                                                                                                                                            |
|--------------------------|------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `bootstrap_servers`      | — (required)     | list of `host:port` brokers used for the initial connection                                                                                                                                  |
| `client_id`              | hostname         | client id sent with every request (broker logs, quotas). Consumers append `-<group_id>`                                                                                                     |
| `sasl_enabled`           | `true`           | authenticate with SASL                                                                                                                                                                      |
| `sasl_mechanisms`        | `PLAIN`          | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` (SCRAM is built in). `OAUTHBEARER` / `GSSAPI` would also need a token provider / Kerberos setup, which this library doesn't configure, so don't use them |
| `sasl_username`          | `""`             | SASL user                                                                                                                                                                                   |
| `sasl_password`          | `""`             | SASL password                                                                                                                                                                               |
| `tls_enabled`            | `true`           | use TLS to the brokers                                                                                                                                                                      |
| `insecure_skip_verify`   | `false`          | skip broker certificate verification (only with TLS; don't use in prod)                                                                                                                     |
| `tls_config_server_name` | `""`             | TLS SNI / expected certificate host name; empty = derived from the broker address                                                                                                           |
| `keepalive`              | `0s`             | TCP keep-alive period for broker connections; `0` = OS default                                                                                                                              |
| `net_dial_timeout`, `net_read_timeout`, `net_write_timeout` | `0s` (30s) | per connection attempt / request. A broker that vanished without closing connections is noticed only when these expire: lower them (e.g. `5s`) where brokers can disappear |

**Producer** (only used by producers)

| key                           | default   | values / meaning                                                                                                                                                                                                                  |
|-------------------------------|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `producer_required_acks`      | `-1`      | `-1` = all in-sync replicas must acknowledge (safest, needed for idempotence), `1` = leader only, `0` = no acknowledgement (fire and forget)                                                                                      |
| `producer_idempotent`         | `true`    | broker de-duplicates retried sends, which keeps order per partition. Requires `producer_required_acks: -1` and `producer_retry_max >= 1`, and limits in-flight requests to 1. Set `false` for `required_acks` `0`/`1`, or the producer fails to start |
| `producer_retry_max`          | `3`       | how many times a failed send is retried before the error is returned (sync) or reported (async)                                                                                                                                   |
| `producer_retry_backoff`      | `100ms`   | wait between retries                                                                                                                                                                                                              |
| `producer_retry_max_buffer_length`, `producer_retry_max_buffer_bytes` | `0` (10000 / 64 MiB) | messages held in memory for retry while partitions are unavailable; when full the oldest fail (async: error handler; sync: returned). Negative = unlimited |
| `producer_timeout`            | `10s`     | how long the broker waits for `required_acks` before it fails the request                                                                                                                                                         |
| `compression_type`            | `lz4`     | `none`, `gzip`, `snappy`, `lz4`, `zstd`. An unknown value panics at startup                                                                                                                                                        |
| `producer_flush_frequency`    | `100ms`   | send buffered messages at least this often                                                                                                                                                                                        |
| `producer_flush_messages`     | `0`       | send as soon as this many messages are buffered; `0` = no count trigger                                                                                                                                                           |
| `producer_flush_bytes`        | `1048576` | send as soon as this many bytes are buffered                                                                                                                                                                                      |
| `producer_flush_max_messages` | `0`       | hard cap of messages per request; `0` = unlimited                                                                                                                                                                                 |
| `producer_heartbeat_enabled`  | `false`   | sync producers only: periodically ping every connected broker, so the first message after a long idle period doesn't pay for a reconnect                                                                                          |
| `producer_heartbeat_interval` | `5m`      | ping interval; keep it below the broker's `connections.max.idle.ms` (default 10m)                                                                                                                                                 |

The three flush triggers work together: the first one reached sends the batch. A sync producer waits up to
`producer_flush_frequency` for its message to be sent, so lower it (or set `producer_flush_messages: 1`) if latency
matters more than throughput.

**Consumer** (only used by consumers)

| key                        | default          | values / meaning                                                                                                                                          |
|----------------------------|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `isolation_level`          | `READ_COMMITTED` | `READ_COMMITTED` = skip messages of aborted/open transactions, `READ_UNCOMMITTED` = read everything. Any other value falls back to `READ_COMMITTED`       |
| `consumer_group_max_retry` | `10`             | how many times in a row the consumer tries to rejoin the group after an error (waiting 1s, 2s, 3s, …). When exhausted, the consumer stops and so does the app |
| `slow_message_threshold`   | `5s`             | messages whose publish-to-processed time is at least this long are logged as `slow message` (only with `logging_enabled`)                                 |

**Observability & behaviour**

| key                 | default    | values / meaning                                                                                                                                                                                                   |
|---------------------|------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `logging_enabled`   | `false`    | opt-in: kafka-client logs (connection retries, handler errors, slow messages, async produce errors, heartbeat) go to the gosoline logger on channel `kafka-client`, with a `kafka_connection` field                  |
| `debug`             | `false`    | also forward sarama's internal logs at debug level. Needs `logging_enabled`. Very verbose. sarama has a single process-wide logger, so the last connection that enables it wins                                   |
| `trace_propagation` | `gosoline` | which headers carry the trace: `gosoline` = `traceId` header (see Tracing), `otel` = W3C `traceparent` via OpenTelemetry's global propagator, `all` = both, `none` = no trace headers are written or read        |
| `metrics_disabled`  | `false`    | `true` turns off sarama's internal go-metrics for the whole process (saves CPU; nothing in gosoline reads them). The `kafka_client_*` OpenTelemetry histograms are always recorded                                 |

`auto_offset_reset` and `enable_auto_commit` are accepted but **have no effect**. Where a new group starts reading is
set by the consumer's `ignore_old_messages`, and offsets are always committed automatically (every second) after the
handler succeeds.

### `kafka_client.consumer.<name>`

| key                   | default                   | values / meaning                                                                                                                                                                                                   |
|-----------------------|---------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `connection`          | `default`                 | name of the `kafka_client.connection` entry to use                                                                                                                                                                 |
| `topic`               | — (required)              | topic to consume                                                                                                                                                                                                   |
| `group_id`            | — (required)              | consumer group id. Committed offsets are stored per group, so changing it makes the service start over according to `ignore_old_messages`                                                                         |
| `ignore_old_messages` | `false`                   | only matters when the group has no committed offset yet: `false` = start from the oldest retained message, `true` = start from new messages only                                                                   |
| `batch_size`          | `0`                       | `0` = handle one message at a time (`NewConsumerModuleFactory`); `> 0` = hand the handler up to this many messages at once (`NewBatchConsumerModuleFactory`). Using the wrong factory fails at startup           |
| `batch_timeout`       | `1s`                      | batch consumers only: hand over a partial batch after this long                                                                                                                                                    |
| `worker_count`        | `1`                       | single-message consumers only: handlers per partition. Messages with the same key go to the same handler in order; offsets are committed only up to the oldest unfinished message. Handlers must be concurrency-safe |
| `span_enabled`        | `true`                    | wrap every message in a gosoline tracing span (see Tracing)                                                                                                                                                        |
| `span_name`           | `kafka-consumer-<name>`   | span name                                                                                                                                                                                                          |

Delivery semantics: a message is marked as consumed only after the handler returns `nil`. If the handler returns an
error, the session ends and the consumer rejoins the group. The failed message is redelivered, so an error that keeps
happening will block that partition. To move on instead, wrap the handler in `kafkaclient.WrapWithFallbackHandler`
with `kafkaclient.DeadLetterTopic(...)` (or your own fallback, e.g. a queue), optionally with
`WrapWithRetryBackoffHandler(..., RetryConfig{ReturnError: true, ...})` in front for temporary errors. See
[Fallback and dead-letter topic](../README.md#fallback-and-dead-letter-topic).

### `kafka_client.producer.<name>`

| key                              | default           | values / meaning                                                                                                                                                                   |
|----------------------------------|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `connection`                     | `default`         | name of the `kafka_client.connection` entry to use; connection-level producer tuning (acks, retries, flush, compression) lives there                                                |
| `partitioner`                    | `consistent_hash` | `consistent_hash` = the key's partition, always. `active_partition` = route around partitions that keep failing; **gives up per-key ordering** during failures. `sarama_hash` = sarama's FNV-1a hash, for a key → partition mapping shared with other sarama producers |
| `circuit_breaker.max_failures`   | `3`               | `active_partition` only: consecutive failed sends that take a partition out of rotation (≥ 1)                                                                                       |
| `circuit_breaker.open_duration`  | `30s`             | `active_partition` only: how long a partition stays out of rotation before it gets traffic again (> 0)                                                                              |

The same name gives a sync or an async producer depending on which `Provide...Producer` you call. Topic, key and
headers are set per message. An unknown `partitioner` or an invalid circuit breaker value fails at startup.

`active_partition` is meant for brokers that can disappear, like spot instances: publishing continues on the remaining
partitions while Kafka elects new leaders. The messages that already failed are not moved, so pair it with error
handling that stores them elsewhere. Details: [Partitioning](../README.md#partitioning).

```yaml
kafka_client:
  producer:
    clicks:
      partitioner: active_partition
      circuit_breaker:
        max_failures: 3
        open_duration: 30s
```

## Examples

[examples/gosoline-service](../examples/gosoline-service/main.go) is a complete service: a consumer and a batch
consumer, an HTTP endpoint sharing a sync producer with the consumer, an async producer, and an
`active_partition` producer, configured in [config.dist.yml](../examples/gosoline-service/config.dist.yml).
