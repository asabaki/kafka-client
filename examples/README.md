kafka-client examples
===

Runnable programs, one scenario each. They live in their own Go module, so their extra dependencies (OpenTelemetry
SDK, gin) don't end up in the library's `go.mod`.

## Run them

```sh
# from the repository root: a single-node Kafka on localhost:9092
docker compose up -d

cd examples
go run ./sync-producer
```

All examples read the broker from `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`). Setting
`KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` switches them to SCRAM-SHA-512 over TLS. See
[internal/env](internal/env/env.go) for the shared config.

## Examples

| Example                                           | Shows                                                                                                                    |
|---------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| [sync-producer](sync-producer/main.go)            | typed `Message` with headers and partition key, `WithTimestamp`, raw publish, handling send errors                      |
| [async-producer](async-producer/main.go)          | fire-and-forget publishing, flush tuning, success/error callbacks, `Close` on shutdown                                   |
| [consumer](consumer/main.go)                      | consumer group with `Run(ctx)` and graceful shutdown on Ctrl-C                                                           |
| [batch-consumer](batch-consumer/main.go)          | batches of up to 100 or 2s, typed via a converter, bulk write                                                            |
| [typed-consumer-retry](typed-consumer-retry/main.go) | typed handler, retry with backoff for temporary errors only, dead-letter topic for what still fails                   |
| [slog-logging](slog-logging/main.go)              | opt-in logging through `log/slog`; `SARAMA_DEBUG=1` forwards sarama's own logs                                           |
| [otel-tracing](otel-tracing/main.go)              | OpenTelemetry SDK setup; one trace across producer → Kafka → consumer                                                    |
| [active-partitioner](active-partitioner/main.go)  | keep publishing while partitions are down, by routing around them (opt-in, gives up per-key ordering)                   |
| [gosoline-service](gosoline-service/main.go)      | gosoline app: consumer + batch consumer + HTTP endpoint sharing a sync producer, an async producer, all from [config.dist.yml](gosoline-service/config.dist.yml) |

Run `gosoline-service` from its own directory, because it loads `config.dist.yml` from the working directory:

```sh
cd gosoline-service
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 go run .
curl -XPOST 'localhost:8088/orders?id=order-1' -d '{"id":"order-1"}'
```

## Topics

The local broker auto-creates topics on first use, with one partition. `active-partitioner` creates its `clicks`
topic with 6 partitions itself, since rerouting needs more than one partition.
