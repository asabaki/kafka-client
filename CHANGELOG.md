Changelog
===

All notable changes are listed here. The project follows [Semantic Versioning](https://semver.org/); until v1.0.0,
minor versions may contain breaking changes.

## Unreleased

First public version.

### Breaking

- Removed `XDGSCRAMClient`, `SHA256` and `SHA512`: SCRAM is configured from `SaslMechanisms`, the client is internal.
- `MessageOption` is unexported; use `Option` values such as `WithTimestamp`.
- `RecordHeaderKeyPartitionKey` is a constant.

### Fixed

- A consumer group whose join failed (for example, the group coordinator broker died) never joined again: every
  retry returned the same error, and the consumer stopped after `ConsumerGroupMaxRetry` attempts. It now rejoins
  once Kafka has moved the group to another broker, about 10s after the broker dies.

- A handler panic crashed the process from a sarama goroutine. It now returns `ErrHandlerPanic` (with the stack) as
  the handler's error, so the message is redelivered, or parked by `WrapWithFallbackHandler`.
- A member that owns no partitions never reset its failure count, so retries spread over its whole lifetime added up
  to `ConsumerGroupMaxRetry` and stopped it. The count now resets on every successful join.

- Stopping a consumer while it waited to retry a failed join started one more join attempt, which could hold the
  shutdown up on unreachable brokers.

### Added

- `ConsumerBalanceStrategies` (`consumer_balance_strategies`), including `cooperative-sticky`, and the group timeouts
  `ConsumerSessionTimeout`, `ConsumerHeartbeatInterval`, `ConsumerRebalanceTimeout`. Every constructor rejects an
  unknown strategy name, producers included, since they share the connection config.

- Sync producer (acknowledged, bounded by `ctx`) and async producer (fire and forget, bounded retry buffer, flushed
  on `Close`).
- Consumer group and batch consumer group, with graceful shutdown, `Run(ctx)`, and an opt-in per-key worker pool
  (`WithConsumerWorkers`).
- Typed handlers, retry with backoff, fallback / dead-letter topic.
- Consistent-hash partitioner (default) and opt-in active-partition partitioner that routes around failing partitions.
- Opt-in logging (`WithLogger`), pluggable trace propagation, OpenTelemetry metrics.
- `kafkagoso`: gosoline modules for consumers, shared producers and producer shutdown, configured from
  `config.dist.yml`.
