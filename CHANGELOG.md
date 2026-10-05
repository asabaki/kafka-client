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

### Added

- Sync producer (acknowledged, bounded by `ctx`) and async producer (fire and forget, bounded retry buffer, flushed
  on `Close`).
- Consumer group and batch consumer group, with graceful shutdown, `Run(ctx)`, and an opt-in per-key worker pool
  (`WithConsumerWorkers`).
- Typed handlers, retry with backoff, fallback / dead-letter topic.
- Consistent-hash partitioner (default) and opt-in active-partition partitioner that routes around failing partitions.
- Opt-in logging (`WithLogger`), pluggable trace propagation, OpenTelemetry metrics.
- `kafkagoso`: gosoline modules for consumers, shared producers and producer shutdown, configured from
  `config.dist.yml`.
