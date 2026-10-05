Contributing
===

Thanks for helping. Bug reports, fixes and small features are welcome; for larger changes, open an issue first so we
can agree on the approach.

## Layout

The repository has three Go modules, tied together by `go.work` for local development:

| Module                                      | Directory     | Contents                                         |
|---------------------------------------------|---------------|--------------------------------------------------|
| `github.com/asabaki/kafka-client`           | `.`           | the library; depends on sarama and OpenTelemetry |
| `github.com/asabaki/kafka-client/kafkagoso` | `kafkagoso/`  | optional gosoline integration                    |
| `github.com/asabaki/kafka-client/examples`  | `examples/`   | runnable examples, not imported by anything      |

## Development

Requires Go 1.26+ and Docker (for the integration tests).

```sh
docker compose up -d                       # single-node Kafka on localhost:9092

go test -race ./...                        # library: unit + integration tests
(cd kafkagoso && go test -race ./...)      # gosoline integration
(cd examples && go build ./...)            # examples compile

golangci-lint run ./...                    # golangci-lint v2
```

- Without a broker, integration tests are **skipped**, so `go test ./...` still runs every unit test. `-short` skips
  them too. Set `KAFKA_REQUIRED=1` to make a missing broker fail the tests instead (CI does).
- Use another broker with `KAFKA_BOOTSTRAP_SERVERS=host:port`; if port 9092 is taken locally, start Kafka with
  `KAFKA_HOST_PORT=19092 docker compose up -d`.
- Tests that depend on time freeze the library clock with `internal/timeutil` instead of sleeping.

## Pull requests

- Keep changes focused, and add or update tests for behaviour changes.
- Run the commands above before pushing; CI runs the same checks.
- Update `README.md` (and `kafkagoso/README.md` for gosoline settings) when you change the public API or a config
  key, and add a line to `CHANGELOG.md` under "Unreleased".
- By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).
