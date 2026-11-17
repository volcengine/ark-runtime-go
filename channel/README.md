# Managed Agents Channel SDK

Connect a Feishu bot to Managed Agents with durable message processing, approvals,
file delivery, and restart recovery. Run the supplied CLI or embed the SDK in an
existing Go service.

Requires **Go 1.25+**. This directory is a separate module,
`github.com/volcengine/ark-runtime-go/channel`, released with `channel/v*` tags
independently of the main SDK. The main SDK retains its Go 1.20 baseline.

## Getting started

- [Quickstart](docs/quickstart.md): prepare a Feishu app, install the state store,
  and run the CLI.
- [Embedded example](../examples/channel_feishu): compose the main SDK client,
  Feishu provider, and persistent store explicitly.
- [Configuration](docs/configuration.md): identity, approvals, streaming,
  attachments, and BYOK.
- [Provider extensions](docs/extending-providers.md): implement another channel.
- [Storage](docs/storage.md): installation, encryption, and storage tests.

## Features

The SDK supports direct messages, groups, and threads; WebSocket or encrypted,
verified webhooks; Session reuse, `/new`, and `/stop`; image, file, and video
inputs; recent context, progress cards, and tool approvals; bot credentials and
user Device OAuth; final-answer streaming and artifact delivery; business hooks,
structured logs, metrics, and administrative commands.

The default bbolt store is designed for a single process. SQLite and MySQL are
validated against the same store contracts. Uncertain writes are reconciled
before another attempt; failed deliveries are retried without rerunning the
Agent. An SSE stream ending does not prove that a turn completed: persisted
history remains authoritative.

## Development

From the repository root, use Go 1.25+ to create an untracked development
workspace and enter this module:

```sh
go work use . ./channel
cd channel
```

Then run:

```sh
make fmt-check
make build
make lint
make test
```

Tests run in four complete groups: core, store, integration, and other. All groups
retain race detection; integration tests measure coverage of the runtime. MySQL
contracts additionally require the `mysqlintegration` build tag and an isolated
database; see [storage](docs/storage.md). Packages under `internal/` are
implementation and test facilities, not public extension APIs.

## Security and license

Report security issues according to the repository [security policy](../SECURITY.md).
This module is licensed under [Apache-2.0](../LICENSE); dependency attribution is in
the repository's [third-party notices](../THIRD_PARTY_NOTICES.md#channel-module-dependencies).
