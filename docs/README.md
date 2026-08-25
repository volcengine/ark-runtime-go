# Ark Runtime Go SDK documentation

This directory contains detailed usage and migration guidance for the Ark
Runtime Go SDK.

## Choose the right document

- [Usage guide](usage.md): install the SDK, select
  Volcengine (CN) or BytePlus, construct typed requests, handle streams, and
  use built-in tools safely.
- [Migration guide](migration.md): move an application from the legacy
  Volcengine or BytePlus Go SDK to this SDK.
- [`../examples/volc`](../examples/volc): runnable Volcengine examples.
- [`../examples/byteplus`](../examples/byteplus): runnable BytePlus examples.

## Important usage rules

1. Select the cloud once when creating the client. Use
   `NewVolcClientWithApiKey` for CN and `NewByteplusClientWithApiKey` for
   BytePlus. Do not copy a base URL between clouds.
2. Keep credentials in `ARK_API_KEY`; never place an API key in source code,
   generated patches, logs, or tests.
3. Use the generated request and union constructors. Do not assemble JSON and
   send it through an unrelated HTTP client unless the application explicitly
   requires raw HTTP.
4. Treat stream events as variants. Ignore unknown variants so applications
   remain compatible when the service adds events.
5. MCP is available in CN and BytePlus. Other built-in Responses tools in the
   examples are CN-only. Send the matching `ark-beta-*` header on every request
   that uses a beta tool.
6. Prefer an application-provided model or endpoint ID. The model names in the
   examples are runnable defaults, not values to hard-code into a library.

## Minimal verification

After a change, run:

```bash
go test ./...
go vet ./...
```

For a migrated application, also run one non-streaming and one streaming
request in the intended cloud. Built-in tool paths need their own smoke test
because a successful ordinary Responses request does not validate tool access
or beta headers.
