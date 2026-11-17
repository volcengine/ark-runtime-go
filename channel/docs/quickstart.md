# Run your first Feishu channel

Run the complete “Feishu message → Managed Agent → Feishu reply” flow locally.
The default WebSocket mode opens an outbound connection and needs no public
callback endpoint.

## Prerequisites

Install Go 1.25+ and prepare a Managed Agents API key, Agent ID, and Environment
ID. Your Feishu app needs bot capabilities, message permissions, and the
`im.message.receive_v1` subscription. Group messages require an @mention by
default. Publish a new Feishu app version after changing permissions or event
subscriptions.

## Build and configure

From the repository root:

```sh
go work use . ./channel
cd channel
go build -o ./bin/channel-runner ./cmd/channel-runner
```

Save the following as `config.json` and replace the resource IDs. Inject the
referenced secrets through your deployment environment; do not put their values
in the configuration file.

```json
{
  "database_path": "state/channel.db",
  "observability": {"admin_addr": "127.0.0.1:9090"},
  "connections": [{
    "id": "primary",
    "feishu": {"app_id": "cli_replace_me", "app_secret_ref": "env:FEISHU_APP_SECRET"},
    "ma": {"base_url": "https://ark.cn-beijing.volces.com/api/v3", "api_key_ref": "env:ARK_API_KEY"},
    "agent_id": "replace_with_agent_id",
    "agent_version": "v1",
    "environment_id": "replace_with_environment_id"
  }]
}
```

Defaults are a non-streaming answer and a single-process bbolt store. Relative
state paths are resolved against the configuration file directory. Unknown
configuration fields are rejected.

## Validate, install, and run

```sh
./bin/channel-runner validate --config ./config.json
./bin/channel-runner install --config ./config.json
./bin/channel-runner doctor --config ./config.json --online
./bin/channel-runner run --config ./config.json
```

`validate` is offline. Run `install` once for a new deployment: it does not
overwrite existing data. `run` does not implicitly create or upgrade the database.
Send a direct message to the bot or @mention it in a group. `Ctrl+C` cancels the
runtime and waits for its components to exit.

Use `status` and `doctor` to diagnose an existing store rather than deleting and
reinstalling it. Use `retry-delivery --id` to retry a failed reply delivery. Before
enabling user identity, read [configuration](configuration.md) and prepare stable
identity labels and an encryption key.

After a public release is available, install the CLI using a confirmed version,
for example:

```sh
go install github.com/volcengine/ark-runtime-go/channel/cmd/channel-runner@v0.1.0
```

This command illustrates versioned installation; it does not claim that v0.1.0
has already been published. Building from the checkout uses the local workspace.
