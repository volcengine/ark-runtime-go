# Embedded Feishu channel example

Compose the main SDK client, Feishu Provider, and persistent Store in your own Go
service. This example does not discover internal platform configuration.

Requires Go 1.25+. Inject ARK_API_KEY, FEISHU_APP_ID, FEISHU_APP_SECRET,
ARK_AGENT_ID, and ARK_ENVIRONMENT_ID through the deployment environment.
CHANNEL_DATABASE is optional.

```sh
go run . install  # First installation only; never overwrites existing data.
go run .          # Run in the foreground; Ctrl+C shuts down gracefully.
```

The example uses bbolt for a single process. Use the SQL Store for multiple
instances. Diagnose startup errors instead of rerunning install or deleting data.
See the [quickstart](../../channel/docs/quickstart.md) for Feishu app setup and
additional permissions.

The example pins the published root SDK v0.21.0 and uses a local Channel
replacement for checkout development. When moving it to an external project,
select a published Channel version and remove that replacement.
