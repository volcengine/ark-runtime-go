# Client-side MCP tools

This optional module converts tools from the official MCP Go SDK into:

- `agent.ToolItem` declarations for creating or updating a Managed Agent.
- `agenttoolset.Tool` implementations executed by a self-hosted worker.

The MCP server only needs to be reachable from the worker. Its credentials stay
in the worker-side MCP transport and are not sent to Managed Agents.

The adapter wraps an already connected MCP client session, so applications may
use stdio or another transport supported by their MCP client. Keep one client
session open for the worker lifetime. That session is reused across all Managed
Agents Sessions handled by the worker; calls do not automatically include a
Managed Agents `session_id` or `work_id`, and Session idle/deletion is not an
MCP lifecycle notification. Use stateless tools or implement explicit
tenant/session isolation in the MCP server.

The Agent declaration and worker registry must be built from the same MCP tool
list. Tools are discovered at startup; restart the worker and update the Agent
when the MCP server changes its tool list.

```go
tools := make([]*mcpsdk.Tool, 0)
for tool, listErr := range session.Tools(ctx, nil) {
	if listErr != nil {
		return listErr
	}
	tools = append(tools, tool)
}

declarations, err := arkmcp.CustomToolItems(tools)
if err != nil {
	return err
}
// Use declarations in CreateAgentRequest.Tools or UpdateAgentRequest.Tools.

customTools, err := arkmcp.NewTools(tools, session)
if err != nil {
	return err
}
worker := environments.NewEnvironmentWorkerForClient(client, environments.EnvironmentWorkerOptions{
	EnvironmentID: environmentID,
	CustomTools:   customTools,
})
return worker.Run(ctx)
```

This module uses the official MCP Go SDK, which requires Go 1.23 or newer. It is
a separate Go module so the main Ark Runtime SDK keeps its Go 1.20 baseline.
Install the adapter with the same release version as the core SDK:

```bash
go get github.com/volcengine/ark-runtime-go/mcp@v0.6.0
```

Each core `vMAJOR.MINOR.0` release also publishes the matching
`mcp/vMAJOR.MINOR.0` module tag.

See [`examples/self_hosted_mcp_worker`](../examples/self_hosted_mcp_worker) for
a complete local MCP Server -> Agent custom tool -> self-hosted worker example.

The main module also exposes the protocol-independent
`arkruntime/selfhosted/mcp.Client` interface. Applications may implement that
interface directly when they use another MCP transport or cannot use the
official Go SDK.

Managed Agents currently accepts the top-level JSON Schema fields `type`,
`properties`, and `required`. The helper keeps those fields structured, inlines
local `$defs` and `definitions` references used by properties, and appends other
top-level constraints as compact JSON to the tool description. The MCP server
remains the authoritative validator when the worker executes the call. Agent
tool descriptions, including appended constraints, must fit within 10,000
characters.

## Tool result support

The worker preserves MCP `isError` and supports these result blocks:

- text;
- `image/jpeg`, `image/png`, `image/gif`, and `image/webp` image blocks;
- embedded resources with the same image MIME types;
- embedded `application/pdf` resources; and
- embedded text resources whose MIME type is absent, empty, or starts with
  `text/`.

When a result has no content blocks but has `structuredContent`, the helper
serializes it as compact JSON text. Audio, resource links, unknown content
types, and other resource MIME types become an error result. If a result mixes
supported and unsupported blocks, the whole converted result is an error; the
supported blocks are not returned separately.

## Operational and security notes

- Fetch every `tools/list` page. Use the exact same selected tool definitions
  for the Agent declaration and worker registry. Managed Agents currently
  accepts at most eight custom tools per Agent, so explicitly select a stable
  subset when the MCP server exposes more.
- Tool discovery happens at worker startup. When the MCP server changes its
  tools, update the Agent while it is idle and restart the worker.
- Tool names must match `[a-zA-Z0-9_-]{1,128}`. Avoid names that collide with
  built-in Agent tools, and add your own prefixes when multiple MCP servers
  expose the same name.
- Managed Agents permission policies do not apply to custom tools. The worker
  executes each matching custom tool call, so implement approval, authorization,
  and operation allowlists in the MCP server or a wrapper tool.
- Client-side MCP servers run with the worker's OS, filesystem, and network
  permissions; Managed Agents does not put them in a separate sandbox. Run them
  with least privilege and a minimal environment. Do not pass `ARK_API_KEY` to
  an MCP subprocess; use separate MCP-specific credentials.
- Only wrap MCP servers you trust. Tool names, descriptions, inputs, and results
  enter the model context and must be treated as untrusted content.
- Configure MCP transport or client timeouts. The worker `ToolTimeout` remains
  the final upper bound, but a shorter MCP timeout gives clearer failures.
