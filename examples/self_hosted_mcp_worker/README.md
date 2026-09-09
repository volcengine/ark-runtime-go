# Self-hosted MCP worker

This example follows Anthropic's client-side MCP helper example at the same
level of abstraction: connect to an MCP server, discover its tools, convert
them, and run an existing self-hosted Environment Worker.

The self-hosted Environment must exist before starting the worker. Create or
update an Agent with the printed `Agent custom tool` declarations before
creating a Session. Printing declarations does not update the Agent
automatically. The same MCP tool list is registered with the worker for
execution, and the example reads every `tools/list` page.

## Manual end-to-end verification

The example registers the MCP tool implementation with the self-hosted worker,
but it does not create or update Managed Agents resources. Complete the
following control-plane fields manually:

1. Create a self-hosted Environment and copy its ID into
   `MA_ENVIRONMENT_ID`.
2. Set `ARK_API_KEY`. Set `ARK_BASE_URL` only when using a non-production
   endpoint.
3. Start the worker with the MCP server command after `--`:

   ```bash
   export ARK_API_KEY=...
   export MA_ENVIRONMENT_ID=env_xxx
   # Optional, for example when testing against staging:
   # export ARK_BASE_URL=https://example.com/api/v3

   cd examples/self_hosted_mcp_worker
   go run . -- go run ./server
   ```

4. Copy every printed `Agent custom tool: {...}` declaration into the Agent's
   tool configuration. For the bundled server, use the declaration below.
   Configure it before creating the Session; printing the declaration does not
   update the Agent automatically.
5. Create a Session that uses both that Agent and the same self-hosted
   Environment from `MA_ENVIRONMENT_ID`.
6. Send a message such as:

   ```text
   Call mcp_echo exactly once with text "Hello from MCP echo!" and report the result.
   ```

The verification passes when the Session shows an `mcp_echo` call with that
input, a `user.custom_tool_result` containing
`MCP echo: Hello from MCP echo!`, a final Agent response, and a final
`session.status_idle` whose stop reason is `end_turn`. A temporary
`session.status_idle` with stop reason `requires_action` means that the Session
is waiting for the external custom-tool result; it is expected and is not an
approval prompt or a failure. At the event level, observe these milestones:

```text
agent.custom_tool_use
session.status_idle          stop_reason=requires_action
user.custom_tool_result      posted by the worker
agent.message
session.status_idle          stop_reason=end_turn
```

Do not depend on the first idle event and the tool-result POST being displayed
in an exact relative order: the worker starts executing as soon as it observes
`agent.custom_tool_use`.

Keep the worker process running for the whole verification. The command after
`--` is a stdio MCP server command, not a URL; the worker starts the process and
communicates with it through stdin/stdout.

The bundled server exposes this declaration:

```json
{
  "type": "custom",
  "name": "mcp_echo",
  "description": "Echo text through the local MCP server.",
  "input_schema": {
    "type": "object",
    "properties": {"text": {"type": "string"}},
    "required": ["text"]
  }
}
```

To use another stdio MCP server, replace the command after `--`. Set
`ARK_BASE_URL` only when overriding the SDK's production endpoint. The example
removes `ARK_API_KEY` from the MCP subprocess environment, but inherits other
environment variables. Review or allowlist them before production and use
separate MCP-specific credentials.

The example opens one MCP process and client session for the lifetime of the
Environment Worker and reuses it for every Managed Agents Session handled by
that worker. MCP calls do not automatically contain the Managed Agents
`session_id` or `work_id`, and Session idle/deletion is not an MCP lifecycle
notification. Use a stateless MCP server or implement explicit tenant/session
isolation, and expect the MCP process to stop only when the worker exits. The
command-line example accepts a stdio child command only; other transports can
be used by constructing an MCP client session programmatically.

Managed Agents currently accepts at most eight custom tools per Agent. If the
server exposes more, select the same stable subset for both the Agent and the
worker. Custom tools do not use Managed Agents permission policies: the worker
executes matching calls directly, so put approval, authorization, and operation
allowlists in the MCP server or wrapper. Only connect trusted servers, avoid
tool names that collide with built-in Agent tools, and configure an MCP client
timeout. MCP servers run with the worker's OS, filesystem, and network
permissions rather than in a Managed Agents sandbox, so run them with least
privilege and do not pass `ARK_API_KEY` to them. Update the Agent while it is
idle and restart the worker whenever the server's tool list changes.

This directory is a separate Go 1.23 module so the optional MCP dependency does
not change the core SDK's Go 1.20 baseline.
