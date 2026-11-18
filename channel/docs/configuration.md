# Features and credentials

Start with the minimal configuration and enable the capabilities you need.

| Setting | Behavior |
|---|---|
| `feishu.receive_mode` | `websocket` by default; `webhook` requires a public HTTPS proxy and verification-token/encrypt-key references |
| `group_trigger` / `group_reply_mode` | Group trigger: `mention` / `all`; reply location: `thread` / `reply` / `chat` |
| `show_progress` / `progress_protocol` | Show progress; optionally use `native_cot` |
| `response_mode` | `non_stream`, `process_stream`, or `final_stream` |
| `approvals` | Tool approvals; requires the `card.action.trigger` callback |
| `recent_context` | Add bounded recent context to a new Session; requires permission to read conversation messages |
| `deliver_artifacts` | Deliver files referenced in the final answer; supported images/videos can be displayed inline |
| `runtime.idle_timeout` | Maximum continuous period without effective progress; defaults to 60 minutes |
| `runtime.attachments` | Attachment count, per-file size, and total size; defaults to 20, 99,999,999 bytes, and 500 MB |
| `store.driver` | `embedded`, `sqlite`, or `mysql` |

Native final-answer streaming is provider-specific. `NewRuntime` rejects
`StreamFinalAnswer` for a custom provider that does not support the required
protocol.

Use `env:NAME` or `file:/absolute/path` references for secrets. Secret files must be
regular files with 0400/0600 permissions and cannot be symbolic links. Keep actual
tokens, API keys, and authorization links out of source control and logs.

## Embedded MA client

The CLI and `ma.NewClient` disable automatic transport retries on SDK clients
they create. Runtime records write intent and reconciles uncertain outcomes;
replaying a non-idempotent request inside the transport could duplicate an
already accepted input or Vault creation.

When injecting an SDK client through `ma.New`, Channel preserves its settings.
Configure `arkruntime.WithRetryTimes(0)` for clients used for these writes, as in
the embedded example. Channel does not refresh API keys or replay requests after
an authorization failure.

## BYOK

`ma.model_header_refs` resolves model headers through the SecretResolver on each
request:

```json
{"ma":{"base_url":"https://ark.cn-beijing.volces.com/api/v3","api_key_ref":"env:ARK_API_KEY","model_header_refs":{"Authorization":"env:MODEL_API_KEY"}}}
```

These target headers are sent only with events write requests using the
`X-User-Model-Header-*` transport prefix. They are not attached to reads, downloads,
or SSE subscriptions. Embedded applications can use `ma.Options.ModelHeaderSource`;
the same header-name, size, and reserved-header checks apply.

## User authorization and bot identity

Bot identity requires an explicitly configured Vault and allowed hosts. User
Device OAuth requires a readable cloud Environment. Authorization state must be
encrypted; failures must not fall back to plaintext.

The CLI [user-device example](../configs/user-device.example.json) uses
owner/project/channel_id as stable, deployment-defined isolation labels, not as
queries to an internal control plane. Keep them stable across restarts.
`encryption_key_ref` identifies a base64-encoded, 32-byte AES key managed by the
deployment; the SDK does not generate a replacement key. Stop the process and run
`expand-user-auth` explicitly when enabling authorization on an existing store.

For embedding, use `feishu.NewWithContext` with `UserAuthorization`. Supply a stable
Namespace, the same MA/Store handles used by the Runtime, and a Codec. Install the
authorization structures and enable payload encryption on the actual Store before
initializing the Provider. Initialization reads the Environment and application
user permissions and can be cancelled through ctx. Changing Namespace changes
identity isolation; it does not recover another namespace's grants.

## Observability and platform support

Hooks have deadlines, concurrency limits, and panic isolation. They must honor
their context: expired results are rejected, but a callback that ignores
cancellation can still block its worker and shutdown. Observers must
return promptly, support concurrent calls, and avoid arbitrary IDs as metric
labels. The CLI management HTTP listener is restricted to loopback addresses;
configure the public webhook listener separately.

macOS and Linux are the existing runtime paths. Windows has not been validated.
Feishu and international Lark use different default service addresses; explicitly
configure the appropriate official OpenAPI address for Lark.

## Queue and delivery budgets

Configure `QueueTTL` when constructing the Store, through `embedded.Options` or
`sql.Options`. It persists a deadline derived from the inbound message's
`received_at`; expired queued turns are settled even at the queue tail. Zero
disables this limit. Runtime options do not override an existing Store's queue
policy. Code that previously set the ineffective `channel.Options.QueueTTL`
field must move that setting to the Store options.

`DeliveryTimeout` also bounds each attempt to persist a delivery result. If that
attempt exhausts its own budget while the original lease remains valid, Runtime
retries saving the same result without calling the Provider again. Shutdown
still allows one independent, bounded completion attempt after a successful
send. A process crash can lose that in-memory result; recovery then relies on
the persisted delivery UUID and the Provider's deduplication window. This is not
an exactly-once guarantee.

## Feishu callback and OAuth boundaries

Webhook business events require a valid signature whose
`X-Lark-Request-Timestamp` is within five minutes of the receiver's current time,
in either direction. Keep the host clock synchronized. This check uses the
request signature time, so an older event can still arrive with a fresh signature.
The encrypted URL verification handshake is a controlled exception: it validates
the Verification Token and only echoes the challenge, without entering the Inbox
or executing a business handler. Feishu does not guarantee signature headers on
that handshake; see [receiving events](https://open.feishu.cn/document/server-docs/event-subscription-guide/event-subscription-configure-/encrypt-key-encryption-configuration-case).

A Feishu Provider supports sequential `Start` calls. Concurrent calls are
rejected, and stopping clears the webhook handler. `Ready` closes after the first
successful receiver setup and stays closed across restarts; it is not a current
connection-health signal.

Device OAuth service base URLs and user verification URLs require HTTPS. Tests
can inject a client that trusts their TLS fixture; there is no insecure-HTTP
configuration switch. User tokens with a refresh token are refreshed on lookup
within 30 seconds of expiry, under the existing distributed maintenance lease.
Their recorded expiry remains the upstream expiry. Without a refresh token,
they remain usable until that actual expiry.

Bot tokens are cached per Provider and reused for COT calls and attachment
downloads. Refresh starts before expiry with a margin of ten percent of the
returned lifetime, capped at five minutes. Concurrent refreshes are serialized
with a cache recheck; a waiting call can cancel independently. Cache state is
local to the Provider, so separate processes do not share it.
