# Examples

Runnable examples for the `arkruntime` Go SDK. Set `ARK_API_KEY` and, for most examples, `ARK_MODEL` to a model ID available in your account.

Run commands from the repository root:

```bash
export ARK_API_KEY=...
export ARK_MODEL=...
go run examples/volc/responses/basic/main.go
```

Cloud-specific examples are grouped by cloud:

- [`volc/`](./volc) uses `NewVolcClientWithApiKey` and Volcengine China model IDs.
- [`byteplus/`](./byteplus) uses `NewByteplusClientWithApiKey` and BytePlus model IDs.

The paired multimodal and sparse embedding examples default to `doubao-embedding-vision-251215` / `skylark-embedding-vision-251215`. The paired image examples default to `doubao-seedream-5-0-pro-260628` / `dola-seedream-5-0-pro-260628`. The paired video-generation examples default to `doubao-seedance-2-0-fast-260128` / `dreamina-seedance-2-0-fast-260128`.

MCP is available in both clouds and its examples explicitly send `ark-beta-mcp: true`. Other built-in tools are CN-only: Web Search sends `ark-beta-web-search: true`, and Doubao App sends `ark-beta-doubao-app: true`.

The [`self_hosted_worker/`](./self_hosted_worker) example runs a local Managed Agents worker for an existing self-hosted environment. It requires `MA_ENVIRONMENT_ID`; the client defaults to `https://ark.cn-beijing.volces.com/api/v3`.
