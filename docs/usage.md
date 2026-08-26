# Usage guide

Use this guide when creating or editing a Go application with Ark Runtime. The
repository examples are the source of truth for complete request shapes.

## 1. Install and configure

```bash
go get github.com/volcengine/ark-runtime-go@latest
# Set ARK_API_KEY in the process environment before running the application.
```

Keep the key outside source control. Let the application accept a model or
endpoint ID through configuration, for example `ARK_MODEL`.

## 2. Select the cloud

Only the client constructor changes. Request construction and response
handling stay the same.

```go
// Volcengine (CN)
client := arkruntime.NewVolcClientWithApiKey(os.Getenv("ARK_API_KEY"))

// BytePlus
client := arkruntime.NewByteplusClientWithApiKey(os.Getenv("ARK_API_KEY"))
```

Use the models provisioned for that cloud. Current example defaults include:

| API | Volcengine (CN) | BytePlus |
|---|---|---|
| Responses / Chat | `doubao-seed-2-1-pro-260628` | `seed-2-0-lite-260428` |
| Multimodal / sparse embeddings | `doubao-embedding-vision-251215` | `skylark-embedding-vision-251215` |
| Image generation | `doubao-seedream-5-0-pro-260628` | `dola-seedream-5-0-pro-260628` |
| Video generation | `doubao-seedance-2-0-fast-260128` | `dreamina-seedance-2-0-fast-260128` |

An account may expose a different model name or an endpoint ID. Configuration
from the user takes precedence over this table.

BytePlus currently has no model for the text-only `/embeddings` endpoint, so
its examples use `/embeddings/multimodal` instead.

## 3. Build typed requests

Generated models represent JSON unions explicitly. Always use the matching
constructor instead of filling the union internals by hand.

For a simple Responses input:

```go
req := &responses.ResponsesRequest{
    Model: model,
    Input: responses.NewStringResponsesInput("Explain LLMs in one sentence."),
}
response, err := client.CreateResponses(ctx, req)
```

For a structured Responses input, create the leaf content, wrap it as an input
item, and then wrap the list:

```go
message := responses.ItemEasyMessage{
    Role: responses.NewOptMessageRole(responses.MessageRoleUser),
    Content: responses.NewContentItemArrayMessageContent([]responses.ContentItem{
        {OneOf: responses.NewContentItemTextContentItemSum(
            responses.ContentItemText{
                Type: responses.ContentItemTextTypeInputText,
                Text: "Describe this request",
            },
        )},
    }),
}
req.Input = responses.NewInputItemArrayResponsesInput([]responses.InputItem{
    {OneOf: responses.NewItemEasyMessageInputItemSum(message)},
})
```

Chat messages use the same pattern. See
[`examples/volc/chat/basic`](../examples/volc/chat/basic) or the corresponding
[`examples/byteplus/chat/basic`](../examples/byteplus/chat/basic) directory.

Practical rules:

- Pass request structs by pointer.
- Use `NewOpt*` helpers for optional generated fields.
- Use the generated `New*Sum` constructor for a union variant.
- Do not set `OneOf.Type` separately from its value.
- Preserve user-provided extension fields and custom headers during refactors.

## 4. Handle responses and streams

Non-streaming Responses output is a list of typed output-item variants. Check
the variant before reading its fields. The full extraction pattern is in the
basic Responses examples.

Chat streams yield chunks:

```go
stream, err := client.CreateChatCompletionStream(ctx, req)
if err != nil { /* handle */ }
defer stream.Close()

for {
    chunk, err := stream.Recv()
    if err == io.EOF {
        break
    }
    if err != nil { /* handle */ }
    if len(chunk.Choices) > 0 {
        fmt.Print(chunk.Choices[0].Delta.Content.Or(""))
    }
}
```

Responses streams yield `*responses.ResponseStreamEvent`. Dispatch on
`event.OneOf.Type`:

```go
switch event.OneOf.Type {
case responses.ResponseTextDeltaEventResponseStreamEventSum:
    fmt.Print(event.OneOf.ResponseTextDeltaEvent.Delta.Or(""))
case responses.ResponseCompletedEventResponseStreamEventSum:
    responseID := event.OneOf.ResponseCompletedEvent.Response.ID
    _ = responseID
default:
    // Forward-compatible: ignore events this application does not consume.
}
```

Do not assume that every event contains text. Reasoning, output-item,
function-call, MCP, completion, and error events carry different payloads.
Keep `io.EOF` separate from an actual stream error and close every stream.

## 5. Built-in tools and headers

Pass beta headers as request options on both streaming and non-streaming calls:

```go
response, err := client.CreateResponses(
    ctx,
    req,
    arkruntime.WithCustomHeader("ark-beta-mcp", "true"),
)
```

| Tool | Cloud | Required header |
|---|---|---|
| MCP | CN and BytePlus | `ark-beta-mcp: true` |
| Web search | CN only | `ark-beta-web-search: true` |
| Knowledge search | CN only | `ark-beta-knowledge-search: true` |
| Doubao App | CN only | `ark-beta-doubao-app: true` |
| Image process | CN only | `ark-beta-image-process: true` |

Do not migrate a CN-only built-in tool to BytePlus. Function calling is an
application-defined tool flow and is not the same as a hosted built-in tool.

## 6. Choose an example

Examples are divided by cloud, then API. Start with the matching directory and
retain its constructor, model family, and tool availability:

- Volcengine: [`examples/volc`](../examples/volc)
- BytePlus: [`examples/byteplus`](../examples/byteplus)

Use this routing table instead of adapting an unrelated Chat or Responses
example:

| Intent | Example path below the region directory |
|---|---|
| Chat, including stream/non-stream/function calling | `chat/` |
| Responses, including stream/non-stream/tools | `responses/` |
| Text embeddings (Volcengine only) | `embeddings/` |
| Sparse or multimodal embeddings | `sparseembeddings/`, `multimodalembeddings/` |
| Image generation | `images/` |
| Video generation | `contentgeneration/` |
| File upload and operations | `files/` |
| Batch inference | `batch/` and the `batch_*` examples |
| Agents, sessions, memory stores, environments | the matching lifecycle example |
| Token counting | `tokenization/` |

Some historical directory names use different separators. Prefer the example
listed by the regional README or the one that imports this SDK's generated
models. Chat and Responses include both streaming and non-streaming flows.

## 7. Completion checklist

- The dependency uses `github.com/volcengine/ark-runtime-go` only.
- Exactly one regional constructor is selected by application configuration.
- No secret is committed.
- Request unions use generated constructors.
- Streaming code handles `io.EOF`, errors, and unknown event variants.
- Every built-in tool request carries its matching beta header.
- A CN-only tool is not present in BytePlus code.
- `go test ./...` and `go vet ./...` pass.
