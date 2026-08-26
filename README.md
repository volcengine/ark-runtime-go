# Ark Runtime Go SDK

The official Go library for accessing ModelArk on Volcengine and BytePlus. It provides typed request and response models, streaming, authentication, retries, and timeout configuration.

## Installation

Requires **Go 1.20+**.

```bash
go get github.com/volcengine/ark-runtime-go
```

## Choose Volcengine or BytePlus

Set `ARK_API_KEY`, then choose the client factory for the service you use. The factory configures the correct base URL and region; request construction and all subsequent SDK calls are the same.

### Volcengine (China)

```go
client := arkruntime.NewVolcClient()
```

To pass the key directly:

```go
client := arkruntime.NewVolcClientWithApiKey("your-api-key")
```

### BytePlus (BP)

```go
client := arkruntime.NewByteplusClient()
```

To pass the key directly:

```go
client := arkruntime.NewByteplusClientWithApiKey("your-api-key")
```

Use a model ID available in the corresponding Volcengine or BytePlus account. Model IDs can differ between the two services; the examples use `doubao-seed-2-1-pro-260628` for Volcengine and `seed-2-0-lite-260428` for BytePlus. Override either default with `ARK_MODEL`.

## Quick start

### Responses API

The Responses API is the primary way to interact with Ark models.

```go
package main

import (
    "context"
    "fmt"
    "os"

    "github.com/volcengine/ark-runtime-go/arkruntime"
    "github.com/volcengine/ark-runtime-go/arkruntime/model/responses"
)

func main() {
    client := arkruntime.NewVolcClientWithApiKey(os.Getenv("ARK_API_KEY"))

    req := &responses.ResponsesRequest{
        Model: os.Getenv("ARK_MODEL"),
        Input: responses.NewStringResponsesInput("What is the capital of France?"),
    }

    resp, err := client.CreateResponses(context.Background(), req)
    if err != nil {
        panic(err)
    }
    fmt.Println(resp)
}
```

Set `ARK_MODEL` to a model ID from your account before running the example.

## Usage

### Chat Completions

```go
package main

import (
    "context"
    "fmt"
    "os"

    "github.com/volcengine/ark-runtime-go/arkruntime"
    "github.com/volcengine/ark-runtime-go/arkruntime/model/chat"
)

func main() {
    client := arkruntime.NewVolcClientWithApiKey(os.Getenv("ARK_API_KEY"))

    req := &chat.ChatCompletionRequest{
        Model: os.Getenv("ARK_MODEL"),
        Messages: []chat.ChatCompletionRequestMessage{
            {
                OneOf: chat.NewChatCompletionRequestUserMessageChatCompletionRequestMessageSum(
                    chat.ChatCompletionRequestUserMessage{
                        Role:    chat.ChatCompletionRequestUserMessageRoleUser,
                        Content: chat.NewStringChatCompletionMessageContent("What is the capital of France?"),
                    },
                ),
            },
        },
    }

    resp, err := client.CreateChatCompletion(context.Background(), req)
    if err != nil {
        panic(err)
    }
    if len(resp.Choices) > 0 {
        fmt.Println(resp.Choices[0].Message.Content)
    }
}
```

## Request extensions

All APIs with JSON object request bodies accept additional top-level fields
through `WithExtraBody`. Extra fields override serialized typed fields with the
same name. Multiple options are applied in order, so later values win.

```go
resp, err := client.CreateResponses(
    context.Background(),
    req,
    arkruntime.WithExtraBody(map[string]interface{}{
        "preview_feature": true,
    }),
)
```

`WithExtraBody` is not supported for bodyless, multipart, pre-marshalled, or
non-object requests. Use the typed request fields whenever the SDK already
models the parameter.

## Streaming

### Responses streaming

Use `CreateResponsesStream()` and match on `event.OneOf.Type` to handle each event kind.

```go
stream, err := client.CreateResponsesStream(context.Background(), req)
if err != nil {
    panic(err)
}
for {
    event, err := stream.Recv()
    if err == io.EOF {
        break
    }
    if err != nil {
        panic(err)
    }
    switch event.OneOf.Type {
    case responses.ResponseTextDeltaEventResponseStreamEventSum:
        fmt.Print(event.OneOf.ResponseTextDeltaEvent.Delta.Or(""))
    case responses.ResponseTextDoneEventResponseStreamEventSum:
        fmt.Printf("\nFull text: %s\n", event.OneOf.ResponseTextDoneEvent.Text.Or(""))
    case responses.ResponseReasoningSummaryTextDeltaEventResponseStreamEventSum:
        fmt.Print(event.OneOf.ResponseReasoningSummaryTextDeltaEvent.Delta.Or(""))
    }
}
```

### Chat Completions streaming

```go
stream, err := client.CreateChatCompletionStream(context.Background(), req)
if err != nil {
    panic(err)
}
defer stream.Close()
for {
    recv, err := stream.Recv()
    if err == io.EOF {
        break
    }
    if err != nil {
        panic(err)
    }
    if len(recv.Choices) > 0 {
        fmt.Print(recv.Choices[0].Delta.Content.Or(""))
    }
}
```

## Function calling

Register tools, detect when the model emits a call, execute it locally, and feed the result back.

```go
import (
    "github.com/go-faster/jx"
    "github.com/volcengine/ark-runtime-go/arkruntime/model/responses"
)

// 1. Define the tool
sumTool := responses.Tool{
    OneOf: responses.NewFunctionToolToolSum(responses.FunctionTool{
        Type:        responses.FunctionToolTypeFunction,
        Name:        "sum",
        Description: responses.NewOptString("Add two integers"),
        Parameters: responses.NewOptFunctionToolParameters(responses.FunctionToolParameters{
            "type": jx.Raw(`"object"`),
            "properties": jx.Raw(`{
                "a": {"type": "integer"},
                "b": {"type": "integer"}
            }`),
            "required": jx.Raw(`["a", "b"]`),
        }),
    }),
}

// 2. Send the request with tools
req := &responses.ResponsesRequest{
    Model: os.Getenv("ARK_MODEL"),
    Input: responses.NewStringResponsesInput("What is 1 + 2?"),
    Tools: []responses.Tool{sumTool},
}

// 3. Stream events; when the model returns a function_call item,
//    execute it and send the output back as a follow-up turn:
toolOutput := responses.ItemFunctionToolCallOutput{
    Type:   responses.ItemFunctionToolCallOutputTypeFunctionCallOutput,
    CallID: responses.NewOptString(callID),
    Output: responses.NewOptMessageContent(responses.NewStringMessageContent("3")),
}
req.Input = responses.NewInputItemArrayResponsesInput([]responses.InputItem{
    {OneOf: responses.NewItemFunctionToolCallOutputInputItemSum(toolOutput)},
})
```

See [examples/volc/responses/function_call](./examples/volc/responses/function_call) or [examples/byteplus/responses/function_call](./examples/byteplus/responses/function_call) for a complete runnable example.

## Authentication

```go
// API key (recommended)
client := arkruntime.NewVolcClientWithApiKey(os.Getenv("ARK_API_KEY"))
client := arkruntime.NewByteplusClientWithApiKey(os.Getenv("ARK_API_KEY"))

// AK/SK authentication
client := arkruntime.NewVolcClientWithAkSk(
    os.Getenv("VOLC_ACCESSKEY"),
    os.Getenv("VOLC_SECRETKEY"),
)
client := arkruntime.NewByteplusClientWithAkSk(
    os.Getenv("BYTEPLUS_ACCESSKEY"),
    os.Getenv("BYTEPLUS_SECRETKEY"),
)
```

The no-argument cloud factories prefer `ARK_API_KEY` and otherwise use the cloud-specific AK/SK environment variables shown above.

## Error handling

API errors are returned as standard Go errors. Check for them using the usual `if err != nil` pattern. For streaming, `io.EOF` signals a clean end of the stream.

```go
resp, err := client.CreateResponses(ctx, req)
if err != nil {
    // Handle error (network, auth, rate limit, invalid request, etc.)
    log.Fatalf("API error: %v", err)
}
```

## Examples

For detailed usage guidance and legacy migration, see
[`docs/README.md`](docs/README.md) and
[`docs/migration.md`](docs/migration.md).

Runnable examples are in the [examples/](./examples) directory:

- [volc](./examples/volc) — Volcengine China examples for Chat, Responses, images, video generation, embeddings, files, tokenization, batch APIs, and resource APIs
- [byteplus](./examples/byteplus) — BytePlus counterparts using the BytePlus client and regional model IDs

MCP examples are provided for both clouds and show the required `ark-beta-mcp: true` header. Other built-in-tool examples are CN-only and show their corresponding beta headers.

Run any example with:

```bash
ARK_API_KEY=your-key go run examples/volc/responses/basic/main.go
```

## Requirements

- Go 1.20 or later
- A Volcengine or BytePlus ModelArk API key

## Third-party notices

See [THIRD_PARTY_NOTICES.md](./THIRD_PARTY_NOTICES.md) for third-party
attribution notices.
