# Ark Runtime Go SDK

The official Go library for the Ark runtime API. It provides convenient access to the Ark REST API from any Go application, with typed request/response models, streaming support, and built-in authentication.

## Installation

Requires **Go 1.20+**.

```bash
go get github.com/volcengine/ark-runtime-go
```

## Usage

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
    client := arkruntime.NewClientWithApiKey(os.Getenv("ARK_API_KEY"))

    req := &responses.ResponsesRequest{
        Model: "doubao-seed-1-6",
        Input: responses.NewStringResponsesInput("What is the capital of France?"),
    }

    resp, err := client.CreateResponses(context.Background(), req)
    if err != nil {
        panic(err)
    }
    fmt.Println(resp)
}
```

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
    client := arkruntime.NewClientWithApiKey(os.Getenv("ARK_API_KEY"))

    req := &chat.ChatCompletionRequest{
        Model: "doubao-seed-1-6",
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
    Model: "doubao-seed-1-6",
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

See [examples/responses/function_call](./examples/responses/function_call) for a complete runnable example.

## Authentication

```go
// API key (recommended) — reads from code or from ARK_API_KEY env var
client := arkruntime.NewClientWithApiKey(os.Getenv("ARK_API_KEY"))

// AK/SK authentication
client := arkruntime.NewClientWithAkSk(
    os.Getenv("VOLC_ACCESSKEY"),
    os.Getenv("VOLC_SECRETKEY"),
)

// Cloud-aware factories (auto-detect base URL and env vars)
client := arkruntime.NewVolcClient()
client := arkruntime.NewByteplusClient()
```

## Error handling

API errors are returned as standard Go errors. Check for them using the usual `if err != nil` pattern. For streaming, `io.EOF` signals a clean end of the stream.

```go
resp, err := client.CreateResponses(ctx, req)
if err != nil {
    // Handle error (network, auth, rate limit, invalid request, etc.)
    log.Fatalf("API error: %v", err)
}
```

## API coverage

| API | Methods |
|-----|---------|
| Responses | `client.CreateResponses()` / `client.CreateResponsesStream()` |
| Chat Completions | `client.CreateChatCompletion()` / `client.CreateChatCompletionStream()` |
| Embeddings | `client.CreateEmbeddings()` |
| Multimodal Embeddings | `client.CreateMultiModalEmbeddings()` |
| Content Generation | `client.CreateContentGenerationTask()` |
| Images | `client.CreateImageGeneration()` |
| Files | `client.CreateFile()` / `client.ListFiles()` / `client.DeleteFile()` |
| Tokenization | `client.CreateTokenization()` |

## Package layout

```
arkruntime/                           Client, auth, retries, streaming
arkruntime/model/responses/           Responses API types
arkruntime/model/chat/                Chat Completions API types
arkruntime/model/embedding/           Text Embedding API types
arkruntime/model/multimodalembedding/ Multimodal Embedding API types
arkruntime/model/contentgeneration/   Content Generation API types
arkruntime/model/images/              Image Generation API types
arkruntime/model/tokenization/        Tokenization API types
arkruntime/model/file/                Files API types
```

## Examples

Runnable examples are in the [examples/](./examples) directory:

- [responses/basic](./examples/responses/basic) — streaming responses with multi-turn chaining
- [responses/function_call](./examples/responses/function_call) — tool use with local execution
- [responses/web_search](./examples/responses/web_search) — built-in web search tool
- [responses/video](./examples/responses/video) — video upload and analysis
- [responses/mcp](./examples/responses/mcp) — remote MCP server integration
- [chat/basic](./examples/chat/basic) — standard and streaming chat completions
- [embeddings](./examples/embeddings) — text embeddings
- [multimodalembeddings](./examples/multimodalembeddings) — image embeddings
- [contentgeneration](./examples/contentgeneration) — video generation tasks
- [files](./examples/files) — file upload, list, and delete
- [tokenization](./examples/tokenization) — tokenize text and inspect tokens

Run any example with:

```bash
ARK_API_KEY=your-key go run ./examples/responses/basic
```

## Requirements

- Go 1.20 or later
- An Ark API key (set via `ARK_API_KEY` environment variable or passed directly to the client)
