# Migrate from the legacy Go SDK

This guide covers both legacy packages:

- `github.com/volcengine/volcengine-go-sdk/service/arkruntime`
- `github.com/byteplus-sdk/byteplus-go-sdk-v2/service/arkruntime`

The new package is `github.com/volcengine/ark-runtime-go/arkruntime` for both
clouds. Migration is partly mechanical and partly semantic because the new SDK
uses generated, typed unions.

## 1. Migration order

Migrate one API flow at a time:

1. Choose the target cloud: Volcengine (CN) or BytePlus.
2. Update the Ark Runtime import paths and regional client constructor.
3. Move each request to its new API-specific generated model package.
4. Rebuild union-valued request fields with the generated constructors.
5. Update non-streaming response access and streaming event dispatch.
6. Add the required beta header to every built-in-tool request and remove any
   CN-only tool from a BytePlus target.
7. Compile and smoke-test that flow before migrating the next one.

Do not use a project-wide regular-expression replacement for request models or
stream events. Their correct mapping depends on the selected union variant and
the events the application consumes.

## 2. Dependency, imports, and client

| Legacy | New |
|---|---|
| `github.com/volcengine/volcengine-go-sdk/service/arkruntime` | `github.com/volcengine/ark-runtime-go/arkruntime` |
| `github.com/byteplus-sdk/byteplus-go-sdk-v2/service/arkruntime` | `github.com/volcengine/ark-runtime-go/arkruntime` |
| `.../service/arkruntime/model/responses` | `github.com/volcengine/ark-runtime-go/arkruntime/model/responses` |
| `NewClientWithApiKey(key)` | CN: `NewVolcClientWithApiKey(key)`; BP: `NewByteplusClientWithApiKey(key)` |

After imports are updated, run `go mod tidy`. Remove the legacy root SDK only
when no other service in the application imports it.

The legacy `service/arkruntime/model` root package does not have a one-to-one
replacement. Pick the new API-specific package such as `model/chat`,
`model/responses`, `model/embeddings`, or `model/images`.

## 3. Chat request mapping

Legacy Chat used `model.CreateChatCompletionRequest`, pointer message slices,
and pointer-based content. New Chat uses the `model/chat` package and explicit
message variants.

```go
// New SDK
req := &chat.ChatCompletionRequest{
    Model: model,
    Messages: []chat.ChatCompletionRequestMessage{
        {OneOf: chat.NewChatCompletionRequestUserMessageChatCompletionRequestMessageSum(
            chat.ChatCompletionRequestUserMessage{
                Role: chat.ChatCompletionRequestUserMessageRoleUser,
                Content: chat.NewStringChatCompletionMessageContent("Hello"),
            },
        )},
    },
}
```

The service method names remain recognizable:

| Legacy | New |
|---|---|
| `CreateChatCompletion(ctx, req)` | `CreateChatCompletion(ctx, &req)` |
| `CreateChatCompletionStream(ctx, req)` | `CreateChatCompletionStream(ctx, &req)` |
| `choice.Message.Content.StringValue` | `choice.Message.Content` |
| streaming `choice.Delta.Content` string | `choice.Delta.Content.Or("")` |

Use the regional basic Chat example as the canonical full conversion.

## 4. Responses request mapping

The main request maps from the legacy `responses.ResponsesRequest` to the new
type with the same name. Its union-valued fields change:

| Legacy shape | New shape |
|---|---|
| `ResponsesInput_StringValue` | `responses.NewStringResponsesInput(value)` |
| `ResponsesInput_ListValue` | `responses.NewInputItemArrayResponsesInput(items)` |
| `InputItem_InputMessage` | `responses.NewItemEasyMessageInputItemSum(message)` |
| `ContentItem_Text` | `responses.NewContentItemTextContentItemSum(text)` |
| pointer optional scalar | generated `responses.NewOpt*` helper |
| `PreviousResponseId` | `PreviousResponseID` |

Build from the leaves inward: content variant, message, input-item variant,
input list, then `ResponsesRequest`. This preserves the JSON discriminator and
payload together.

Do not copy a serialized legacy request body into a new struct. If an
application stores raw JSON templates, unmarshal them into the new type in a
test and compare the emitted JSON with the intended API body.

## 5. Stream-event mapping

Both SDKs use `stream.Recv()`, but the event representation changed.

| Legacy | New |
|---|---|
| `*responses.Event` | `*responses.ResponseStreamEvent` |
| `event.GetEventType()` | `event.OneOf.Type` |
| `event.GetText().GetDelta()` | `event.OneOf.ResponseTextDeltaEvent.Delta.Or("")` |
| `event.GetResponse()...GetId()` | read `ResponseCreatedEvent` or `ResponseCompletedEvent` variant |

Dispatch only on events the application consumes and retain a `default` case.
For function calling, capture the call ID from the output-item-done variant and
the response ID from a response event; send a function-call-output input item
in the next request. For MCP approvals, preserve both the approval request ID
and previous response ID.

## 6. Extra headers and regional behavior

Headers are call options in the new SDK:

```go
client.CreateResponses(ctx, req,
    arkruntime.WithCustomHeader("ark-beta-mcp", "true"))
client.CreateResponsesStream(ctx, req,
    arkruntime.WithCustomHeader("ark-beta-mcp", "true"))
```

Use `ark-beta-mcp` for MCP in either cloud. Web search
(`ark-beta-web-search`), knowledge search (`ark-beta-knowledge-search`), Doubao
App (`ark-beta-doubao-app`), and image process (`ark-beta-image-process`) are
CN-only. Do not silently drop a tool or header when migrating; fail migration
review if a CN-only tool appears in a BytePlus target.

## 7. Regional model IDs

Model names and endpoint IDs are cloud-specific. Prefer application
configuration, and update any legacy hard-coded default when changing clouds:

| API | Volcengine (CN) example | BytePlus example |
|---|---|---|
| Responses / Chat | `doubao-seed-2-1-pro-260628` | `seed-2-0-lite-260428` |
| Multimodal / sparse embeddings | `doubao-embedding-vision-251215` | `skylark-embedding-vision-251215` |
| Image generation | `doubao-seedream-5-0-pro-260628` | `dola-seedream-5-0-pro-260628` |
| Video generation | `doubao-seedance-2-0-fast-260128` | `dreamina-seedance-2-0-fast-260128` |

Use a model or endpoint ID provisioned for the target account if it differs
from these example defaults.

## 8. Validate the migration

1. Search the application for legacy imports and generic client constructors;
   none should remain in migrated Ark Runtime code.
2. Run `gofmt` on changed Go files, then `go mod tidy`.
3. Run `go test ./...` and `go vet ./...`.
4. Run a non-streaming request and check the returned content.
5. Run a streaming request through completion and verify error handling.
6. Smoke-test each built-in tool and confirm its beta header is present.
7. Run the same checks separately for CN and BytePlus if the application
   supports both; do not reuse a key, model, endpoint ID, or client across them.

Review every migrated request and event handler against the corresponding
regional example before calling the migration complete.
