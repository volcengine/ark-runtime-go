// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"net/http"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/chat"
	"github.com/volcengine/ark-runtime-go/arkruntime/utils"
)

// Request and response types live in the chat package, regenerated from
// openapi/chat/openapi.yaml. The *WithHeader wrapper satisfies model.Response
// so the request pipeline can attach headers; it delegates JSON
// (un)marshalling to the embedded generated type.

// CreateChatCompletion creates a non-streaming chat completion.
//
// Defensively clears body.Stream so callers that reuse a single
// *ChatCompletionRequest across CreateChatCompletionStream and
// CreateChatCompletion don't carry the streaming flag into a
// non-streaming request — which would otherwise make the server emit
// SSE and break this method's JSON decoder.
func (c *Client) CreateChatCompletion(ctx context.Context, body *chat.ChatCompletionRequest, opts ...requestOption) (res *chat.ChatCompletionResponseWithHeader, err error) {
	body.Stream = chat.NewOptBool(false)
	path := "/chat/completions"
	opts = append(opts, withBody(body))
	res = &chat.ChatCompletionResponseWithHeader{}
	err = c.Do(ctx, http.MethodPost, c.fullURL(path), resourceTypeEndpoint, body.Model, res, opts...)
	return
}

// CreateChatCompletionStream creates a streaming chat completion. The
// returned reader yields *chat.ChatCompletionStreamResponse chunks until
// the server emits [DONE] or an error.
func (c *Client) CreateChatCompletionStream(ctx context.Context, body *chat.ChatCompletionRequest, opts ...requestOption) (stream *utils.ChatGenStreamReader, err error) {
	body.Stream = chat.NewOptBool(true)
	opts = append(opts, withBody(body))
	path := "/chat/completions"
	return c.ChatGenStreamRequestDo(ctx, http.MethodPost, c.fullURL(path), body.Model, opts...)
}
