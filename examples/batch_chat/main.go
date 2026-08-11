// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/chat"
)

/**
 * Authentication
 * If you authorize your endpoint using an API key, you can set your api key to environment variable "ARK_API_KEY"
 * client := arkruntime.NewClientWithApiKey(os.Getenv("ARK_API_KEY"))
 *
 * Batch chat completions use the same request/response shapes as
 * /chat/completions, but are dispatched against /batch/chat/completions
 * with a high-concurrency HTTP client and a per-model breaker that
 * honours Retry-After headers. Streaming is not supported.
 */

func main() {
	client := arkruntime.NewClientWithApiKey(
		os.Getenv("ARK_API_KEY"),
		arkruntime.WithBatchMaxParallel(3000), // max parallel in-flight batch requests
	)

	// In a real program, load requests from a file, queue, or DB. Here we
	// fan out a small fixed set so the example runs end-to-end.
	const total = 20
	requests := mockRequests("${YOUR_ENDPOINT_ID}", total)

	// Global deadline: if exceeded, every outstanding request is cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()

	var wg sync.WaitGroup
	for req := range requests {
		wg.Add(1)
		go func(req *chat.ChatCompletionRequest) {
			defer wg.Done()

			// Per-request deadline on top of the global one.
			reqCtx, reqCancel := context.WithTimeout(ctx, 10*time.Minute)
			defer reqCancel()

			resp, err := client.CreateBatchChatCompletion(reqCtx, req)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return
			}
			fmt.Println(mustMarshalJSON(resp))
		}(req)
	}
	wg.Wait()
}

func mockRequests(endpoint string, count int) <-chan *chat.ChatCompletionRequest {
	out := make(chan *chat.ChatCompletionRequest)
	go func() {
		defer close(out)
		for i := 0; i < count; i++ {
			out <- &chat.ChatCompletionRequest{
				Model: endpoint,
				Messages: []chat.ChatCompletionRequestMessage{
					systemMsg("你是豆包，是由字节跳动开发的 AI 人工智能助手"),
					userMsg("常见的十字花科植物有哪些？"),
				},
			}
		}
	}()
	return out
}

func systemMsg(content string) chat.ChatCompletionRequestMessage {
	return chat.ChatCompletionRequestMessage{
		OneOf: chat.NewChatCompletionRequestSystemMessageChatCompletionRequestMessageSum(
			chat.ChatCompletionRequestSystemMessage{
				Role:    chat.ChatCompletionRequestSystemMessageRoleSystem,
				Content: chat.NewStringChatCompletionMessageContent(content),
			},
		),
	}
}

func userMsg(content string) chat.ChatCompletionRequestMessage {
	return chat.ChatCompletionRequestMessage{
		OneOf: chat.NewChatCompletionRequestUserMessageChatCompletionRequestMessageSum(
			chat.ChatCompletionRequestUserMessage{
				Role:    chat.ChatCompletionRequestUserMessageRoleUser,
				Content: chat.NewStringChatCompletionMessageContent(content),
			},
		),
	}
}

func mustMarshalJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
