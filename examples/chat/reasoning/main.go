// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/chat"
)

/**
 * Authentication
 * If you authorize your endpoint using an API key, you can set your api key to environment variable "ARK_API_KEY"
 * client := arkruntime.NewClientWithApiKey(os.Getenv("ARK_API_KEY"))
 */

func main() {
	client := arkruntime.NewClientWithApiKey(os.Getenv("ARK_API_KEY"))
	ctx := context.Background()

	req := &chat.ChatCompletionRequest{
		Model: "${YOUR_ENDPOINT_ID}",
		Messages: []chat.ChatCompletionRequestMessage{
			userMsg("How many Rs are there in the word 'strawberry'?"),
		},
		Thinking: chat.NewOptThinking(chat.Thinking{
			Type: chat.NewOptThinkingMode(chat.ThinkingModeEnabled),
		}),
	}

	fmt.Println("----- streaming request -----")
	stream, err := client.CreateChatCompletionStream(ctx, req)
	if err != nil {
		fmt.Printf("stream chat error: %v\n", err)
		return
	}
	defer stream.Close()

	for {
		recv, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("stream chat error: %v\n", err)
			break
		}
		if len(recv.Choices) == 0 {
			continue
		}
		delta := recv.Choices[0].Delta
		if r, ok := delta.ReasoningContent.Get(); ok && r != "" {
			fmt.Print(r)
		} else {
			fmt.Print(delta.Content.Or(""))
		}
	}
	fmt.Println()

	fmt.Println("----- standard request -----")
	resp, err := client.CreateChatCompletion(ctx, req)
	if err != nil {
		fmt.Printf("standard chat error: %v\n", err)
		return
	}
	if len(resp.Choices) > 0 {
		msg := resp.Choices[0].Message
		if r, ok := msg.ReasoningContent.Get(); ok && r != "" {
			fmt.Println(r)
		}
		fmt.Println(msg.Content)
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
