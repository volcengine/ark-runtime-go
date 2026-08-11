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

	messages := []chat.ChatCompletionRequestMessage{
		systemMsg("你是豆包，是由字节跳动开发的 AI 人工智能助手"),
		userMsg("常见的十字花科植物有哪些？"),
	}

	fmt.Println("----- standard request -----")
	req := &chat.ChatCompletionRequest{
		Model:    "${YOUR_ENDPOINT_ID}",
		Messages: messages,
	}
	resp, err := client.CreateChatCompletion(ctx, req)
	if err != nil {
		fmt.Printf("standard chat error: %v\n", err)
		return
	}
	if len(resp.Choices) > 0 {
		fmt.Println(resp.Choices[0].Message.Content)
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
			fmt.Println()
			return
		}
		if err != nil {
			fmt.Printf("stream chat error: %v\n", err)
			return
		}
		if len(recv.Choices) > 0 {
			fmt.Print(recv.Choices[0].Delta.Content.Or(""))
		}
	}
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
