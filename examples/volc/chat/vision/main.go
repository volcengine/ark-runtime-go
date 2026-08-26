// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/chat"
)

/**
 * Authentication
 * If you authorize your endpoint using an API key, you can set your api key to environment variable "ARK_API_KEY"
 * client := arkruntime.NewVolcClientWithApiKey(os.Getenv("ARK_API_KEY"))
 */

func main() {
	client := arkruntime.NewVolcClientWithApiKey(os.Getenv("ARK_API_KEY"))
	ctx := context.Background()

	textPart := chat.ChatCompletionContentPart{
		OneOf: chat.NewChatCompletionContentPartTextChatCompletionContentPartSum(
			chat.ChatCompletionContentPartText{
				Type: chat.ChatCompletionContentPartTextTypeText,
				Text: "这是哪里？",
			},
		),
	}
	imagePart := chat.ChatCompletionContentPart{
		OneOf: chat.NewChatCompletionContentPartImageChatCompletionContentPartSum(
			chat.ChatCompletionContentPartImage{
				Type: chat.ChatCompletionContentPartImageTypeImageURL,
				ImageURL: chat.ChatCompletionContentPartImageImageUrl{
					URL: chat.NewOptString("https://ark-project.tos-cn-beijing.volces.com/images/view.jpeg"),
				},
			},
		),
	}

	req := &chat.ChatCompletionRequest{
		Model: "doubao-seed-2-1-pro-260628",
		Messages: []chat.ChatCompletionRequestMessage{
			{
				OneOf: chat.NewChatCompletionRequestUserMessageChatCompletionRequestMessageSum(
					chat.ChatCompletionRequestUserMessage{
						Role:    chat.ChatCompletionRequestUserMessageRoleUser,
						Content: chat.NewChatCompletionContentPartArrayChatCompletionMessageContent([]chat.ChatCompletionContentPart{textPart, imagePart}),
					},
				),
			},
		},
	}

	fmt.Println("----- image input -----")
	resp, err := client.CreateChatCompletion(ctx, req)
	if err != nil {
		fmt.Printf("standard chat error: %v\n", err)
		return
	}
	if len(resp.Choices) > 0 {
		fmt.Println(resp.Choices[0].Message.Content)
	}
}
