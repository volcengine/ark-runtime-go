// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/go-faster/jx"
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

	tools := []chat.ChatCompletionTool{weatherTool()}
	req := &chat.ChatCompletionRequest{
		Model: "${YOUR_ENDPOINT_ID}",
		Messages: []chat.ChatCompletionRequestMessage{
			userMsg("What's the weather like in Boston today?"),
		},
		Tools: tools,
	}

	fmt.Println("----- function call request -----")
	resp, err := client.CreateChatCompletion(ctx, req)
	if err != nil {
		fmt.Printf("standard chat error: %v\n", err)
		return
	}
	out, _ := json.Marshal(resp)
	fmt.Println(string(out))

	fmt.Println("----- function call stream request -----")
	stream, err := client.CreateChatCompletionStream(ctx, req)
	if err != nil {
		fmt.Printf("stream chat error: %v\n", err)
		return
	}
	defer stream.Close()

	type aggCall struct {
		ID   string
		Name string
		Args string
	}
	finalToolCalls := map[int32]*aggCall{}
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
		fmt.Print(delta.Content.Or(""))
		for _, tc := range delta.ToolCalls {
			cur, ok := finalToolCalls[tc.Index]
			if !ok {
				cur = &aggCall{}
				finalToolCalls[tc.Index] = cur
			}
			if id, ok := tc.ID.Get(); ok && cur.ID == "" {
				cur.ID = id
			}
			if fn, ok := tc.Function.Get(); ok {
				if name, ok := fn.Name.Get(); ok && cur.Name == "" {
					cur.Name = name
				}
				cur.Args += fn.Arguments.Or("")
			}
		}
	}
	for _, c := range finalToolCalls {
		fmt.Printf("\ntool_call id=%s name=%s args=%s\n", c.ID, c.Name, c.Args)
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

// weatherTool builds the get_current_weather tool used in this example.
func weatherTool() chat.ChatCompletionTool {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"location": map[string]any{
				"type":        "string",
				"description": "The city and state, e.g. San Francisco, CA",
			},
			"unit": map[string]any{
				"type": "string",
				"enum": []string{"celsius", "fahrenheit"},
			},
		},
		"required": []string{"location"},
	}
	return chat.ChatCompletionTool{
		Type: chat.ToolTypeFunction,
		Function: chat.FunctionObject{
			Name:        "get_current_weather",
			Description: chat.NewOptString("Get the current weather in a given location"),
			Parameters:  chat.NewOptFunctionObjectParameters(toRaw(schema)),
		},
	}
}

func toRaw(m map[string]any) chat.FunctionObjectParameters {
	out := make(chat.FunctionObjectParameters, len(m))
	for k, v := range m {
		b, _ := json.Marshal(v)
		out[k] = jx.Raw(b)
	}
	return out
}
