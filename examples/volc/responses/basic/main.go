// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/responses"
)

/**
 * Authentication
 * If you authorize your endpoint using an API key, you can set your api key to environment variable "ARK_API_KEY"
 * client := arkruntime.NewVolcClientWithApiKey(os.Getenv("ARK_API_KEY"))
 */

func main() {
	client := arkruntime.NewVolcClientWithApiKey(os.Getenv("ARK_API_KEY"))
	ctx := context.Background()

	fmt.Println("----- round 1 message -----")
	// round 1 message: a single text input item
	userMessage := responses.ItemEasyMessage{
		Role: responses.NewOptMessageRole(responses.MessageRoleUser),
		Content: responses.NewContentItemArrayMessageContent([]responses.ContentItem{
			{
				OneOf: responses.NewContentItemTextContentItemSum(responses.ContentItemText{
					Type: responses.ContentItemTextTypeInputText,
					Text: "请介绍一下你自己",
				}),
			},
		}),
	}
	inputItem := responses.InputItem{
		OneOf: responses.NewItemEasyMessageInputItemSum(userMessage),
	}

	req := &responses.ResponsesRequest{
		Model: "doubao-seed-2-1-pro-260628",
		Input: responses.NewInputItemArrayResponsesInput([]responses.InputItem{inputItem}),
	}

	stream, err := client.CreateResponsesStream(ctx, req)
	if err != nil {
		fmt.Printf("stream error: %v\n", err)
		return
	}
	var responseID string
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("stream error: %v\n", err)
			return
		}
		handleEvent(event)
		if event.OneOf.Type == responses.ResponseCreatedEventResponseStreamEventSum {
			responseID = event.OneOf.ResponseCreatedEvent.Response.ID
		}
	}

	fmt.Println()
	// round 2: reference the prior response and pass a plain string input
	fmt.Println("----- round 2 -----")
	req.Input = responses.NewStringResponsesInput("总结一下我们刚才聊了什么")
	req.PreviousResponseID = responses.NewOptString(responseID)
	stream, err = client.CreateResponsesStream(ctx, req)
	if err != nil {
		fmt.Printf("stream error: %v\n", err)
		return
	}
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("stream error: %v\n", err)
			return
		}
		handleEvent(event)
	}
	fmt.Println()
	fmt.Println(responseID)
}

func handleEvent(event *responses.ResponseStreamEvent) {
	switch event.OneOf.Type {
	case responses.ResponseReasoningSummaryTextDeltaEventResponseStreamEventSum:
		fmt.Print(event.OneOf.ResponseReasoningSummaryTextDeltaEvent.Delta.Or(""))
	case responses.ResponseReasoningSummaryTextDoneEventResponseStreamEventSum:
		fmt.Printf("\naggregated reasoning text: %s\n", event.OneOf.ResponseReasoningSummaryTextDoneEvent.Text.Or(""))
	case responses.ResponseTextDeltaEventResponseStreamEventSum:
		fmt.Print(event.OneOf.ResponseTextDeltaEvent.Delta.Or(""))
	case responses.ResponseTextDoneEventResponseStreamEventSum:
		fmt.Printf("\naggregated output text: %s\n", event.OneOf.ResponseTextDoneEvent.Text.Or(""))
	default:
		return
	}
}
