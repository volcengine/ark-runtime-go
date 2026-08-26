// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Web-search tool example: registers the built-in `web_search` tool with
// MaxToolCalls=1 and asks the model a query that requires fresh
// information. Demonstrates both streaming and non-streaming code paths.
//
// Run with:
//
//	ARK_API_KEY=... go run ./examples/responses/web_search
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/responses"
)

const modelName = "doubao-seed-2-1-pro-260628"
const webSearchBetaHeader = "ark-beta-web-search"

func main() {
	stream()
	fmt.Println()
	nonStream()
}

func nonStream() {
	fmt.Println("===== non-streaming =====")
	client := arkruntime.NewVolcClientWithApiKey(os.Getenv("ARK_API_KEY"))
	ctx := context.Background()

	resp, err := client.CreateResponses(ctx, buildRequest(), arkruntime.WithCustomHeader(webSearchBetaHeader, "true"))
	if err != nil {
		fmt.Printf("create error: %v\n", err)
		return
	}
	fmt.Printf("response: %+v\n", resp)
}

func stream() {
	fmt.Println("===== streaming =====")
	client := arkruntime.NewVolcClientWithApiKey(os.Getenv("ARK_API_KEY"))
	ctx := context.Background()

	s, err := client.CreateResponsesStream(ctx, buildRequest(), arkruntime.WithCustomHeader(webSearchBetaHeader, "true"))
	if err != nil {
		fmt.Printf("stream error: %v\n", err)
		return
	}
	for {
		event, err := s.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Printf("stream error: %v\n", err)
			return
		}
		handleEvent(event)
	}
}

// buildRequest constructs a ResponsesRequest with the web-search tool
// registered. MaxToolCalls=1 caps how many search hops the model can make
// for this turn.
func buildRequest() *responses.ResponsesRequest {
	userMsg := responses.ItemEasyMessage{
		Role: responses.NewOptMessageRole(responses.MessageRoleUser),
		Content: responses.NewContentItemArrayMessageContent([]responses.ContentItem{
			{
				OneOf: responses.NewContentItemTextContentItemSum(responses.ContentItemText{
					Type: responses.ContentItemTextTypeInputText,
					Text: "查一下今天的AI相关新闻",
				}),
			},
		}),
	}

	webSearch := responses.Tool{
		OneOf: responses.NewWebSearchToolToolSum(responses.WebSearchTool{
			Type: responses.WebSearchToolTypeWebSearch,
		}),
	}

	return &responses.ResponsesRequest{
		Model: modelName,
		Input: responses.NewInputItemArrayResponsesInput([]responses.InputItem{
			{OneOf: responses.NewItemEasyMessageInputItemSum(userMsg)},
		}),
		Tools:        []responses.Tool{webSearch},
		MaxToolCalls: responses.NewOptInt64(1),
	}
}

func handleEvent(event *responses.ResponseStreamEvent) {
	switch event.OneOf.Type {
	case responses.ResponseReasoningSummaryTextDeltaEventResponseStreamEventSum:
		fmt.Print(event.OneOf.ResponseReasoningSummaryTextDeltaEvent.Delta.Or(""))
	case responses.ResponseReasoningSummaryTextDoneEventResponseStreamEventSum:
		fmt.Printf("\n[reasoning done] %s\n", event.OneOf.ResponseReasoningSummaryTextDoneEvent.Text.Or(""))
	case responses.ResponseTextDeltaEventResponseStreamEventSum:
		fmt.Print(event.OneOf.ResponseTextDeltaEvent.Delta.Or(""))
	case responses.ResponseTextDoneEventResponseStreamEventSum:
		fmt.Printf("\n[text done] %s\n", event.OneOf.ResponseTextDoneEvent.Text.Or(""))
	}
}
