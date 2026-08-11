// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// DoubaoApp tool example: drives the four built-in Doubao app features
// (chat / deep_chat / ai_search / reasoning_search) through the
// Responses API, in both streaming and non-streaming variants.
//
// The DoubaoApp tool is currently behind the ``ark-beta-doubao-app: true``
// request header, which the example sets on every call.
//
// Run with:
//
//	ARK_API_KEY=... go run ./examples/responses/doubao_app
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/responses"
)

const (
	modelName    = "doubao-seed-2-1-pro"
	doubaoHeader = "ark-beta-doubao-app"
)

const (
	chatFeature            = "chat"
	deepChatFeature        = "deepChat"
	aiSearchFeature        = "aiSearch"
	reasoningSearchFeature = "reasoningSearch"
)

var featureToQuery = map[string]string{
	chatFeature:            "你好介绍一下你自己",
	deepChatFeature:        "为什么天空是蓝色",
	aiSearchFeature:        "今天的AI新闻",
	reasoningSearchFeature: "今天的AI新闻",
}

func main() {
	stream(chatFeature)    // change to deepChatFeature, aiSearchFeature, reasoningSearchFeature to test other features
	fmt.Println()
	nonStream(chatFeature)
}

func nonStream(feature string) {
	fmt.Println("===== non-streaming =====")
	client := arkruntime.NewClientWithApiKey(os.Getenv("ARK_API_KEY"))
	ctx := context.Background()

	fmt.Println("----- round 1 message -----")
	req := &responses.ResponsesRequest{
		Model: modelName,
		Input: textInput(featureToQuery[feature]),
		Tools: []responses.Tool{doubaoAppTool(feature)},
	}
	resp, err := client.CreateResponses(ctx, req, arkruntime.WithCustomHeader(doubaoHeader, "true"))
	if err != nil {
		fmt.Printf("request error: %v\n", err)
		return
	}
	fmt.Printf("response id: %s\n", resp.ID)
	fmt.Printf("output text: %s\n", firstOutputText(&resp.Response))

	fmt.Println()
	fmt.Println("----- round 2 -----")
	req2 := &responses.ResponsesRequest{
		Model:              modelName,
		Input:              textInput("刚刚我们聊了什么"),
		Tools:              []responses.Tool{doubaoAppTool(feature)},
		PreviousResponseID: responses.NewOptString(resp.ID),
	}
	resp2, err := client.CreateResponses(ctx, req2, arkruntime.WithCustomHeader(doubaoHeader, "true"))
	if err != nil {
		fmt.Printf("request error: %v\n", err)
		return
	}
	fmt.Printf("response id: %s\n", resp2.ID)
	fmt.Printf("output text: %s\n", firstOutputText(&resp2.Response))
}

func stream(feature string) {
	fmt.Println("===== streaming =====")
	client := arkruntime.NewClientWithApiKey(os.Getenv("ARK_API_KEY"))
	ctx := context.Background()

	fmt.Println("----- round 1 message -----")
	req := &responses.ResponsesRequest{
		Model: modelName,
		Input: textInput(featureToQuery[feature]),
		Tools: []responses.Tool{doubaoAppTool(feature)},
	}
	s, err := client.CreateResponsesStream(ctx, req, arkruntime.WithCustomHeader(doubaoHeader, "true"))
	if err != nil {
		fmt.Printf("stream error: %v\n", err)
		return
	}
	var responseID string
	for {
		event, err := s.Recv()
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
	fmt.Println("----- round 2 -----")
	req2 := &responses.ResponsesRequest{
		Model:              modelName,
		Input:              textInput("刚刚我们聊了什么"),
		Tools:              []responses.Tool{doubaoAppTool(feature)},
		PreviousResponseID: responses.NewOptString(responseID),
	}
	s2, err := client.CreateResponsesStream(ctx, req2, arkruntime.WithCustomHeader(doubaoHeader, "true"))
	if err != nil {
		fmt.Printf("stream error: %v\n", err)
		return
	}
	for {
		event, err := s2.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("stream error: %v\n", err)
			return
		}
		handleEvent(event)
	}
}

func textInput(text string) responses.ResponsesInput {
	msg := responses.ItemEasyMessage{
		Role: responses.NewOptMessageRole(responses.MessageRoleUser),
		Content: responses.NewContentItemArrayMessageContent([]responses.ContentItem{
			{
				OneOf: responses.NewContentItemTextContentItemSum(responses.ContentItemText{
					Type: responses.ContentItemTextTypeInputText,
					Text: text,
				}),
			},
		}),
	}
	return responses.NewInputItemArrayResponsesInput([]responses.InputItem{
		{OneOf: responses.NewItemEasyMessageInputItemSum(msg)},
	})
}

func doubaoAppTool(feature string) responses.Tool {
	enabled := responses.NewOptDoubaoAppFeatureMode(responses.DoubaoAppFeatureModeEnabled)
	item := responses.OptDoubaoAppFeatureItem{}
	item.SetTo(responses.DoubaoAppFeatureItem{Type: enabled})

	var f responses.DoubaoAppFeature
	switch feature {
	case chatFeature:
		f.Chat = item
	case deepChatFeature:
		f.DeepChat = item
	case aiSearchFeature:
		f.AiSearch = item
	case reasoningSearchFeature:
		f.ReasoningSearch = item
	}
	return responses.Tool{
		OneOf: responses.NewDoubaoAppToolToolSum(responses.DoubaoAppTool{
			Type:    responses.DoubaoAppToolTypeDoubaoApp,
			Feature: f,
		}),
	}
}

func firstOutputText(r *responses.Response) string {
	for _, item := range r.Output {
		if msg, ok := item.OneOf.GetItemOutputMessage(); ok {
			for _, c := range msg.Content {
				if t, ok := c.OneOf.GetOutputContentItemText(); ok {
					if s := t.Text.Or(""); s != "" {
						return s
					}
				}
			}
		}
	}
	return ""
}

func handleEvent(event *responses.ResponseStreamEvent) {
	switch event.OneOf.Type {
	case responses.ResponseDoubaoAppCallSearchSearchingEventResponseStreamEventSum:
		fmt.Printf("[searching] %s\n", event.OneOf.ResponseDoubaoAppCallSearchSearchingEvent.SearchingState.Or(""))
	case responses.ResponseDoubaoAppCallSearchCompletedEventResponseStreamEventSum:
		fmt.Printf("\n[search done] %s\n", event.OneOf.ResponseDoubaoAppCallSearchCompletedEvent.Summary.Or(""))
	case responses.ResponseDoubaoAppCallReasoningSearchCompletedEventResponseStreamEventSum:
		fmt.Printf("\n[reasoning_search done] %s\n", event.OneOf.ResponseDoubaoAppCallReasoningSearchCompletedEvent.Summary.Or(""))
	case responses.ResponseDoubaoAppCallReasoningTextDeltaEventResponseStreamEventSum:
		fmt.Print(event.OneOf.ResponseDoubaoAppCallReasoningTextDeltaEvent.Delta.Or(""))
	case responses.ResponseDoubaoAppCallReasoningTextDoneEventResponseStreamEventSum:
		fmt.Printf("\naggregated reasoning text: %s\n", event.OneOf.ResponseDoubaoAppCallReasoningTextDoneEvent.Text.Or(""))
	case responses.ResponseDoubaoAppCallOutputTextDeltaEventResponseStreamEventSum:
		fmt.Print(event.OneOf.ResponseDoubaoAppCallOutputTextDeltaEvent.Delta.Or(""))
	case responses.ResponseDoubaoAppCallOutputTextDoneEventResponseStreamEventSum:
		fmt.Printf("\naggregated output text: %s\n", event.OneOf.ResponseDoubaoAppCallOutputTextDoneEvent.Text.Or(""))
	}
}
