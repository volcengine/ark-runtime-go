// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Function-calling example: registers a `sum(a, b)` tool, sends a query
// shaped to trigger it, executes the tool locally when the model emits a
// call, and feeds the result back as a follow-up turn so the model can
// produce its final natural-language answer.
//
// Run with:
//
//	ARK_API_KEY=... go run ./examples/responses/function_call
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/go-faster/jx"
	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/responses"
)

const modelName = "seed-2-0-lite-260428"

func main() {
	client := arkruntime.NewByteplusClientWithApiKey(os.Getenv("ARK_API_KEY"))
	ctx := context.Background()

	sumTool := responses.Tool{
		OneOf: responses.NewFunctionToolToolSum(responses.FunctionTool{
			Type:        responses.FunctionToolTypeFunction,
			Name:        "sum",
			Description: responses.NewOptString("add two integers and return the sum"),
			Parameters: responses.NewOptFunctionToolParameters(responses.FunctionToolParameters{
				"type": jx.Raw(`"object"`),
				"properties": jx.Raw(`{
					"a": {"type": "integer", "description": "first addend"},
					"b": {"type": "integer", "description": "second addend"}
				}`),
				"required": jx.Raw(`["a", "b"]`),
			}),
		}),
	}

	userMsg := responses.ItemEasyMessage{
		Role: responses.NewOptMessageRole(responses.MessageRoleUser),
		Content: responses.NewContentItemArrayMessageContent([]responses.ContentItem{
			{
				OneOf: responses.NewContentItemTextContentItemSum(responses.ContentItemText{
					Type: responses.ContentItemTextTypeInputText,
					Text: "请用 sum 工具帮我计算 1 + 2 等于多少",
				}),
			},
		}),
	}

	req := &responses.ResponsesRequest{
		Model: modelName,
		Input: responses.NewInputItemArrayResponsesInput([]responses.InputItem{
			{OneOf: responses.NewItemEasyMessageInputItemSum(userMsg)},
		}),
		Tools: []responses.Tool{sumTool},
	}

	fmt.Println("----- round 1: model decides to call the tool -----")
	toolCall, err := streamUntilToolCall(ctx, client, req)
	if err != nil {
		fmt.Printf("round 1 stream error: %v\n", err)
		return
	}
	if toolCall == nil {
		fmt.Println("model returned a plain-text answer; tool was not invoked")
		return
	}

	args := toolCall.Arguments.Or("")
	callID := toolCall.CallID.Or("")
	fmt.Printf("\n[tool call] name=%s call_id=%s arguments=%s\n",
		toolCall.Name.Or(""), callID, args)

	result, err := executeSum(args)
	if err != nil {
		fmt.Printf("local sum() error: %v\n", err)
		return
	}
	fmt.Printf("[tool result] sum=%s\n", result)

	// Round 2: append the tool's call (the assistant's emission) and the
	// tool's output (our local result) to the input list, then re-stream so
	// the model can produce its natural-language answer using the result.
	toolOutput := responses.ItemFunctionToolCallOutput{
		Type:   responses.ItemFunctionToolCallOutputTypeFunctionCallOutput,
		CallID: responses.NewOptString(callID),
		Output: responses.NewOptMessageContent(responses.NewStringMessageContent(result)),
	}

	req.Input = responses.NewInputItemArrayResponsesInput([]responses.InputItem{
		{OneOf: responses.NewItemEasyMessageInputItemSum(userMsg)},
		{OneOf: responses.NewItemFunctionToolCallInputItemSum(*toolCall)},
		{OneOf: responses.NewItemFunctionToolCallOutputInputItemSum(toolOutput)},
	})

	fmt.Println("\n----- round 2: model produces final answer -----")
	if err := streamFinalAnswer(ctx, client, req); err != nil {
		fmt.Printf("round 2 stream error: %v\n", err)
		return
	}
	fmt.Println()
}

// streamUntilToolCall consumes the stream and watches for an
// `output_item.done` event whose item is an ItemFunctionToolCall, returning
// it. Returns nil if the model produced only text and never called a tool.
// Reasoning + text deltas are printed for visibility.
func streamUntilToolCall(
	ctx context.Context,
	client *arkruntime.Client,
	req *responses.ResponsesRequest,
) (*responses.ItemFunctionToolCall, error) {
	stream, err := client.CreateResponsesStream(ctx, req)
	if err != nil {
		return nil, err
	}
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		switch event.OneOf.Type {
		case responses.ResponseReasoningSummaryTextDeltaEventResponseStreamEventSum:
			fmt.Print(event.OneOf.ResponseReasoningSummaryTextDeltaEvent.Delta.Or(""))
		case responses.ResponseTextDeltaEventResponseStreamEventSum:
			fmt.Print(event.OneOf.ResponseTextDeltaEvent.Delta.Or(""))
		case responses.ResponseOutputItemDoneEventResponseStreamEventSum:
			item := event.OneOf.ResponseOutputItemDoneEvent.Item
			if call, ok := item.OneOf.GetItemFunctionToolCall(); ok {
				return &call, nil
			}
		}
	}
}

// streamFinalAnswer prints text/reasoning deltas to stdout until EOF.
func streamFinalAnswer(
	ctx context.Context,
	client *arkruntime.Client,
	req *responses.ResponsesRequest,
) error {
	stream, err := client.CreateResponsesStream(ctx, req)
	if err != nil {
		return err
	}
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch event.OneOf.Type {
		case responses.ResponseReasoningSummaryTextDeltaEventResponseStreamEventSum:
			fmt.Print(event.OneOf.ResponseReasoningSummaryTextDeltaEvent.Delta.Or(""))
		case responses.ResponseTextDeltaEventResponseStreamEventSum:
			fmt.Print(event.OneOf.ResponseTextDeltaEvent.Delta.Or(""))
		case responses.ResponseTextDoneEventResponseStreamEventSum:
			fmt.Printf("\n[final text] %s\n", event.OneOf.ResponseTextDoneEvent.Text.Or(""))
		}
	}
}

// executeSum parses the JSON arguments emitted by the model and computes
// the sum locally. Returns the result as a string so it can flow back to
// the model verbatim as a tool output.
func executeSum(jsonArgs string) (string, error) {
	jsonArgs = strings.TrimSpace(jsonArgs)
	if jsonArgs == "" {
		return "", fmt.Errorf("empty arguments")
	}
	var parsed struct {
		A int64 `json:"a"`
		B int64 `json:"b"`
	}
	if err := json.Unmarshal([]byte(jsonArgs), &parsed); err != nil {
		return "", fmt.Errorf("parse arguments %q: %w", jsonArgs, err)
	}
	return fmt.Sprintf("%d", parsed.A+parsed.B), nil
}
