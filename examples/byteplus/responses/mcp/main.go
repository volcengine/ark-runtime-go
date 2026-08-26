// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// MCP tool example: registers a remote MCP server (deepwiki) as a tool,
// walks through the human-in-the-loop approval flow when the model asks
// to call it, and demonstrates skipping approval entirely with
// “RequireApproval = never“. Both streaming and non-streaming variants.
//
// MCP support is currently behind the “ark-beta-mcp: true“ request
// header, which the example sets on every call.
//
// Run with:
//
//	ARK_API_KEY=... go run ./examples/responses/mcp
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
	modelName     = "seed-2-0-lite-260428"
	mcpServerURL  = "https://mcp.deepwiki.com/mcp"
	mcpLabel      = "deepwiki"
	mcpBetaHeader = "ark-beta-mcp"
	userPrompt    = "查一下 mark3labs/mcp-go 这个仓库的结构"
)

func main() {
	stream()
	fmt.Println()
	nonStream()
}

// nonStream demonstrates the three rounds with non-streaming requests.
//
//  1. Send the prompt → model returns an mcp_approval_request.
//  2. Send back an mcp_approval_response with approve=true → model now
//     executes the call and returns the answer.
//  3. Re-send the original prompt with RequireApproval=never to skip the
//     approval roundtrip entirely.
func nonStream() {
	fmt.Println("===== non-streaming =====")
	client := arkruntime.NewByteplusClientWithApiKey(os.Getenv("ARK_API_KEY"))
	ctx := context.Background()

	// Round 1: ask, model responds with an approval request.
	fmt.Println("----- round 1: prompt → approval request -----")
	req1 := &responses.ResponsesRequest{
		Model: modelName,
		Input: responses.NewInputItemArrayResponsesInput([]responses.InputItem{
			{OneOf: responses.NewItemEasyMessageInputItemSum(buildUserMessage())},
		}),
		Tools: []responses.Tool{mcpTool(false)},
	}
	resp1, err := client.CreateResponses(ctx, req1, arkruntime.WithCustomHeader(mcpBetaHeader, "true"))
	if err != nil {
		fmt.Printf("round 1 error: %v\n", err)
		return
	}
	fmt.Printf("response: %+v\n", resp1)

	approvalReqID := findApprovalRequestID(resp1.Output)
	if approvalReqID == "" {
		fmt.Println("no approval request emitted; nothing to approve")
		return
	}
	responseID := resp1.ID

	// Round 2: approve.
	fmt.Println("\n----- round 2: approve -----")
	req2 := &responses.ResponsesRequest{
		Model: modelName,
		Input: responses.NewInputItemArrayResponsesInput([]responses.InputItem{
			{OneOf: responses.NewItemFunctionMcpApprovalResponseInputItemSum(responses.ItemFunctionMcpApprovalResponse{
				Type:              responses.ItemFunctionMcpApprovalResponseTypeMcpApprovalResponse,
				ApprovalRequestID: approvalReqID,
				Approve:           true,
			})},
		}),
		Tools:              []responses.Tool{mcpTool(false)},
		PreviousResponseID: responses.NewOptString(responseID),
	}
	resp2, err := client.CreateResponses(ctx, req2, arkruntime.WithCustomHeader(mcpBetaHeader, "true"))
	if err != nil {
		fmt.Printf("round 2 error: %v\n", err)
		return
	}
	fmt.Printf("response: %+v\n", resp2)

	// Round 3: never require approval.
	fmt.Println("\n----- round 3: skip approval entirely -----")
	req3 := &responses.ResponsesRequest{
		Model: modelName,
		Input: responses.NewInputItemArrayResponsesInput([]responses.InputItem{
			{OneOf: responses.NewItemEasyMessageInputItemSum(buildUserMessage())},
		}),
		Tools: []responses.Tool{mcpTool(true)},
	}
	resp3, err := client.CreateResponses(ctx, req3, arkruntime.WithCustomHeader(mcpBetaHeader, "true"))
	if err != nil {
		fmt.Printf("round 3 error: %v\n", err)
		return
	}
	fmt.Printf("response: %+v\n", resp3)
}

// stream mirrors nonStream but consumes the SSE stream. The approval
// request id is captured from output_item.done events instead of the
// final response object.
func stream() {
	fmt.Println("===== streaming =====")
	client := arkruntime.NewByteplusClientWithApiKey(os.Getenv("ARK_API_KEY"))
	ctx := context.Background()

	fmt.Println("----- round 1: prompt → approval request (streaming) -----")
	req1 := &responses.ResponsesRequest{
		Model: modelName,
		Input: responses.NewInputItemArrayResponsesInput([]responses.InputItem{
			{OneOf: responses.NewItemEasyMessageInputItemSum(buildUserMessage())},
		}),
		Tools: []responses.Tool{mcpTool(false)},
	}
	responseID, approvalReqID, err := streamAndCapture(ctx, client, req1)
	if err != nil {
		fmt.Printf("round 1 stream error: %v\n", err)
		return
	}
	if approvalReqID == "" {
		fmt.Println("no approval request emitted; nothing to approve")
		return
	}

	fmt.Println("\n----- round 2: approve (streaming) -----")
	req2 := &responses.ResponsesRequest{
		Model: modelName,
		Input: responses.NewInputItemArrayResponsesInput([]responses.InputItem{
			{OneOf: responses.NewItemFunctionMcpApprovalResponseInputItemSum(responses.ItemFunctionMcpApprovalResponse{
				Type:              responses.ItemFunctionMcpApprovalResponseTypeMcpApprovalResponse,
				ApprovalRequestID: approvalReqID,
				Approve:           true,
			})},
		}),
		Tools:              []responses.Tool{mcpTool(false)},
		PreviousResponseID: responses.NewOptString(responseID),
	}
	if _, _, err := streamAndCapture(ctx, client, req2); err != nil {
		fmt.Printf("round 2 stream error: %v\n", err)
		return
	}

	fmt.Println("\n----- round 3: skip approval entirely (streaming) -----")
	req3 := &responses.ResponsesRequest{
		Model: modelName,
		Input: responses.NewInputItemArrayResponsesInput([]responses.InputItem{
			{OneOf: responses.NewItemEasyMessageInputItemSum(buildUserMessage())},
		}),
		Tools: []responses.Tool{mcpTool(true)},
	}
	if _, _, err := streamAndCapture(ctx, client, req3); err != nil {
		fmt.Printf("round 3 stream error: %v\n", err)
	}
}

func buildUserMessage() responses.ItemEasyMessage {
	return responses.ItemEasyMessage{
		Role: responses.NewOptMessageRole(responses.MessageRoleUser),
		Content: responses.NewContentItemArrayMessageContent([]responses.ContentItem{
			{
				OneOf: responses.NewContentItemTextContentItemSum(responses.ContentItemText{
					Type: responses.ContentItemTextTypeInputText,
					Text: userPrompt,
				}),
			},
		}),
	}
}

// mcpTool builds the deepwiki MCP tool. When neverApprove is true, the
// model is told to skip the approval roundtrip and call directly.
func mcpTool(neverApprove bool) responses.Tool {
	mcp := responses.McpTool{
		Type:              responses.McpToolTypeMcp,
		ServerLabel:       mcpLabel,
		ServerURL:         mcpServerURL,
		ServerDescription: responses.NewOptString("test desc"),
	}
	if neverApprove {
		mcp.RequireApproval = responses.NewOptMcpRequireApproval(
			responses.NewMcpApprovalModeMcpRequireApproval(responses.McpApprovalModeNever),
		)
	}
	return responses.Tool{OneOf: responses.NewMcpToolToolSum(mcp)}
}

// findApprovalRequestID scans the model's Output items for an
// ItemFunctionMcpApprovalRequest and returns its id (empty if none).
func findApprovalRequestID(output []responses.OutputItem) string {
	for i := len(output) - 1; i >= 0; i-- {
		if req, ok := output[i].OneOf.GetItemFunctionMcpApprovalRequest(); ok {
			return req.ID.Or("")
		}
	}
	return ""
}

// streamAndCapture prints reasoning + text deltas, surfaces MCP-specific
// progress events for visibility, and returns (responseID,
// approvalRequestID) so the caller can chain follow-up rounds.
func streamAndCapture(
	ctx context.Context,
	client *arkruntime.Client,
	req *responses.ResponsesRequest,
) (string, string, error) {
	stream, err := client.CreateResponsesStream(ctx, req, arkruntime.WithCustomHeader(mcpBetaHeader, "true"))
	if err != nil {
		return "", "", err
	}
	var responseID, approvalReqID string
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return responseID, approvalReqID, nil
		}
		if err != nil {
			return responseID, approvalReqID, err
		}
		switch event.OneOf.Type {
		case responses.ResponseCreatedEventResponseStreamEventSum:
			responseID = event.OneOf.ResponseCreatedEvent.Response.ID
		case responses.ResponseReasoningSummaryTextDeltaEventResponseStreamEventSum:
			fmt.Print(event.OneOf.ResponseReasoningSummaryTextDeltaEvent.Delta.Or(""))
		case responses.ResponseTextDeltaEventResponseStreamEventSum:
			fmt.Print(event.OneOf.ResponseTextDeltaEvent.Delta.Or(""))
		case responses.ResponseTextDoneEventResponseStreamEventSum:
			fmt.Printf("\n[text done] %s\n", event.OneOf.ResponseTextDoneEvent.Text.Or(""))
		case responses.ResponseOutputItemDoneEventResponseStreamEventSum:
			item := event.OneOf.ResponseOutputItemDoneEvent.Item
			if req, ok := item.OneOf.GetItemFunctionMcpApprovalRequest(); ok {
				if id := req.ID.Or(""); id != "" {
					approvalReqID = id
				}
				fmt.Printf("\n[mcp approval request] name=%s arguments=%s\n",
					req.Name, req.Arguments)
			}
		}
	}
}
