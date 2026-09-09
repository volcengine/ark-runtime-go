// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func main() {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "ark-self-hosted-mcp-example",
		Version: "1.0.0",
	}, nil)
	server.AddTool(&mcp.Tool{
		Name:        "mcp_echo",
		Description: "Echo text through the local MCP server.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}`),
	}, func(_ context.Context, request *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var input struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(request.Params.Arguments, &input); err != nil {
			return nil, err
		}
		return &mcp.CallToolResult{Content: []mcp.Content{
			&mcp.TextContent{Text: "MCP echo: " + input.Text},
		}}, nil
	})

	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
