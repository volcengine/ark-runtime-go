// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	coremcp "github.com/volcengine/ark-runtime-go/arkruntime/selfhosted/mcp"
)

func TestCustomToolItem(t *testing.T) {
	item, err := CustomToolItem(&mcpsdk.Tool{
		Name:        "lookup_order",
		Description: "Lookup an order",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"order_id": map[string]any{"type": "string"},
			},
			"required":             []string{"order_id"},
			"additionalProperties": false,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if item.Type != "custom" || item.Name.Value != "lookup_order" ||
		!strings.HasPrefix(item.Description.Value, "Lookup an order") {
		t.Fatalf("unexpected custom tool item: %+v", item)
	}
	schema := item.InputSchema.Value
	if schema.Type.Value != "object" || len(schema.Required) != 1 || schema.Required[0] != "order_id" {
		t.Fatalf("unexpected custom tool schema: %+v", schema)
	}
	if got := string(schema.Properties.Value["order_id"]); got != `{"type":"string"}` {
		t.Fatalf("unexpected order_id schema: %s", got)
	}
	if !strings.Contains(item.Description.Value, `"additionalProperties":false`) {
		t.Fatalf("compatibility constraints missing from description: %s", item.Description.Value)
	}
}

func TestCustomToolItemDefaultsDescriptionAndSchema(t *testing.T) {
	item, err := CustomToolItem(&mcpsdk.Tool{Name: "ping"})
	if err != nil {
		t.Fatal(err)
	}
	if item.Description.Value != "ping" {
		t.Fatalf("description = %q, want ping", item.Description.Value)
	}
	if item.InputSchema.Value.Type.Value != "object" {
		t.Fatalf("schema type = %q, want object", item.InputSchema.Value.Type.Value)
	}
}

func TestCustomToolItemsRejectsDuplicateNames(t *testing.T) {
	_, err := CustomToolItems([]*mcpsdk.Tool{{Name: "same"}, {Name: "same"}})
	if err == nil {
		t.Fatal("expected duplicate name error")
	}
}

func TestRunnableToolCallsMCPSession(t *testing.T) {
	ctx := context.Background()
	server := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "server", Version: "1.0.0"}, nil)
	server.AddTool(&mcpsdk.Tool{
		Name:        "echo",
		Description: "Echo text",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}}}`),
	}, func(_ context.Context, request *mcpsdk.CallToolRequest) (*mcpsdk.CallToolResult, error) {
		var input struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(request.Params.Arguments, &input); err != nil {
			return nil, err
		}
		return &mcpsdk.CallToolResult{Content: []mcpsdk.Content{
			&mcpsdk.TextContent{Text: "echo: " + input.Text},
		}}, nil
	})

	serverTransport, clientTransport := mcpsdk.NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer serverSession.Close()
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "client", Version: "1.0.0"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer clientSession.Close()

	tool, err := NewTool(&mcpsdk.Tool{Name: "echo"}, clientSession)
	if err != nil {
		t.Fatal(err)
	}
	result := tool.Execute(ctx, json.RawMessage(`{"text":"hello"}`))
	if result.IsError || len(result.Content) != 1 || result.Content[0].Text != "echo: hello" {
		t.Fatalf("unexpected tool result: %+v", result)
	}
}

func TestConvertCallToolResult(t *testing.T) {
	converted, err := callToolResult(&mcpsdk.CallToolResult{
		Content: []mcpsdk.Content{
			&mcpsdk.ImageContent{MIMEType: "image/png", Data: []byte("image")},
			&mcpsdk.EmbeddedResource{Resource: &mcpsdk.ResourceContents{
				URI:      "file:///result.txt",
				MIMEType: "text/plain",
				Text:     "resource text",
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coremcp.ConvertCallToolResult(converted)
	if result.IsError || len(result.Content) != 2 {
		t.Fatalf("unexpected result: %+v", result)
	}
	imageSource, ok := result.Content[0].Source.(map[string]any)
	if !ok || imageSource["type"] != "base64" || imageSource["data"] != "aW1hZ2U=" {
		t.Fatalf("unexpected image source: %#v", result.Content[0].Source)
	}
	documentSource, ok := result.Content[1].Source.(map[string]any)
	if !ok || documentSource["type"] != "text" || documentSource["data"] != "resource text" {
		t.Fatalf("unexpected document source: %#v", result.Content[1].Source)
	}
}

func TestConvertStructuredAndErrorResults(t *testing.T) {
	converted, err := callToolResult(&mcpsdk.CallToolResult{
		StructuredContent: map[string]any{"status": "ok"},
	})
	if err != nil {
		t.Fatal(err)
	}
	structured := coremcp.ConvertCallToolResult(converted)
	if structured.IsError || len(structured.Content) != 1 || structured.Content[0].Text != `{"status":"ok"}` {
		t.Fatalf("unexpected structured result: %+v", structured)
	}

	converted, err = callToolResult(&mcpsdk.CallToolResult{
		Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: "not found"}},
		IsError: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	failed := coremcp.ConvertCallToolResult(converted)
	if !failed.IsError || len(failed.Content) != 1 || failed.Content[0].Text != "not found" {
		t.Fatalf("unexpected error result: %+v", failed)
	}
}
