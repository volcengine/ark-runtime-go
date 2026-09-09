// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

type fakeClient struct {
	name      string
	arguments map[string]any
	result    *CallToolResult
}

func (f *fakeClient) CallTool(_ context.Context, name string, arguments map[string]any) (*CallToolResult, error) {
	f.name = name
	f.arguments = arguments
	return f.result, nil
}

func TestCustomToolItemAdaptsSchemaToCurrentAgentContract(t *testing.T) {
	item, err := CustomToolItem(ToolDefinition{
		Name:        "lookup_order",
		Description: "Lookup an order",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"order_id": map[string]any{"$ref": "#/$defs/order_id"},
			},
			"required":             []string{"order_id"},
			"additionalProperties": false,
			"$schema":              "https://json-schema.org/draft/2020-12/schema",
			"$defs": map[string]any{
				"order_id": map[string]any{"type": "string", "minLength": 1},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := item.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var itemJSON map[string]any
	if err := json.Unmarshal(raw, &itemJSON); err != nil {
		t.Fatal(err)
	}
	inputSchema, ok := itemJSON["input_schema"].(map[string]any)
	if !ok || len(inputSchema) != 3 {
		t.Fatalf("input schema does not match current agent contract: %s", raw)
	}
	if _, exists := inputSchema["additionalProperties"]; exists {
		t.Fatalf("unsupported top-level constraint leaked into input schema: %s", raw)
	}
	properties := inputSchema["properties"].(map[string]any)
	orderID := properties["order_id"].(map[string]any)
	if orderID["type"] != "string" || orderID["minLength"] != float64(1) || orderID["$ref"] != nil {
		t.Fatalf("local schema reference was not inlined: %s", raw)
	}
	description := itemJSON["description"].(string)
	if !strings.Contains(description, `MCP input constraints (JSON Schema): {"additionalProperties":false}`) ||
		strings.Contains(description, `"$defs"`) ||
		strings.Contains(description, `"$schema"`) {
		t.Fatalf("unexpected compatibility description: %s", description)
	}
}

func TestCustomToolItemRetainsReferencedDefinitions(t *testing.T) {
	item, err := CustomToolItem(ToolDefinition{
		Name: "lookup",
		InputSchema: map[string]any{
			"type":  "object",
			"allOf": []any{map[string]any{"$ref": "#/$defs/constraint"}},
			"$defs": map[string]any{
				"constraint": map[string]any{"additionalProperties": false},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(item.Description.Value, `"$defs"`) ||
		!strings.Contains(item.Description.Value, `"$ref":"#/$defs/constraint"`) {
		t.Fatalf("referenced definitions are absent from description: %s", item.Description.Value)
	}
}

func TestCustomToolItemDescribesUnresolvedReferences(t *testing.T) {
	item, err := CustomToolItem(ToolDefinition{
		Name: "lookup",
		InputSchema: map[string]any{
			"type":       "object",
			"properties": map[string]any{"order": map[string]any{"$ref": "https://example.com/order.json"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(item.Description.Value, `"$ref":"https://example.com/order.json"`) {
		t.Fatalf("unresolved reference is absent from description: %s", item.Description.Value)
	}
	if got := string(item.InputSchema.Value.Properties.Value["order"]); got != `{}` {
		t.Fatalf("unresolved reference leaked into input schema: %s", got)
	}
}

func TestCustomToolItemRejectsOversizedCompatibilityDescription(t *testing.T) {
	_, err := CustomToolItem(ToolDefinition{
		Name:        "large",
		Description: strings.Repeat("a", maxCustomToolDescriptionRunes),
		InputSchema: map[string]any{"type": "object", "additionalProperties": false},
	})
	if err == nil || !strings.Contains(err.Error(), "description exceeds") {
		t.Fatalf("expected description length error, got %v", err)
	}
}

func TestNewToolCallsClient(t *testing.T) {
	client := &fakeClient{result: &CallToolResult{
		Content: []Content{{Type: "text", Text: "echo: hello"}},
	}}
	tool, err := NewTool(ToolDefinition{Name: "echo"}, client)
	if err != nil {
		t.Fatal(err)
	}
	result := tool.Execute(context.Background(), json.RawMessage(`{"text":"hello"}`))
	if result.IsError || len(result.Content) != 1 || result.Content[0].Text != "echo: hello" {
		t.Fatalf("unexpected tool result: %+v", result)
	}
	if client.name != "echo" || client.arguments["text"] != "hello" {
		t.Fatalf("unexpected client call: name=%q arguments=%v", client.name, client.arguments)
	}
}

func TestNewToolAddsNameToEmptyError(t *testing.T) {
	tool, err := NewTool(ToolDefinition{Name: "lookup"}, &fakeClient{
		result: &CallToolResult{IsError: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := tool.Execute(context.Background(), json.RawMessage(`{}`))
	if !result.IsError || len(result.Content) != 1 ||
		result.Content[0].Text != `mcp tool "lookup" reported an error but returned no content` {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestConvertCallToolResultPreservesRichContent(t *testing.T) {
	result := ConvertCallToolResult(&CallToolResult{Content: []Content{
		{Type: "image", MIMEType: "image/png", Data: []byte("image")},
		{Type: "resource", Resource: &Resource{
			URI:      "file:///result.txt",
			MIMEType: "text/plain",
			Text:     "resource text",
		}},
	}})
	if result.IsError || len(result.Content) != 2 {
		t.Fatalf("unexpected result: %+v", result)
	}
	imageSource, ok := result.Content[0].Source.(map[string]any)
	if !ok || imageSource["data"] != "aW1hZ2U=" {
		t.Fatalf("unexpected image source: %#v", result.Content[0].Source)
	}
	documentSource, ok := result.Content[1].Source.(map[string]any)
	if !ok || documentSource["data"] != "resource text" {
		t.Fatalf("unexpected document source: %#v", result.Content[1].Source)
	}
}

func TestConvertTextResourceNormalizesMIMEType(t *testing.T) {
	result := ConvertCallToolResult(&CallToolResult{Content: []Content{{
		Type: "resource",
		Resource: &Resource{
			MIMEType: "text/html",
			Text:     "<p>hello</p>",
		},
	}}})
	if result.IsError || len(result.Content) != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	source, ok := result.Content[0].Source.(map[string]any)
	if !ok || source["media_type"] != "text/plain" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestConvertStructuredAndErrorResults(t *testing.T) {
	structured := ConvertCallToolResult(&CallToolResult{StructuredContent: map[string]any{"status": "ok"}})
	if structured.IsError || len(structured.Content) != 1 || structured.Content[0].Text != `{"status":"ok"}` {
		t.Fatalf("unexpected structured result: %+v", structured)
	}

	failed := ConvertCallToolResult(&CallToolResult{
		Content: []Content{{Type: "text", Text: "not found"}},
		IsError: true,
	})
	if !failed.IsError || len(failed.Content) != 1 || failed.Content[0].Text != "not found" {
		t.Fatalf("unexpected error result: %+v", failed)
	}

	emptyFailure := ConvertCallToolResult(&CallToolResult{IsError: true})
	if !emptyFailure.IsError || len(emptyFailure.Content) != 1 ||
		emptyFailure.Content[0].Text != "mcp tool returned an error but returned no content" {
		t.Fatalf("unexpected empty error result: %+v", emptyFailure)
	}
}

func TestConvertCallToolResultDoesNotExposeResourceURI(t *testing.T) {
	const secretURI = "https://example.com/file?signature=secret"
	result := ConvertCallToolResult(&CallToolResult{Content: []Content{{
		Type: "resource",
		Resource: &Resource{
			URI:      secretURI,
			MIMEType: "image/png",
		},
	}}})
	if !result.IsError || len(result.Content) != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if strings.Contains(result.Content[0].Text, secretURI) || strings.Contains(result.Content[0].Text, "secret") {
		t.Fatalf("resource URI leaked into error: %q", result.Content[0].Text)
	}
}
