// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package mcp 提供与具体 MCP SDK 无关的 self-hosted MCP 工具适配能力。
package mcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/agent"
	"github.com/volcengine/ark-runtime-go/arkruntime/tools/agenttoolset"
)

var supportedImageMIMETypes = map[string]bool{
	"image/gif":  true,
	"image/jpeg": true,
	"image/png":  true,
	"image/webp": true,
}

const (
	compatibilityDescriptionPrefix = "\n\nMCP input constraints (JSON Schema): "
	maxCustomToolDescriptionRunes  = 10_000
	objectSchemaType               = "object"
)

var ignoredTopLevelSchemaKeywords = map[string]bool{
	"$anchor":        true,
	"$comment":       true,
	"$dynamicAnchor": true,
	"$id":            true,
	"$schema":        true,
	"title":          true,
}

// Client 是 self-hosted worker 调用 MCP Server 所需的最小接口。
type Client interface {
	CallTool(ctx context.Context, name string, arguments map[string]any) (*CallToolResult, error)
}

// ToolDefinition 描述一个 MCP Tool 及其输入 JSON Schema。
type ToolDefinition struct {
	Name        string
	Description string
	InputSchema any
}

// CallToolResult 是 MCP Tool 调用结果的协议无关表示。
type CallToolResult struct {
	Content           []Content
	StructuredContent any
	IsError           bool
}

// Content 是 MCP Tool 返回的单个内容块。
type Content struct {
	Type     string
	Text     string
	MIMEType string
	Data     []byte
	Resource *Resource
}

// Resource 是 MCP embedded resource 的协议无关表示。
type Resource struct {
	URI      string
	MIMEType string
	Text     string
	Blob     []byte
}

// NewTool 把 MCP Tool 定义与 Client 包装成 worker Custom Tool。
func NewTool(tool ToolDefinition, client Client) (agenttoolset.Tool, error) {
	if client == nil {
		return nil, errors.New("mcp client is required")
	}
	if tool.Name == "" {
		return nil, errors.New("mcp tool name is required")
	}
	return &runnableTool{tool: tool, client: client}, nil
}

// NewTools 批量包装 MCP Tools。
func NewTools(tools []ToolDefinition, client Client) (map[string]agenttoolset.Tool, error) {
	out := make(map[string]agenttoolset.Tool, len(tools))
	for _, tool := range tools {
		wrapped, err := NewTool(tool, client)
		if err != nil {
			return nil, err
		}
		if _, exists := out[wrapped.Name()]; exists {
			return nil, fmt.Errorf("duplicate mcp tool name %q", wrapped.Name())
		}
		out[wrapped.Name()] = wrapped
	}
	return out, nil
}

// CustomToolItem 把 MCP Tool 定义转换成创建或更新 Agent 使用的 custom ToolItem。
func CustomToolItem(tool ToolDefinition) (agent.ToolItem, error) {
	if tool.Name == "" {
		return agent.ToolItem{}, errors.New("mcp tool name is required")
	}
	schema, constraints, err := customToolInputSchema(tool.InputSchema)
	if err != nil {
		return agent.ToolItem{}, fmt.Errorf("mcp tool %s input schema: %w", tool.Name, err)
	}
	description := tool.Description
	if description == "" {
		description = tool.Name
	}
	if constraints != "" {
		description += compatibilityDescriptionPrefix + constraints
	}
	if utf8.RuneCountInString(description) > maxCustomToolDescriptionRunes {
		return agent.ToolItem{}, fmt.Errorf(
			"mcp tool %s description exceeds %d characters after adding input constraints",
			tool.Name,
			maxCustomToolDescriptionRunes,
		)
	}
	return agent.ToolItem{
		Type:        "custom",
		Name:        agent.NewOptString(tool.Name),
		Description: agent.NewOptString(description),
		InputSchema: agent.NewOptCustomToolInputSchema(schema),
	}, nil
}

// CustomToolItems 批量生成 Agent custom tool 声明。
func CustomToolItems(tools []ToolDefinition) ([]agent.ToolItem, error) {
	out := make([]agent.ToolItem, 0, len(tools))
	seen := make(map[string]struct{}, len(tools))
	for _, tool := range tools {
		item, err := CustomToolItem(tool)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[tool.Name]; exists {
			return nil, fmt.Errorf("duplicate mcp tool name %q", tool.Name)
		}
		seen[tool.Name] = struct{}{}
		out = append(out, item)
	}
	return out, nil
}

type runnableTool struct {
	tool   ToolDefinition
	client Client
}

func (t *runnableTool) Name() string { return t.tool.Name }

func (t *runnableTool) Execute(ctx context.Context, input json.RawMessage) agenttoolset.Result {
	if len(input) == 0 {
		input = json.RawMessage("{}")
	}
	var arguments map[string]any
	if err := json.Unmarshal(input, &arguments); err != nil {
		return errorResult(fmt.Sprintf("mcp tool %s: invalid input: %v", t.tool.Name, err))
	}
	result, err := t.client.CallTool(ctx, t.tool.Name, arguments)
	if err != nil {
		return errorResult(fmt.Sprintf("mcp tool %s: %v", t.tool.Name, err))
	}
	return convertCallToolResult(result, t.tool.Name)
}

func customToolInputSchema(value any) (agent.CustomToolInputSchema, string, error) {
	if value == nil {
		value = map[string]any{"type": objectSchemaType}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return agent.CustomToolInputSchema{}, "", err
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		return agent.CustomToolInputSchema{}, "", errors.New("schema must be an object")
	}
	if object == nil {
		object = make(map[string]any)
	}

	typeName := objectSchemaType
	if rawType, ok := object["type"]; ok {
		if rawType == nil {
			typeName = objectSchemaType
		} else if parsedType, valid := rawType.(string); valid {
			typeName = parsedType
		} else {
			return agent.CustomToolInputSchema{}, "", errors.New("type must be a string")
		}
	}
	if typeName != objectSchemaType {
		return agent.CustomToolInputSchema{}, "", errors.New("top-level type must be \"object\"")
	}

	var properties map[string]any
	if rawProperties, ok := object["properties"]; ok {
		var valid bool
		properties, valid = rawProperties.(map[string]any)
		if !valid {
			return agent.CustomToolInputSchema{}, "", errors.New("properties must be an object")
		}
	}
	required, err := requiredNames(object["required"])
	if err != nil {
		return agent.CustomToolInputSchema{}, "", err
	}

	constraints := unsupportedTopLevelConstraints(object)
	resolvedProperties, unresolved := resolveLocalReferences(properties, object)
	if unresolved {
		constraints["properties"] = properties
	}
	removeUnreferencedDefinitions(constraints)
	constraintJSON, err := json.Marshal(constraints)
	if err != nil {
		return agent.CustomToolInputSchema{}, "", err
	}
	if len(constraints) == 0 {
		constraintJSON = nil
	}

	schema := agent.CustomToolInputSchema{
		Type:     agent.NewOptString(objectSchemaType),
		Required: required,
	}
	if resolvedProperties != nil {
		converted := make(agent.CustomToolInputSchemaProperties, len(resolvedProperties))
		for name, property := range resolvedProperties {
			rawProperty, marshalErr := json.Marshal(property)
			if marshalErr != nil {
				return agent.CustomToolInputSchema{}, "", marshalErr
			}
			converted[name] = rawProperty
		}
		schema.Properties = agent.NewOptCustomToolInputSchemaProperties(converted)
	}
	return schema, string(constraintJSON), nil
}

func requiredNames(value any) ([]string, error) {
	if value == nil {
		return nil, nil
	}
	values, ok := value.([]any)
	if !ok {
		return nil, errors.New("required must be an array of strings")
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		name, ok := value.(string)
		if !ok {
			return nil, errors.New("required must be an array of strings")
		}
		result = append(result, name)
	}
	return result, nil
}

func unsupportedTopLevelConstraints(schema map[string]any) map[string]any {
	constraints := make(map[string]any)
	for key, value := range schema {
		if key == "type" || key == "properties" || key == "required" || ignoredTopLevelSchemaKeywords[key] {
			continue
		}
		constraints[key] = value
	}
	return constraints
}

func removeUnreferencedDefinitions(constraints map[string]any) {
	definitionReferences := map[string]string{
		"$defs":       "#/$defs",
		"definitions": "#/definitions",
	}
	for definitionKey, referencePrefix := range definitionReferences {
		if _, exists := constraints[definitionKey]; !exists {
			continue
		}
		referenced := false
		for key, value := range constraints {
			if key != definitionKey && containsReference(value, referencePrefix) {
				referenced = true
				break
			}
		}
		if !referenced {
			delete(constraints, definitionKey)
		}
	}
}

func containsReference(value any, prefix string) bool {
	switch typed := value.(type) {
	case map[string]any:
		if reference, ok := typed["$ref"].(string); ok &&
			(reference == prefix || strings.HasPrefix(reference, prefix+"/")) {
			return true
		}
		for _, item := range typed {
			if containsReference(item, prefix) {
				return true
			}
		}
	case []any:
		for _, item := range typed {
			if containsReference(item, prefix) {
				return true
			}
		}
	}
	return false
}

func resolveLocalReferences(properties map[string]any, root map[string]any) (map[string]any, bool) {
	if properties == nil {
		return nil, false
	}
	resolved, unresolved := resolveSchemaValue(properties, root, make(map[string]bool))
	return resolved.(map[string]any), unresolved
}

func resolveSchemaValue(value any, root map[string]any, resolving map[string]bool) (any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		if reference, ok := typed["$ref"].(string); ok {
			target, found := resolveJSONPointer(root, reference)
			if found && !resolving[reference] {
				resolving[reference] = true
				resolvedTarget, unresolved := resolveSchemaValue(target, root, resolving)
				delete(resolving, reference)
				if targetMap, valid := resolvedTarget.(map[string]any); valid {
					merged := make(map[string]any, len(targetMap)+len(typed)-1)
					for key, item := range targetMap {
						merged[key] = item
					}
					for key, item := range typed {
						if key != "$ref" {
							merged[key] = item
						}
					}
					resolved, mergedUnresolved := resolveSchemaValue(merged, root, resolving)
					return resolved, unresolved || mergedUnresolved
				}
				return mapWithoutReference(typed, root, resolving, true)
			}
			return mapWithoutReference(typed, root, resolving, true)
		}
		out := make(map[string]any, len(typed))
		unresolved := false
		for key, item := range typed {
			resolved, itemUnresolved := resolveSchemaValue(item, root, resolving)
			out[key] = resolved
			unresolved = unresolved || itemUnresolved
		}
		return out, unresolved
	case []any:
		out := make([]any, len(typed))
		unresolved := false
		for i, item := range typed {
			resolved, itemUnresolved := resolveSchemaValue(item, root, resolving)
			out[i] = resolved
			unresolved = unresolved || itemUnresolved
		}
		return out, unresolved
	default:
		return value, false
	}
}

func mapWithoutReference(value map[string]any, root map[string]any, resolving map[string]bool, unresolved bool) (any, bool) {
	out := make(map[string]any, len(value)-1)
	for key, item := range value {
		if key == "$ref" {
			continue
		}
		resolved, itemUnresolved := resolveSchemaValue(item, root, resolving)
		out[key] = resolved
		unresolved = unresolved || itemUnresolved
	}
	return out, unresolved
}

func resolveJSONPointer(root map[string]any, reference string) (any, bool) {
	if !strings.HasPrefix(reference, "#/") {
		return nil, false
	}
	var current any = root
	for _, pointerPart := range strings.Split(strings.TrimPrefix(reference, "#/"), "/") {
		pointerPart = strings.ReplaceAll(strings.ReplaceAll(pointerPart, "~1", "/"), "~0", "~")
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[pointerPart]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

// ConvertCallToolResult 把协议无关的 MCP 结果转换成 worker Tool Result。
func ConvertCallToolResult(result *CallToolResult) agenttoolset.Result {
	return convertCallToolResult(result, "")
}

func convertCallToolResult(result *CallToolResult, toolName string) agenttoolset.Result {
	if result == nil {
		return errorResult("mcp tool returned no result")
	}
	blocks := make([]agenttoolset.ContentBlock, 0, len(result.Content))
	for _, content := range result.Content {
		block, err := contentBlock(content)
		if err != nil {
			return errorResult(err.Error())
		}
		blocks = append(blocks, block)
	}
	if len(blocks) == 0 && result.StructuredContent != nil {
		raw, err := json.Marshal(result.StructuredContent)
		if err != nil {
			return errorResult(fmt.Sprintf("serialize mcp structured content: %v", err))
		}
		blocks = append(blocks, agenttoolset.ContentBlock{Type: "text", Text: string(raw)})
	}
	if len(blocks) == 0 && result.IsError {
		message := "mcp tool returned an error but returned no content"
		if toolName != "" {
			message = fmt.Sprintf("mcp tool %q reported an error but returned no content", toolName)
		}
		blocks = append(blocks, agenttoolset.ContentBlock{Type: "text", Text: message})
	}
	return agenttoolset.Result{Content: blocks, IsError: result.IsError}
}

func contentBlock(content Content) (agenttoolset.ContentBlock, error) {
	switch content.Type {
	case "text":
		return agenttoolset.ContentBlock{Type: "text", Text: content.Text}, nil
	case "image":
		if !supportedImageMIMETypes[content.MIMEType] {
			return agenttoolset.ContentBlock{}, fmt.Errorf("unsupported image MIME type %q", content.MIMEType)
		}
		return base64Block("image", content.MIMEType, content.Data), nil
	case "resource":
		return resourceBlock(content.Resource)
	case "audio", "resource_link":
		return agenttoolset.ContentBlock{}, fmt.Errorf("unsupported MCP content type %s", content.Type)
	default:
		return agenttoolset.ContentBlock{}, fmt.Errorf("unsupported MCP content type %s", content.Type)
	}
}

func resourceBlock(resource *Resource) (agenttoolset.ContentBlock, error) {
	if resource == nil {
		return agenttoolset.ContentBlock{}, errors.New("embedded MCP resource has no content")
	}
	if supportedImageMIMETypes[resource.MIMEType] {
		if resource.Blob == nil {
			return agenttoolset.ContentBlock{}, errors.New("image resource must contain blob data")
		}
		return base64Block("image", resource.MIMEType, resource.Blob), nil
	}
	if resource.MIMEType == "application/pdf" {
		if resource.Blob == nil {
			return agenttoolset.ContentBlock{}, errors.New("PDF resource must contain blob data")
		}
		return base64Block("document", resource.MIMEType, resource.Blob), nil
	}
	if resource.MIMEType == "" || strings.HasPrefix(resource.MIMEType, "text/") {
		text := resource.Text
		if resource.Blob != nil {
			// Text blobs are interpreted as UTF-8, matching the other SDK adapters.
			text = string(resource.Blob)
		}
		return agenttoolset.ContentBlock{
			Type: "document",
			Source: map[string]any{
				"type":       "text",
				"media_type": "text/plain",
				"data":       text,
			},
		}, nil
	}
	return agenttoolset.ContentBlock{}, fmt.Errorf("unsupported resource MIME type %q", resource.MIMEType)
}

func base64Block(blockType, mimeType string, data []byte) agenttoolset.ContentBlock {
	return agenttoolset.ContentBlock{
		Type: blockType,
		Source: map[string]any{
			"type":       "base64",
			"media_type": mimeType,
			"data":       base64.StdEncoding.EncodeToString(data),
		},
	}
}

func errorResult(message string) agenttoolset.Result {
	return agenttoolset.Result{
		Content: []agenttoolset.ContentBlock{{Type: "text", Text: message}},
		IsError: true,
	}
}
