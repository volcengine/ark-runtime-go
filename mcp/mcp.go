// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package mcp 将官方 MCP Go SDK 适配到方舟 self-hosted MCP 接口。
package mcp

import (
	"context"
	"errors"
	"fmt"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/agent"
	coremcp "github.com/volcengine/ark-runtime-go/arkruntime/selfhosted/mcp"
	"github.com/volcengine/ark-runtime-go/arkruntime/tools/agenttoolset"
)

// Client 把官方 MCP ClientSession 适配成核心 MCP Client 接口。
type Client struct {
	session *mcpsdk.ClientSession
}

// NewClient 创建官方 MCP ClientSession adapter。
func NewClient(session *mcpsdk.ClientSession) (*Client, error) {
	if session == nil {
		return nil, errors.New("mcp client session is required")
	}
	return &Client{session: session}, nil
}

// CallTool 调用官方 MCP ClientSession。
func (c *Client) CallTool(ctx context.Context, name string, arguments map[string]any) (*coremcp.CallToolResult, error) {
	result, err := c.session.CallTool(ctx, &mcpsdk.CallToolParams{Name: name, Arguments: arguments})
	if err != nil {
		return nil, err
	}
	return callToolResult(result)
}

// NewTool 把 MCP Tool 与保持打开的 MCP ClientSession 包装成 worker Custom Tool。
func NewTool(tool *mcpsdk.Tool, session *mcpsdk.ClientSession) (agenttoolset.Tool, error) {
	client, err := NewClient(session)
	if err != nil {
		return nil, err
	}
	definition, err := toolDefinition(tool)
	if err != nil {
		return nil, err
	}
	return coremcp.NewTool(definition, client)
}

// NewTools 批量包装 MCP Tools，并返回可传给 EnvironmentWorkerOptions.CustomTools 的映射。
func NewTools(tools []*mcpsdk.Tool, session *mcpsdk.ClientSession) (map[string]agenttoolset.Tool, error) {
	client, err := NewClient(session)
	if err != nil {
		return nil, err
	}
	definitions, err := toolDefinitions(tools)
	if err != nil {
		return nil, err
	}
	return coremcp.NewTools(definitions, client)
}

// CustomToolItem 把 MCP Tool 定义转换成创建或更新 Agent 使用的 custom ToolItem。
func CustomToolItem(tool *mcpsdk.Tool) (agent.ToolItem, error) {
	definition, err := toolDefinition(tool)
	if err != nil {
		return agent.ToolItem{}, err
	}
	return coremcp.CustomToolItem(definition)
}

// CustomToolItems 批量生成 Agent custom tool 声明。
func CustomToolItems(tools []*mcpsdk.Tool) ([]agent.ToolItem, error) {
	definitions, err := toolDefinitions(tools)
	if err != nil {
		return nil, err
	}
	return coremcp.CustomToolItems(definitions)
}

func toolDefinitions(tools []*mcpsdk.Tool) ([]coremcp.ToolDefinition, error) {
	definitions := make([]coremcp.ToolDefinition, 0, len(tools))
	for _, tool := range tools {
		definition, err := toolDefinition(tool)
		if err != nil {
			return nil, err
		}
		definitions = append(definitions, definition)
	}
	return definitions, nil
}

func toolDefinition(tool *mcpsdk.Tool) (coremcp.ToolDefinition, error) {
	if tool == nil {
		return coremcp.ToolDefinition{}, errors.New("mcp tool is required")
	}
	return coremcp.ToolDefinition{
		Name:        tool.Name,
		Description: tool.Description,
		InputSchema: tool.InputSchema,
	}, nil
}

func callToolResult(result *mcpsdk.CallToolResult) (*coremcp.CallToolResult, error) {
	if result == nil {
		return nil, nil
	}
	converted := &coremcp.CallToolResult{
		Content:           make([]coremcp.Content, 0, len(result.Content)),
		StructuredContent: result.StructuredContent,
		IsError:           result.IsError,
	}
	for _, content := range result.Content {
		item, err := contentValue(content)
		if err != nil {
			return nil, err
		}
		converted.Content = append(converted.Content, item)
	}
	return converted, nil
}

func contentValue(content mcpsdk.Content) (coremcp.Content, error) {
	switch value := content.(type) {
	case *mcpsdk.TextContent:
		return coremcp.Content{Type: "text", Text: value.Text}, nil
	case *mcpsdk.ImageContent:
		return coremcp.Content{Type: "image", MIMEType: value.MIMEType, Data: value.Data}, nil
	case *mcpsdk.EmbeddedResource:
		return coremcp.Content{Type: "resource", Resource: resourceValue(value.Resource)}, nil
	case *mcpsdk.AudioContent:
		return coremcp.Content{Type: "audio"}, nil
	case *mcpsdk.ResourceLink:
		return coremcp.Content{Type: "resource_link"}, nil
	default:
		return coremcp.Content{}, fmt.Errorf("unsupported MCP content type %T", content)
	}
}

func resourceValue(resource *mcpsdk.ResourceContents) *coremcp.Resource {
	if resource == nil {
		return nil
	}
	return &coremcp.Resource{
		URI:      resource.URI,
		MIMEType: resource.MIMEType,
		Text:     resource.Text,
		Blob:     resource.Blob,
	}
}
