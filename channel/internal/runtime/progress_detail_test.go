// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestProgressProjectsToolDetails(t *testing.T) {
	j := displayJob()
	j.binding.ProgressProtocol = "native_cot"
	steps := progressFromEvents([]Event{
		{ID: "input", Type: "user.message"},
		{ID: "tool", Type: "agent.tool_use", ToolName: "bash", ToolInput: `{"command":"printf 'hello\\n'","api_key":"private-key-value"}`},
		{ID: "result", Type: "agent.tool_result", ToolUseID: "tool", Content: []TextBlock{{Type: "text", Text: "hello\n"}, {Type: "image", Text: "image-private"}, {Type: "text", Text: "API_TOKEN=private-token-value"}}},
	}, j)
	require.Len(t, steps, 1)
	raw, err := json.Marshal(steps[0])
	require.NoError(t, err)
	require.Contains(t, steps[0].Arguments, "printf", "工具参数应进入明细")
	require.Contains(t, steps[0].Output, "hello\n", "工具结果正文应进入明细")
	for _, private := range []string{"private-key-value", "private-token-value", "image-private"} {
		require.NotContains(t, string(raw), private)
	}
}

func TestProgressDetailsMatchToolsAndPreserveBoundaries(t *testing.T) {
	j := displayJob()
	j.binding.ProgressProtocol = "native_cot"
	start := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	finish := start.Add(1831 * time.Millisecond)
	steps := progressFromEvents([]Event{
		{ID: "old", Type: "agent.tool_use", ToolInput: "old-private"},
		{ID: "input", Type: "user.message"},
		{ID: "tool", Type: "agent.tool_use", ToolName: "bash", ToolInput: `{"command":"ls"}`, ProcessedAt: &start},
		{ID: "mcp", Type: "agent.mcp_tool_use", ToolName: "search", MCPServerName: "docs", ToolInput: `{"query":"SDK"}`},
		{ID: "child", Type: "agent.tool_use", SessionThreadID: "sthr-child", ToolInput: "child-private"},
		{ID: "child-result", Type: "agent.tool_result", SessionThreadID: "sthr-child", ToolUseID: "tool", Content: []TextBlock{{Type: "text", Text: "child-private"}}},
		{ID: "mcp-result", Type: "agent.mcp_tool_result", ToolUseID: "mcp", IsError: true, Content: []TextBlock{{Type: "text", Text: "not found"}}},
		{ID: "tool-result", Type: "agent.tool_result", ToolUseID: "tool", ProcessedAt: &finish, Content: []TextBlock{{Type: "text", Text: "file.md\n"}}},
		{ID: "idle", Type: "session.status_idle", StopReason: "end_turn"},
		{ID: "later-result", Type: "agent.tool_result", ToolUseID: "tool", Content: []TextBlock{{Type: "text", Text: "later-private"}}},
	}, j)
	require.Equal(t, []ProgressStep{
		{EventID: "tool", Kind: "tool", Name: "bash", Arguments: `{"command":"ls"}`, Output: "file.md\n", Done: true, DurationMS: 1831},
		{EventID: "mcp", Kind: "mcp_tool", Name: "docs/search", Arguments: `{"query":"SDK"}`, Output: "not found", Done: true, Failed: true},
	}, steps)
}

func TestToolDetailRedactsCredentials(t *testing.T) {
	// Build the synthetic PEM at runtime so source scans never see a key block.
	privateKeyBody := []byte("redaction-test-only")
	privateKeyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: privateKeyBody}))
	for _, tc := range []struct {
		name, input string
		private     []string
	}{
		{name: "nested JSON", input: `{"command":"ls","env":{"API_KEY":"sk-private-value"},"headers":{"Authorization":"Bearer private-token"},"password":"private-password","count":1234567890123456789}`, private: []string{"sk-private-value", "private-token", "private-password"}},
		{name: "shell and flags", input: `API_TOKEN='private-token-value' curl -H 'Authorization: Bearer private-bearer' https://user:private-pass@example.com --password "private flag value"`, private: []string{"private-token-value", "private-bearer", "private-pass", "private flag value"}},
		{name: "private key", input: privateKeyPEM, private: []string{base64.StdEncoding.EncodeToString(privateKeyBody)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &toolDetailRedactor{}
			preview := r.preview(tc.input)
			require.Contains(t, preview, "[REDACTED]")
			for _, private := range tc.private {
				require.NotContains(t, preview, private)
			}
			if json.Valid([]byte(tc.input)) {
				require.True(t, json.Valid([]byte(preview)))
				require.Contains(t, preview, "1234567890123456789", "不丢失参数中的整数精度")
			}
		})
	}
}

func TestToolDetailRedactsCredentialEchoAndBoundsText(t *testing.T) {
	r := &toolDetailRedactor{}
	r.preview(`{"env":{"API_TOKEN":"echo-private-value"}}`)
	require.Equal(t, "echo: [REDACTED]\n", r.preview("echo: echo-private-value\n"))
	preview := r.preview("\x1b[31m正文\x1b[0m\x00\r\n\t" + strings.Repeat("中文\n", maxToolDetailBytes))
	require.True(t, utf8.ValidString(preview))
	require.LessOrEqual(t, len(preview), maxToolDetailBytes)
	require.True(t, strings.HasPrefix(preview, "正文\n\t中文\n"))
	require.Contains(t, preview, "已截断")
}

func TestToolDetailPreservesJSONCommandStructure(t *testing.T) {
	r := &toolDetailRedactor{}
	preview := r.preview(`{"command":"API_TOKEN=private-token","env":{"API_KEY":"private-key"},"echo":"private-key","untouched":null}`)
	require.True(t, json.Valid([]byte(preview)), "脱敏不能破坏参数 JSON 的引号与结构")
	require.NotContains(t, preview, "private-token")
	require.NotContains(t, preview, "private-key")
	var args map[string]any
	require.NoError(t, json.Unmarshal([]byte(preview), &args))
	require.Equal(t, "API_TOKEN=[REDACTED]", args["command"])
	require.Equal(t, "[REDACTED]", args["echo"])
	require.Contains(t, args, "untouched")
}
