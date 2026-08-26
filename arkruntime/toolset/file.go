// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package toolset

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

// ReadTool 实现 read 工具。
type ReadTool struct {
	resolver *Resolver
	limits   Limits
}

// NewReadTool 创建 read 工具。
func NewReadTool(resolver *Resolver, limits Limits) *ReadTool {
	return &ReadTool{resolver: resolver, limits: limits}
}

// Name 返回工具名。
func (t *ReadTool) Name() string { return "read" }

// Execute 执行 read。
func (t *ReadTool) Execute(_ context.Context, input json.RawMessage) Result {
	var req struct {
		FilePath  string `json:"file_path"`
		ViewRange []int  `json:"view_range,omitempty"`
		Offset    int    `json:"offset,omitempty"`
		Limit     int    `json:"limit,omitempty"`
	}
	if err := decodeInput(input, &req); err != nil {
		return ErrorResult(err.Error())
	}
	host, err := t.resolver.ResolveExisting(req.FilePath)
	if err != nil {
		return ErrorResult(err.Error())
	}
	info, err := os.Stat(host)
	if err != nil {
		return ErrorResult(err.Error())
	}
	if !info.Mode().IsRegular() {
		return ErrorResult("path is not a regular file")
	}
	if t.limits.MaxInputFileBytes > 0 && info.Size() > t.limits.MaxInputFileBytes {
		return ErrorResult(fmt.Sprintf("file too large: %d bytes", info.Size()))
	}
	data, err := os.ReadFile(host)
	if err != nil {
		return ErrorResult(err.Error())
	}
	if !utf8.Valid(data) {
		return ErrorResult("binary file cannot be read directly")
	}
	if len(req.ViewRange) > 0 {
		if len(req.ViewRange) != 2 {
			return ErrorResult("view_range must be [start_line, end_line]")
		}
		lines := strings.Split(string(data), "\n")
		start := req.ViewRange[0] - 1
		if start < 0 {
			start = 0
		}
		if start >= len(lines) {
			return TextResult("")
		}
		end := len(lines)
		if req.ViewRange[1] > 0 && req.ViewRange[1] < end {
			end = req.ViewRange[1]
		}
		if end < start {
			return ErrorResult(fmt.Sprintf("view_range end line %d is before start line %d", req.ViewRange[1], req.ViewRange[0]))
		}
		return TextResult(strings.Join(lines[start:end], "\n"))
	}
	if req.Offset == 0 && req.Limit == 0 {
		return TextResult(string(data))
	}
	lines := strings.Split(string(data), "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	offset := req.Offset
	if offset < 0 {
		offset = 0
	}
	if offset > len(lines) {
		offset = len(lines)
	}
	limit := req.Limit
	if limit <= 0 {
		limit = t.limits.ReadDefaultLines
	}
	if limit <= 0 {
		limit = 2000
	}
	end := offset + limit
	if end > len(lines) {
		end = len(lines)
	}
	var b strings.Builder
	for i := offset; i < end; i++ {
		line := lines[i]
		if t.limits.ReadMaxLineChars > 0 && utf8.RuneCountInString(line) > t.limits.ReadMaxLineChars {
			line = string([]rune(line)[:t.limits.ReadMaxLineChars])
		}
		fmt.Fprintf(&b, "%6d\t%s\n", i+1, line)
	}
	if end < len(lines) {
		fmt.Fprintf(&b, "\n[truncated: showing lines %d-%d of %d]\n", offset+1, end, len(lines))
	}
	return TextResult(b.String())
}

// WriteTool 实现 write 工具。
type WriteTool struct {
	resolver *Resolver
	limits   Limits
}

// NewWriteTool 创建 write 工具。
func NewWriteTool(resolver *Resolver, limits Limits) *WriteTool {
	return &WriteTool{resolver: resolver, limits: limits}
}

// Name 返回工具名。
func (t *WriteTool) Name() string { return "write" }

// Execute 执行 write。
func (t *WriteTool) Execute(_ context.Context, input json.RawMessage) Result {
	var req struct {
		FilePath string `json:"file_path"`
		Content  string `json:"content"`
	}
	if err := decodeInput(input, &req); err != nil {
		return ErrorResult(err.Error())
	}
	if t.limits.MaxInputFileBytes > 0 && int64(len(req.Content)) > t.limits.MaxInputFileBytes {
		return ErrorResult(fmt.Sprintf("content too large: %d bytes", len(req.Content)))
	}
	host, err := t.resolver.ResolveForWrite(req.FilePath)
	if err != nil {
		return ErrorResult(err.Error())
	}
	if err := os.MkdirAll(filepath.Dir(host), 0o755); err != nil {
		return ErrorResult(err.Error())
	}
	verified, err := t.resolver.ResolveForWrite(req.FilePath)
	if err != nil {
		return ErrorResult(err.Error())
	}
	if verified != host {
		return ErrorResult("path resolution changed while writing")
	}
	if err := writeFileAtomically(host, []byte(req.Content), 0o600); err != nil {
		return ErrorResult(err.Error())
	}
	return TextResult(fmt.Sprintf("wrote %d bytes to %s", len(req.Content), req.FilePath))
}

// EditTool 实现 edit 工具。
type EditTool struct {
	resolver *Resolver
	limits   Limits
}

// NewEditTool 创建 edit 工具。
func NewEditTool(resolver *Resolver, limits Limits) *EditTool {
	return &EditTool{resolver: resolver, limits: limits}
}

// Name 返回工具名。
func (t *EditTool) Name() string { return "edit" }

// Execute 执行 edit。
func (t *EditTool) Execute(_ context.Context, input json.RawMessage) Result {
	var req struct {
		FilePath   string `json:"file_path"`
		OldString  string `json:"old_string"`
		NewString  string `json:"new_string"`
		ReplaceAll bool   `json:"replace_all,omitempty"`
	}
	if err := decodeInput(input, &req); err != nil {
		return ErrorResult(err.Error())
	}
	if req.OldString == "" {
		return ErrorResult("old_string must not be empty")
	}
	host, err := t.resolver.ResolveExisting(req.FilePath)
	if err != nil {
		return ErrorResult(err.Error())
	}
	writable, err := t.resolver.ResolveForWrite(req.FilePath)
	if err != nil {
		return ErrorResult(err.Error())
	}
	if writable != host {
		return ErrorResult("path resolution changed while editing")
	}
	info, err := os.Stat(host)
	if err != nil {
		return ErrorResult(err.Error())
	}
	if !info.Mode().IsRegular() {
		return ErrorResult("path is not a regular file")
	}
	if t.limits.MaxInputFileBytes > 0 && info.Size() > t.limits.MaxInputFileBytes {
		return ErrorResult(fmt.Sprintf("file too large: %d bytes", info.Size()))
	}
	data, err := os.ReadFile(host)
	if err != nil {
		return ErrorResult(err.Error())
	}
	content := string(data)
	count := strings.Count(content, req.OldString)
	switch {
	case count == 0:
		return ErrorResult("old_string not found")
	case count > 1 && !req.ReplaceAll:
		return ErrorResult(fmt.Sprintf("old_string is not unique: %d matches", count))
	}
	n := 1
	if req.ReplaceAll {
		n = -1
	}
	next := strings.Replace(content, req.OldString, req.NewString, n)
	if int64(len(next)) > t.limits.MaxInputFileBytes && t.limits.MaxInputFileBytes > 0 {
		return ErrorResult(fmt.Sprintf("edited file too large: %d bytes", len(next)))
	}
	verified, err := t.resolver.ResolveForWrite(req.FilePath)
	if err != nil {
		return ErrorResult(err.Error())
	}
	if verified != host {
		return ErrorResult("path resolution changed while editing")
	}
	if err := writeFileAtomically(host, []byte(next), info.Mode().Perm()); err != nil {
		return ErrorResult(err.Error())
	}
	return TextResult(fmt.Sprintf("replaced %d occurrence(s) in %s", count, req.FilePath))
}

func writeFileAtomically(target string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(target), ".ark-write-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, target)
}
