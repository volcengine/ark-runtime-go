// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package toolset

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

const (
	readBlockTypeImage    = "image"
	readBlockTypeDocument = "document"
	readMediaTypePDF      = "application/pdf"
)

// ReadTool 实现 read 工具。
type ReadTool struct {
	resolver *Resolver
	limits   Limits
}

type readRequest struct {
	FilePath  string `json:"file_path"`
	Path      string `json:"path"`
	File      string `json:"file"`
	ViewRange []int  `json:"view_range,omitempty"`
	Offset    *int   `json:"offset,omitempty"`
	Limit     *int   `json:"limit,omitempty"`
}

// NewReadTool 创建 read 工具。
func NewReadTool(resolver *Resolver, limits Limits) *ReadTool {
	return &ReadTool{resolver: resolver, limits: limits}
}

// Name 返回工具名。
func (t *ReadTool) Name() string { return "read" }

// Execute 执行 read。
func (t *ReadTool) Execute(ctx context.Context, input json.RawMessage) Result {
	var req readRequest
	if err := decodeInput(input, &req); err != nil {
		return ErrorResult(err.Error())
	}
	path, err := req.resolvedPath()
	if err != nil {
		return ErrorResult(err.Error())
	}
	if len(req.ViewRange) > 0 && (req.Offset != nil || req.Limit != nil) {
		return ErrorResult("view_range cannot be combined with offset or limit")
	}
	host, err := t.resolver.ResolveExisting(path)
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
	if err := ctx.Err(); err != nil {
		return ErrorResult(err.Error())
	}
	blockType, mediaType, err := detectReadMedia(host)
	if err != nil {
		return ErrorResult(err.Error())
	}
	if blockType != "" {
		if len(req.ViewRange) > 0 || req.Offset != nil || req.Limit != nil {
			return ErrorResult("view_range, offset, and limit are only supported for text files")
		}
		limit := t.limits.MaxMediaFileBytes
		if limit == 0 {
			limit = t.limits.MaxInputFileBytes
		}
		if limit > 0 && info.Size() > limit {
			return ErrorResult(fmt.Sprintf("media file too large: %d bytes", info.Size()))
		}
		reader, err := os.Open(host)
		if err != nil {
			return ErrorResult(err.Error())
		}
		defer func() { _ = reader.Close() }()
		var source io.Reader = reader
		if limit > 0 {
			source = io.LimitReader(reader, limit+1)
		}
		data, err := io.ReadAll(source)
		if err != nil {
			return ErrorResult(err.Error())
		}
		if limit > 0 && int64(len(data)) > limit {
			size := info.Size()
			if int64(len(data)) > size {
				size = int64(len(data))
			}
			return ErrorResult(fmt.Sprintf("media file too large: %d bytes", size))
		}
		if err := ctx.Err(); err != nil {
			return ErrorResult(err.Error())
		}
		return Result{Content: []ContentBlock{{
			Type: blockType,
			Source: map[string]any{
				"type":       "base64",
				"media_type": mediaType,
				"data":       base64.StdEncoding.EncodeToString(data),
			},
		}}}
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
		start := 0
		if req.ViewRange[0] > 1 {
			start = req.ViewRange[0] - 1
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
	lines := strings.Split(string(data), "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" && strings.HasSuffix(string(data), "\n") {
		lines = lines[:len(lines)-1]
	}
	offset := 0
	if req.Offset != nil {
		if *req.Offset < 1 {
			return ErrorResult(fmt.Sprintf("offset is the 1-based start line and must be >= 1, got %d", *req.Offset))
		}
		offset = *req.Offset - 1
	}
	if offset > len(lines) {
		offset = len(lines)
	}
	limit := 0
	if req.Limit != nil {
		limit = *req.Limit
	}
	if limit <= 0 {
		limit = t.limits.ReadDefaultLines
	}
	if limit <= 0 {
		limit = 2000
	}
	end := len(lines)
	if limit < len(lines)-offset {
		end = offset + limit
	}
	var b strings.Builder
	for i := offset; i < end; i++ {
		line := lines[i]
		if t.limits.ReadMaxLineChars > 0 && utf8.RuneCountInString(line) > t.limits.ReadMaxLineChars {
			line = string([]rune(line)[:t.limits.ReadMaxLineChars]) + " [line truncated]"
		}
		fmt.Fprintf(&b, "%6d\t%s\n", i+1, line)
	}
	if end < len(lines) {
		fmt.Fprintf(&b, "\n[truncated: showing lines %d-%d of %d]\n", offset+1, end, len(lines))
	}
	return TextResult(b.String())
}

func (r readRequest) resolvedPath() (string, error) {
	path := ""
	for _, candidate := range []string{r.FilePath, r.Path, r.File} {
		if candidate == "" {
			continue
		}
		if path != "" && path != candidate {
			return "", errors.New("file_path, path, and file must not conflict")
		}
		path = candidate
	}
	if path == "" {
		return "", errors.New("file_path is required")
	}
	return path, nil
}

func detectReadMedia(path string) (string, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = f.Close() }()

	header := make([]byte, 512)
	n, err := io.ReadFull(f, header)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return "", "", err
	}
	header = header[:n]
	detected := http.DetectContentType(header)
	if blockType := supportedReadMediaBlock(detected); blockType != "" {
		return blockType, detected, nil
	}
	if detected != "application/octet-stream" {
		return "", "", nil
	}
	extensionType := mime.TypeByExtension(strings.ToLower(filepath.Ext(path)))
	if semi := strings.IndexByte(extensionType, ';'); semi >= 0 {
		extensionType = extensionType[:semi]
	}
	extensionType = strings.ToLower(strings.TrimSpace(extensionType))
	return supportedReadMediaBlock(extensionType), extensionType, nil
}

func supportedReadMediaBlock(mediaType string) string {
	switch mediaType {
	case "image/jpeg", "image/png", "image/gif", "image/webp":
		return readBlockTypeImage
	case readMediaTypePDF:
		return readBlockTypeDocument
	default:
		return ""
	}
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
