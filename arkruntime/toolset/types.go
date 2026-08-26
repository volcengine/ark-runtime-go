// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
// Package toolset 实现 self-hosted worker 内置工具集合。
package toolset

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// ContentBlock 是工具结果内容块。
type ContentBlock struct {
	Type      string `json:"type"`
	Text      string `json:"text,omitempty"`
	MediaType string `json:"media_type,omitempty"`
	Data      []byte `json:"data,omitempty"`
}

// Result 是一次工具执行结果。
type Result struct {
	Content []ContentBlock `json:"content"`
	IsError bool           `json:"is_error"`
}

// Tool 是 worker 可执行的本地工具。
type Tool interface {
	Name() string
	// Execute 必须监听 ctx.Done；worker 只能取消 context，不能安全终止任意 Go goroutine。
	Execute(ctx context.Context, input json.RawMessage) Result
}

// ClosableTool 是需要释放资源的工具。
type ClosableTool interface {
	Tool
	Close() error
}

// Options 是默认工具集合配置。
type Options struct {
	Workdir           string
	UnrestrictedPaths bool
	// Env 非 nil 时完全替换继承的进程环境；敏感凭证字段始终会被移除。
	Env         map[string]string
	Limits      Limits
	BashPath    string
	ToolTimeout time.Duration
}

// Limits 是工具执行的尺寸与数量上限。
type Limits struct {
	MaxOutputBytes    int64
	MaxInputFileBytes int64
	ReadDefaultLines  int
	ReadMaxLineChars  int
	GlobMaxMatches    int
	GrepMaxMatches    int
}

// DefaultLimits 返回默认工具限制。
func DefaultLimits() Limits {
	return Limits{
		MaxOutputBytes:    100 << 10,
		MaxInputFileBytes: 256 << 10,
		ReadDefaultLines:  2000,
		ReadMaxLineChars:  2000,
		GlobMaxMatches:    1000,
		GrepMaxMatches:    200,
	}
}

// Set 是可执行工具集合。
type Set struct {
	mu    sync.RWMutex
	tools map[string]Tool
}

// NewDefault 创建 bash/read/write/edit/glob/grep 默认工具集合。
func NewDefault(opts Options) (*Set, error) {
	if opts.Limits == (Limits{}) {
		opts.Limits = DefaultLimits()
	}
	if opts.ToolTimeout <= 0 {
		opts.ToolTimeout = 120 * time.Second
	}
	resolver, err := NewResolverWithOptions(opts.Workdir, opts.UnrestrictedPaths)
	if err != nil {
		return nil, err
	}
	bash, err := NewBashTool(opts)
	if err != nil {
		return nil, err
	}
	s := &Set{tools: map[string]Tool{}}
	s.Register(bash)
	s.Register(NewReadTool(resolver, opts.Limits))
	s.Register(NewWriteTool(resolver, opts.Limits))
	s.Register(NewEditTool(resolver, opts.Limits))
	s.Register(NewGlobTool(resolver, opts.Limits))
	s.Register(NewGrepTool(resolver, opts.Limits))
	return s, nil
}

// Register 注册一个工具。
func (s *Set) Register(tool Tool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tools[tool.Name()] = tool
}

// Execute 执行指定工具。
func (s *Set) Execute(ctx context.Context, name string, input json.RawMessage) Result {
	s.mu.RLock()
	tool := s.tools[name]
	s.mu.RUnlock()
	if tool == nil {
		return ErrorResult(fmt.Sprintf("unknown tool: %s", name))
	}
	return tool.Execute(ctx, input)
}

// Has 判断工具集合是否注册了指定工具。
func (s *Set) Has(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tools[name] != nil
}

// Close 关闭工具集合持有的资源。
func (s *Set) Close() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var first error
	for _, tool := range s.tools {
		if c, ok := tool.(ClosableTool); ok {
			if err := c.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}

// TextResult 构造文本工具结果。
func TextResult(text string) Result {
	return Result{Content: []ContentBlock{{Type: "text", Text: text}}}
}

// ErrorResult 构造错误工具结果。
func ErrorResult(message string) Result {
	return Result{
		Content: []ContentBlock{{Type: "text", Text: message}},
		IsError: true,
	}
}

func decodeInput(input json.RawMessage, out any) error {
	if len(input) == 0 {
		input = []byte("{}")
	}
	if err := json.Unmarshal(input, out); err != nil {
		return fmt.Errorf("invalid tool input: %w", err)
	}
	return nil
}
