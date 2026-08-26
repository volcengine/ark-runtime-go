// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
// Package agenttoolset 提供 MA agent 默认内置工具集的公共入口。
package agenttoolset

import (
	"context"
	"time"

	selfhosted "github.com/volcengine/ark-runtime-go/arkruntime/selfhosted"
	"github.com/volcengine/ark-runtime-go/arkruntime/selfhosted/envinit"
	"github.com/volcengine/ark-runtime-go/arkruntime/toolset"
)

type (
	// Tool 是 worker 可执行的本地工具。
	Tool = toolset.Tool
	// Set 是可执行工具集合。
	Set = toolset.Set
	// Result 是一次工具执行结果。
	Result = toolset.Result
	// ContentBlock 是工具结果内容块。
	ContentBlock = toolset.ContentBlock
	// Limits 是工具执行的尺寸与数量上限。
	Limits = toolset.Limits
)

// AgentToolContext 携带单个 session 绑定的本地工具执行上下文。
type AgentToolContext struct {
	Workdir           string
	UnrestrictedPaths bool
	// Env 非 nil 时完全替换继承的进程环境；敏感凭证字段始终会被移除。
	Env               map[string]string
	Limits            Limits
	BashPath          string
	ToolTimeout       time.Duration
	MaxArchiveBytes   int64
	MaxExtractedBytes int64
	MaxArchiveEntries int
}

// AgentToolset20260401 返回 agent_toolset_20260401 对应的内置工具集合。
func AgentToolset20260401(env *AgentToolContext) (*Set, error) {
	if env == nil {
		env = &AgentToolContext{}
	}
	return toolset.NewDefault(env.ToolOptions())
}

// CloseAll 释放工具集合持有的资源。
func CloseAll(tools *Set) {
	if tools != nil {
		_ = tools.Close()
	}
}

// SetupSkills 下载并安装 session 绑定的 skills。
func (e *AgentToolContext) SetupSkills(ctx context.Context, api selfhosted.API, session *selfhosted.Session) error {
	if e == nil {
		e = &AgentToolContext{}
	}
	return envinit.New(api, e.InitOptions()).Setup(ctx, session)
}

// ToolOptions 转换为内部工具集合配置。
func (e *AgentToolContext) ToolOptions() toolset.Options {
	if e == nil {
		return toolset.Options{}
	}
	return toolset.Options{
		Workdir:           e.Workdir,
		UnrestrictedPaths: e.UnrestrictedPaths,
		Env:               e.Env,
		Limits:            e.Limits,
		BashPath:          e.BashPath,
		ToolTimeout:       e.ToolTimeout,
	}
}

// InitOptions 转换为内部 session 初始化配置。
func (e *AgentToolContext) InitOptions() envinit.Options {
	if e == nil {
		return envinit.Options{}
	}
	return envinit.Options{
		Workdir:           e.Workdir,
		MaxArchiveBytes:   e.MaxArchiveBytes,
		MaxExtractedBytes: e.MaxExtractedBytes,
		MaxArchiveEntries: e.MaxArchiveEntries,
	}
}
