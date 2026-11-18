// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Session 表示公开 MA API 的领域投影；适配器负责解析嵌套 agent 与 tags。
type Session struct {
	ID            string `json:"id"`
	Status        string `json:"status,omitempty"`
	AgentID       string `json:"agent_id,omitempty"`
	AgentVersion  string `json:"agent_version,omitempty"`
	ModelEndpoint string `json:"model_endpoint,omitempty"`
	EnvironmentID string `json:"environment_id,omitempty"`
	// EnvironmentType 来自公开 Session 响应的 environment.config.type 冻结快照。
	EnvironmentType string            `json:"environment_type,omitempty"`
	VaultIDs        []string          `json:"vault_ids,omitempty"`
	Tags            map[string]string `json:"tags,omitempty"`
}

// TextBlock 是事件内容中的文本块。
type TextBlock struct {
	Type string `json:"type,omitempty"`
	Text string `json:"text,omitempty"`
}

// Event 是公开 MA API 的领域事件快照。
type Event struct {
	ID              string      `json:"id"`
	Type            string      `json:"type"`
	SessionThreadID string      `json:"session_thread_id,omitempty"`
	Content         []TextBlock `json:"content,omitempty"`
	StopReason      string      `json:"stop_reason,omitempty"`
	RetryStatus     string      `json:"retry_status,omitempty"`
	// ToolName、MCPServerName 来自 *tool_use 事件；ToolUseID、IsError 来自 *tool_result 事件。
	ToolName      string `json:"name,omitempty"`
	MCPServerName string `json:"mcp_server_name,omitempty"`
	ToolUseID     string `json:"tool_use_id,omitempty"`
	IsError       bool   `json:"is_error,omitempty"`
	// Permission 是 agent.tool_use / agent.mcp_tool_use 的 evaluated_permission（allow/ask/deny）。
	Permission string `json:"evaluated_permission,omitempty"`
	// ActionEventIDs 是 requires_action 停顿时 stop_reason.event_ids 列出的待处理事件。
	ActionEventIDs []string `json:"action_event_ids,omitempty"`
	// ToolInput 是工具调用参数原文（raw JSON），仅用于审批摘要和脱敏后的过程明细，不进日志。
	ToolInput string          `json:"input,omitempty"`
	Raw       json.RawMessage `json:"-"`
	Preview   *EventPreview   `json:"preview,omitempty"`
	// ProcessedAt 用于可证明的步骤耗时，缺失时不推测。
	ProcessedAt *time.Time `json:"processed_at,omitempty"`
}

// EventPreview 保存 SSE 预览的有类型片段；它不进入历史或结果判定。
type EventPreview struct {
	// Live 由只直播、不重放的主线程订阅适配器证明，不能从 JSON 输入赋值。
	Live      bool   `json:"-"`
	EventID   string `json:"event_id"`
	EventType string `json:"event_type,omitempty"`
	Index     int    `json:"index,omitempty"`
	Text      string `json:"text,omitempty"`
}

// PreviewStreamer 是可选的主线程文本/思考增量端口，供过程预览和无进展检测使用。
// 未实现时仍按历史扫描执行。
// 实现可从专用主线程订阅范围证明归属；预览和流结束都不能替代 history 的最终判定。
type PreviewStreamer interface {
	StreamEventPreviews(context.Context, string) (EventStream, error)
}

// CreateSessionSpec 保存创建会话之前已持久化的调用意图。
type CreateSessionSpec struct {
	// UserCredential 只持有引用，nil 保持历史快照哈希。
	UserCredential *UserCredentialSpec `json:"UserCredential,omitempty"`
	Binding        Binding
	CreationKey    string
	LookupKey      string
	// Extras 是 BuildSessionSpec Hook 追加的 Vault / Memory / 标签 / env，已通过保留键校验。
	Extras SessionExtras
	// EnvironmentOverride 非空时以 environment_with_overrides 替代 environment_id：
	// 内含合并后的 config.env（base env + LARKSUITE_CLI_*）。与 environment_id 互斥。
	EnvironmentOverride *EnvironmentOverride
	// EnvironmentType 是冻结时读取到的 base Environment 配置类型（cloud/self_hosted）。
	// EnvironmentOverride 为空时也会冻结：self_hosted 等不支持文件挂载的环境在没有
	// 身份 env、不走 override 创建时，挂载阶段仍需据此跳过并退回说明文本。
	EnvironmentType string
	// ChatType 是创建该 Session 的会话类型（group/topic/p2p），供上层按快照身份计算沙箱 env。
	ChatType string
}

// SessionPage 是一次完整分页扫描中的一页会话。
type SessionPage struct {
	Sessions []Session
	NextPage string
}

// EventQuery 描述一次事件分页读取。
//
// CreatedAtGte 非空时只读取服务端写入时间不早于该时刻的事件，用于把对账限定在最近一次
// 远程写附近；Types 为空表示不过滤；Limit 为 0 时由适配器使用默认页大小。结果总是按
// 服务端事件顺序升序返回。
type EventQuery struct {
	Page         string
	Limit        int
	Types        []string
	CreatedAtGte *time.Time
}

// EventPage 是一次分页读取中的一页事件。
type EventPage struct {
	Events   []Event
	NextPage string
}

// ErrEventTooLarge 表示缩页到单条后仍无法读取完整事件，继续重试不能恢复。
var ErrEventTooLarge = errors.New("ma event exceeds read limit")

// SentResponse 保存公开 SDK 发送输入后的同步事件响应。
type SentResponse struct {
	Events []Event
}

// UserInput 是一条 user.message 的完整内容：可选的背景块、用户正文与附件块，按此顺序提交。
type UserInput struct {
	// Context 是只在新 Session 首条消息注入一次的聊天背景，提交时标注为非指令。
	Context string
	Text    string
	Blocks  []InputBlock
	// Mounts 是本轮附件在当前 Session 的挂载结果，顺序与 Blocks 无关，按 FileID 索引。
	Mounts []MountedFile
}

// ExpectedText 返回发送时文本块按顺序拼接的期望值；不包含平台把附件本体降级后新增的文本。
// notice 块以文本提交，因此按附件顺序计入。
func (input UserInput) ExpectedText() string {
	text := input.Context + input.Text
	for _, block := range input.Blocks {
		text += input.AttachmentText(block)
	}
	return text
}

// AttachmentText 统一生成附件的说明、真实挂载路径或下载指引，供发送与回显校验共用。
func (input UserInput) AttachmentText(block InputBlock) string {
	for _, mounted := range input.Mounts {
		if block.FileID == "" || mounted.FileID != block.FileID {
			continue
		}
		if !mounted.Skipped && mounted.ActualPath != "" {
			if block.Type == InputBlockNotice {
				return block.NoticeText(mounted.ActualPath)
			}
			return block.MountText(mounted.ActualPath)
		}
		if mounted.DownloadURL != "" {
			return block.downloadText(mounted.DownloadURL)
		}
	}
	if block.Type == InputBlockNotice {
		return block.NoticeText("")
	}
	return ""
}

// ToolConfirmation 是一次工具调用的允许/拒绝决定。Result 为 allow 或 deny；SessionThreadID 非空时
// 路由到对应 subagent thread。
type ToolConfirmation struct {
	EventID         string
	ToolUseID       string
	Result          string
	DenyMessage     string
	SessionThreadID string
}

// EventStream 是可以由调用方取消和关闭的事件流。
type EventStream interface {
	Recv(context.Context) (Event, error)
	Close() error
}

// MAClient 是仅使用公开 MA API 的运行时端口。
type MAClient interface {
	CreateSession(context.Context, CreateSessionSpec) (Session, error)
	GetSession(context.Context, string) (Session, error)
	ListSessions(context.Context, string, string) (SessionPage, error)
	// SendMessage 提交一条 user.message；input 已在本地冻结，重试必须原样复用。
	SendMessage(context.Context, string, string, UserInput) (SentResponse, error)
	SendInterrupt(context.Context, string, string) (SentResponse, error)
	// SendToolConfirmation 提交 user.tool_confirmation；EventID 稳定，重试复用。
	SendToolConfirmation(context.Context, string, ToolConfirmation) (SentResponse, error)
	// ListEvents 读取一页完整事件；单条事件仍超过读取上限时返回 ErrEventTooLarge。
	ListEvents(context.Context, string, EventQuery) (EventPage, error)
	StreamEvents(context.Context, string) (EventStream, error)
}

// APIErrorCode 由携带平台错误码的适配器错误实现，供 Runtime 分类而不导入适配器。
type APIErrorCode interface {
	HTTPStatus() int
	ErrorCode() string
}

// RequestNotSent 由适配器在本次请求确定尚未发往远端时实现；结果未知不得实现为 true。
type RequestNotSent interface {
	RequestNotSent() bool
}

func requestNotSent(err error) bool {
	var unsent RequestNotSent
	return errors.As(err, &unsent) && unsent.RequestNotSent()
}
