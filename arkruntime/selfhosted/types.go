// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package selfhosted

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

const (
	// EventTypeAgentToolUse 表示 agent 请求执行内置工具。
	EventTypeAgentToolUse = "agent.tool_use"
	// EventTypeAgentCustomToolUse 表示 agent 请求执行客户自定义工具。
	EventTypeAgentCustomToolUse = "agent.custom_tool_use"
	// EventTypeUserToolConfirmation 表示用户确认工具执行。
	EventTypeUserToolConfirmation = "user.tool_confirmation"
	// EventTypeUserToolResult 表示 self-host worker 回写内置工具结果。
	EventTypeUserToolResult = "user.tool_result"
	// EventTypeUserCustomToolResult 表示 self-host worker 回写自定义工具结果。
	EventTypeUserCustomToolResult = "user.custom_tool_result"
	// EventTypeSessionStatusIdle 表示 session 已经进入空闲状态。
	EventTypeSessionStatusIdle = "session.status_idle"
	// EventTypeSessionStatusTerminated 表示 session 已经终止。
	EventTypeSessionStatusTerminated = "session.status_terminated"
	// EventTypeSessionDeleted 表示 session 已经删除。
	EventTypeSessionDeleted = "session.deleted"
)

const (
	// PermissionAllow 表示工具调用已经被授权执行。
	PermissionAllow = "allow"
	// PermissionAsk 表示工具调用需要等待确认。
	PermissionAsk = "ask"
	// PermissionDeny 表示工具调用被拒绝。
	PermissionDeny = "deny"
)

const (
	// ConfirmationAllow 表示用户批准执行。
	ConfirmationAllow = "allow"
	// ConfirmationDeny 表示用户拒绝执行。
	ConfirmationDeny = "deny"
)

const (
	// EventListOrderAsc 表示按事件创建时间正序读取。
	EventListOrderAsc = "asc"
	// EventListOrderDesc 表示按事件创建时间倒序读取。
	EventListOrderDesc = "desc"
)

const (
	// SessionStopReasonEndTurn 表示 agent 自然完成本轮。
	SessionStopReasonEndTurn = "end_turn"
	// SessionStopReasonRequiresAction 表示 agent 正等待用户输入或外部结果。
	SessionStopReasonRequiresAction = "requires_action"
	// SessionStopReasonRetriesExhausted 表示重试预算用尽导致本轮终止。
	SessionStopReasonRetriesExhausted = "retries_exhausted"
)

const (
	// WorkStateQueued 表示 work 仍在队列中。
	WorkStateQueued = "queued"
	// WorkStateStarting 表示 work 已 ack 并正在启动执行环境。
	WorkStateStarting = "starting"
	// WorkStateActive 表示 work lease 仍归当前 worker 所有。
	WorkStateActive = "active"
	// WorkStateRunning 兼容 MA 早期返回的 running 状态。
	//
	// Deprecated: MA work state 不再包含 running，使用 active。
	WorkStateRunning = "running"
	// WorkStateStopping 表示控制面要求 worker 停止当前 work。
	WorkStateStopping = "stopping"
	// WorkStateStopped 表示当前 work 已经停止。
	WorkStateStopped = "stopped"
)

const (
	// ExpectedLastHeartbeatNoHeartbeat 表示 work 尚未产生成功 heartbeat。
	ExpectedLastHeartbeatNoHeartbeat = "NO_HEARTBEAT"
)

const (
	// WorkerErrorKindAuth 表示 worker credential 无效。
	WorkerErrorKindAuth = "auth"
	// WorkerErrorKindPermission 表示当前 worker 无权访问资源。
	WorkerErrorKindPermission = "permission"
	// WorkerErrorKindLeaseConflict 表示 work lease 已丢失或版本冲突。
	WorkerErrorKindLeaseConflict = "lease_conflict"
	// WorkerErrorKindRateLimit 表示控制面限流。
	WorkerErrorKindRateLimit = "rate_limit"
	// WorkerErrorKindTimeout 表示请求或工具执行超时。
	WorkerErrorKindTimeout = "timeout"
	// WorkerErrorKindNetwork 表示网络层错误。
	WorkerErrorKindNetwork = "network"
	// WorkerErrorKindToolError 表示本地工具执行错误。
	WorkerErrorKindToolError = "tool_error"
	// WorkerErrorKindInvalidResponse 表示控制面响应无法解析。
	WorkerErrorKindInvalidResponse = "invalid_response"
)

const (
	// DefaultWorkerClientType 保留用于源码兼容。
	//
	// Deprecated: MA work API 不再接收 worker client header。
	DefaultWorkerClientType = "ark-self-hosted-worker-go"
	// DefaultWorkerClientVersion 保留用于源码兼容。
	//
	// Deprecated: MA work API 不再接收 worker client header。
	DefaultWorkerClientVersion = "0.1.0"
)

// ErrEventStreamUnsupported 表示当前 API 实现不支持 SSE 事件流。
var ErrEventStreamUnsupported = errors.New("ark event stream is not configured")

// API 是 worker 依赖的最小控制面接口。
type API interface {
	PollWork(ctx context.Context, req PollWorkRequest) (*WorkItem, error)
	AckWork(ctx context.Context, req AckWorkRequest) error
	HeartbeatWork(ctx context.Context, req HeartbeatWorkRequest) (*HeartbeatResponse, error)
	StopWork(ctx context.Context, req StopWorkRequest) error
	GetSession(ctx context.Context, req GetSessionRequest) (*Session, error)
	ListEvents(ctx context.Context, req ListEventsRequest) (*ListEventsResponse, error)
	SendEvent(ctx context.Context, req SendEventRequest) error
	OpenSkill(ctx context.Context, req OpenSkillRequest) (*SkillContent, error)
}

// SkillResolver optionally enriches a session skill reference with authoritative metadata.
type SkillResolver interface {
	ResolveSkill(ctx context.Context, skill SkillRef) (SkillRef, error)
}

// PollWorkRequest 是 worker 轮询 work queue 的请求。
type PollWorkRequest struct {
	EnvironmentID string `json:"environment_id"`
	WorkerID      string `json:"worker_id,omitempty"`
	// Deprecated: MA poll 每次最多返回一个 work。
	MaxItems           int `json:"max_items,omitempty"`
	BlockMS            int `json:"block_ms,omitempty"`
	ReclaimOlderThanMS int `json:"reclaim_older_than_ms,omitempty"`
	// Deprecated: MA work API 不再接收 worker client header。
	WorkerClientType string `json:"-"`
	// Deprecated: MA work API 不再接收 worker client header。
	WorkerClientVersion string `json:"-"`
}

// AckWorkRequest 是 worker 确认已接收 work 的请求。
type AckWorkRequest struct {
	EnvironmentID string `json:"environment_id"`
	WorkID        string `json:"work_id"`
	WorkerID      string `json:"worker_id,omitempty"`
	// Deprecated: MA ack 接口不再接收 lease_id。
	LeaseID string `json:"lease_id,omitempty"`
}

// HeartbeatWorkRequest 是 worker 刷新 work lease 的请求。
type HeartbeatWorkRequest struct {
	EnvironmentID         string `json:"environment_id"`
	WorkID                string `json:"work_id"`
	ExpectedLastHeartbeat string `json:"expected_last_heartbeat,omitempty"`
	DesiredTTLSeconds     int    `json:"desired_ttl_seconds,omitempty"`
	// Deprecated: MA heartbeat 接口不再接收 lease_id。
	LeaseID string `json:"lease_id,omitempty"`
	// Deprecated: MA heartbeat 接口不再接收 worker_id。
	WorkerID string `json:"worker_id,omitempty"`
	// Deprecated: 使用 ExpectedLastHeartbeat 表达 CAS 期望。
	LastHeartbeat string `json:"last_heartbeat,omitempty"`
	// Deprecated: MA heartbeat 接口不再接收 active_tool_call_id。
	ActiveToolCallID string `json:"active_tool_call_id,omitempty"`
}

// HeartbeatResponse 是控制面返回的 heartbeat 结果。
type HeartbeatResponse struct {
	LastHeartbeat string `json:"last_heartbeat,omitempty"`
	LeaseExtended *bool  `json:"lease_extended,omitempty"`
	State         string `json:"state,omitempty"`
	TTLSeconds    int    `json:"ttl_seconds,omitempty"`
	Type          string `json:"type,omitempty"`
	// Deprecated: MA heartbeat 响应不再返回 lease_expires_at。
	LeaseExpiresAt string `json:"lease_expires_at,omitempty"`
	// Deprecated: 使用 TTLSeconds。
	LeaseSeconds int `json:"lease_seconds,omitempty"`
	// Deprecated: 使用 State。
	Status string `json:"status,omitempty"`
	// Deprecated: 使用 State 判断 stopping/stopped。
	StopRequested bool `json:"stop_requested,omitempty"`
	// Deprecated: MA heartbeat 响应体不再返回 request_id。
	RequestID string `json:"request_id,omitempty"`
}

// StopWorkRequest 是 worker 请求控制面结束或停止 work 的请求。
type StopWorkRequest struct {
	EnvironmentID string `json:"environment_id"`
	WorkID        string `json:"work_id"`
	Force         bool   `json:"force,omitempty"`
	// Deprecated: MA stop 接口不再接收 lease_id。
	LeaseID string `json:"lease_id,omitempty"`
	// Deprecated: MA stop 接口不再接收 worker_id。
	WorkerID string `json:"worker_id,omitempty"`
	// Deprecated: MA stop 接口只接收 force。
	Reason string `json:"reason,omitempty"`
	// Deprecated: MA stop 接口只接收 force。
	Message string `json:"message,omitempty"`
}

// GetSessionRequest 是读取 session 配置的请求。
type GetSessionRequest struct {
	SessionID string `json:"session_id"`
}

// ListEventsRequest 是读取 session 增量事件的请求。
type ListEventsRequest struct {
	SessionID   string   `json:"session_id"`
	Page        string   `json:"page,omitempty"`
	CreatedAtGt string   `json:"created_at_gt,omitempty"`
	Limit       int      `json:"limit,omitempty"`
	Order       string   `json:"order,omitempty"`
	Types       []string `json:"types,omitempty"`
	// Deprecated: MA list events 接口不接收 work_id。
	WorkID string `json:"work_id,omitempty"`
	// Deprecated: MA list events 接口不接收 lease_id。
	LeaseID string `json:"lease_id,omitempty"`
}

// StreamEventsRequest 是读取 session SSE 事件流的请求。
type StreamEventsRequest struct {
	SessionID   string   `json:"session_id"`
	EventDeltas []string `json:"event_deltas,omitempty"`
}

// SendEventRequest 是回写用户侧事件的请求。
type SendEventRequest struct {
	SessionID string `json:"session_id"`
	Event     Event  `json:"event"`
	// Deprecated: MA send events 接口不接收 Idempotency-Key header。
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	// Deprecated: MA send events 接口不接收 work_id。
	WorkID string `json:"work_id,omitempty"`
	// Deprecated: MA send events 接口不接收 lease_id。
	LeaseID string `json:"lease_id,omitempty"`
}

// OpenSkillRequest 是下载单个 skill 归档的请求。
type OpenSkillRequest struct {
	SessionID string   `json:"session_id,omitempty"`
	Skill     SkillRef `json:"skill"`
}

// WorkItem 表示控制面分配给 worker 的一份工作。
type WorkItem struct {
	ID                string    `json:"id"`
	AcknowledgedAt    string    `json:"acknowledged_at,omitempty"`
	CreatedAt         string    `json:"created_at,omitempty"`
	Data              WorkData  `json:"data,omitempty"`
	EnvironmentID     string    `json:"environment_id,omitempty"`
	LatestHeartbeatAt string    `json:"latest_heartbeat_at,omitempty"`
	Tags              []WorkTag `json:"tags,omitempty"`
	Secret            string    `json:"secret,omitempty"`
	StartedAt         string    `json:"started_at,omitempty"`
	State             string    `json:"state,omitempty"`
	StopRequestedAt   string    `json:"stop_requested_at,omitempty"`
	StoppedAt         string    `json:"stopped_at,omitempty"`
	Type              string    `json:"type,omitempty"`
	// Deprecated: 使用 Data.ID。
	SessionID string `json:"session_id,omitempty"`
	// Deprecated: 使用 State。
	Status string `json:"status,omitempty"`
	// Deprecated: MA work item 不再返回 attempt。
	Attempt int `json:"attempt,omitempty"`
	// Deprecated: MA work item 不再返回 lease_id。
	LeaseID string `json:"lease_id,omitempty"`
	// Deprecated: MA work item 不再返回 lease_expires_at。
	LeaseExpiresAt string `json:"lease_expires_at,omitempty"`
	// Deprecated: MA work item 不再返回 request_id。
	RequestID string `json:"request_id,omitempty"`
	// Deprecated: MA work item 不再返回 ttl_seconds。
	TTLSeconds int `json:"ttl_seconds,omitempty"`
	// Deprecated: MA work item 不再返回 lease_seconds。
	LeaseSeconds int `json:"lease_seconds,omitempty"`
	// Deprecated: 使用 LatestHeartbeatAt。
	LastHeartbeat string `json:"last_heartbeat,omitempty"`
}

// SessionIDValue 返回 work item 对应的 session id。
func (w WorkItem) SessionIDValue() string {
	if w.SessionID != "" {
		return w.SessionID
	}
	if w.Data.SessionID != "" {
		return w.Data.SessionID
	}
	if w.Data.ID != "" && (w.Data.Type == "" || w.Data.Type == "session") {
		return w.Data.ID
	}
	return ""
}

// LeaseTTLSeconds 返回 work item 的 lease TTL，兼容旧字段 lease_seconds。
func (w WorkItem) LeaseTTLSeconds() int {
	if w.TTLSeconds > 0 {
		return w.TTLSeconds
	}
	return w.LeaseSeconds
}

// LatestHeartbeatValue 返回 work item 的 heartbeat CAS 值，兼容旧字段 last_heartbeat。
func (w WorkItem) LatestHeartbeatValue() string {
	if w.LatestHeartbeatAt != "" {
		return w.LatestHeartbeatAt
	}
	return w.LastHeartbeat
}

// WorkTag 是 work item 关联的火山资源标签。
type WorkTag struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// WorkData 是 work item 的业务载荷。
type WorkData struct {
	Type string `json:"type,omitempty"`
	ID   string `json:"id,omitempty"`
	// Deprecated: 使用 ID。
	SessionID string `json:"session_id,omitempty"`
}

// Session 是 worker 启动一个 session 所需的最小配置快照。
type Session struct {
	ID     string      `json:"id"`
	Agent  AgentConfig `json:"agent,omitempty"`
	Skills []SkillRef  `json:"skills,omitempty"`
}

// SkillRefs 返回 session 绑定的 skills，兼容 skills 在顶层或 agent 下的两种形态。
func (s Session) SkillRefs() []SkillRef {
	if len(s.Skills) > 0 {
		return s.Skills
	}
	return s.Agent.Skills
}

// AgentConfig 是 session 内 agent 的最小配置。
type AgentConfig struct {
	Skills []SkillRef `json:"skills,omitempty"`
}

// SkillRef 描述需要安装到 workdir 的 skill。
type SkillRef struct {
	Name        string `json:"name"`
	DisplayName string `json:"display_name,omitempty"`
	ID          string `json:"id,omitempty"`
	SkillID     string `json:"skill_id,omitempty"`
	Type        string `json:"type,omitempty"`
	Version     string `json:"version,omitempty"`
	DownloadURL string `json:"download_url,omitempty"`
}

// IDValue 返回 skill 的稳定 id。
func (s SkillRef) IDValue() string {
	if s.SkillID != "" {
		return s.SkillID
	}
	return s.ID
}

// NameValue 返回 skill 的展示名，缺失时回退到 skill id。
func (s SkillRef) NameValue() string {
	if s.Name != "" {
		return s.Name
	}
	if s.DisplayName != "" {
		return s.DisplayName
	}
	return s.IDValue()
}

// SkillContent 是一次 skill 下载响应。
type SkillContent struct {
	Body          io.ReadCloser
	ContentLength int64
	FileName      string
	ContentType   string
}

// ListEventsResponse 是 session 事件列表响应。
type ListEventsResponse struct {
	Events   []Event `json:"events"`
	NextPage string  `json:"next_page,omitempty"`
}

// Event 是 worker 关心的扁平事件视图。
type Event struct {
	ID                  string             `json:"id"`
	Type                string             `json:"type"`
	Name                string             `json:"name,omitempty"`
	Input               RawJSON            `json:"input,omitempty"`
	ProcessedAt         string             `json:"processed_at,omitempty"`
	EvaluatedPermission string             `json:"evaluated_permission,omitempty"`
	SessionThreadID     string             `json:"session_thread_id,omitempty"`
	ToolUseID           string             `json:"tool_use_id,omitempty"`
	CustomToolUseID     string             `json:"custom_tool_use_id,omitempty"`
	Result              string             `json:"result,omitempty"`
	DenyMessage         string             `json:"deny_message,omitempty"`
	StopReason          *SessionStopReason `json:"stop_reason,omitempty"`
	Content             []ContentBlock     `json:"content,omitempty"`
	IsError             *bool              `json:"is_error,omitempty"`
	Extra               map[string]RawJSON `json:"-"`
}

// UnmarshalJSON 兼容 input 是 JSON 对象或 JSON 字符串的事件形态。
func (e *Event) UnmarshalJSON(data []byte) error {
	type alias Event
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	var raw map[string]RawJSON
	_ = json.Unmarshal(data, &raw)
	for _, k := range []string{
		"id", "type", "name", "input", "processed_at", "evaluated_permission",
		"session_thread_id", "tool_use_id", "custom_tool_use_id", "result",
		"deny_message", "stop_reason", "content", "is_error",
	} {
		delete(raw, k)
	}
	a.Extra = raw
	*e = Event(a)
	return nil
}

// StopReasonType 返回 session idle stop_reason 的 union type。
func (e Event) StopReasonType() string {
	if e.StopReason == nil {
		return ""
	}
	return e.StopReason.Type
}

// SessionStopReason 是 MA session.status_idle.stop_reason 的扁平视图。
type SessionStopReason struct {
	Type     string   `json:"type,omitempty"`
	EventIDs []string `json:"event_ids,omitempty"`
	Raw      RawJSON  `json:"-"`
}

// UnmarshalJSON 兼容 MA union object，并容忍早期字符串形态。
func (r *SessionStopReason) UnmarshalJSON(data []byte) error {
	if r == nil {
		return nil
	}
	trimmed := bytes.TrimSpace(data)
	if string(trimmed) == "null" || len(trimmed) == 0 {
		*r = SessionStopReason{}
		return nil
	}
	if trimmed[0] == '"' {
		var typ string
		if err := json.Unmarshal(trimmed, &typ); err != nil {
			return err
		}
		*r = SessionStopReason{Type: typ, Raw: append((*r).Raw[:0], trimmed...)}
		return nil
	}
	var raw struct {
		Type     string   `json:"type"`
		EventIDs []string `json:"event_ids"`
	}
	if err := json.Unmarshal(trimmed, &raw); err != nil {
		return err
	}
	*r = SessionStopReason{
		Type:     raw.Type,
		EventIDs: append([]string(nil), raw.EventIDs...),
		Raw:      append((*r).Raw[:0], trimmed...),
	}
	return nil
}

// ContentBlock 是 tool result 的内容块。
type ContentBlock struct {
	Type      string `json:"type"`
	Text      string `json:"text,omitempty"`
	MediaType string `json:"media_type,omitempty"`
	Data      []byte `json:"data,omitempty"`
}

// RawJSON 保存未解释的 JSON 对象，兼容 wire 上以字符串承载 raw JSON。
type RawJSON []byte

// UnmarshalJSON 解析 raw JSON。
func (r *RawJSON) UnmarshalJSON(data []byte) error {
	if string(data) == "null" || len(data) == 0 {
		*r = []byte("{}")
		return nil
	}
	if data[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		if s == "" {
			s = "{}"
		}
		*r = []byte(s)
		return nil
	}
	*r = append((*r)[:0], data...)
	return nil
}

// MarshalJSON 输出原始 JSON。
func (r RawJSON) MarshalJSON() ([]byte, error) {
	if len(r) == 0 {
		return []byte("{}"), nil
	}
	return r, nil
}

// BoolPtr 返回 bool 指针。
func BoolPtr(v bool) *bool {
	return &v
}

// NewUserToolResultEvent 构造 user.tool_result 事件。
func NewUserToolResultEvent(toolUseID string, content []ContentBlock, isError bool, threadID string) Event {
	return Event{
		ID:              NewEventID("evt"),
		Type:            EventTypeUserToolResult,
		ToolUseID:       toolUseID,
		Content:         content,
		IsError:         BoolPtr(isError),
		ProcessedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		SessionThreadID: threadID,
	}
}

// NewUserCustomToolResultEvent 构造 user.custom_tool_result 事件。
func NewUserCustomToolResultEvent(customToolUseID string, content []ContentBlock, isError bool, threadID string) Event {
	return Event{
		ID:              NewEventID("evt"),
		Type:            EventTypeUserCustomToolResult,
		CustomToolUseID: customToolUseID,
		Content:         content,
		IsError:         BoolPtr(isError),
		ProcessedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		SessionThreadID: threadID,
	}
}

// NewEventID 生成事件 id。
func NewEventID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(b[:])
}

// APIError 表示控制面返回的非 2xx 错误。
type APIError struct {
	StatusCode int
	Message    string
	RequestID  string
}

// Error 返回错误字符串。
func (e *APIError) Error() string {
	return fmt.Sprintf("worker api status %d: %s", e.StatusCode, e.Message)
}

// IsStatus 判断错误是否是指定 HTTP 状态码。
func IsStatus(err error, status int) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == status
}

// WorkerError 是 SDK 暴露给调用方的可分类错误。
type WorkerError struct {
	Kind      string
	RequestID string
	Retryable bool
	Message   string
	Err       error
}

// Error 返回错误字符串。
func (e *WorkerError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.Kind
}

// Unwrap 返回底层错误。
func (e *WorkerError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
