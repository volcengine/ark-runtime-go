// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/file"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/session"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

const (
	defaultRequestTimeout = 30 * time.Second
	defaultStreamTimeout  = 30 * time.Minute
)

// ClientOptions 设置客户端的请求时限、大小上限及 HTTP transport。
type ClientOptions struct {
	SDKClient *arkruntime.Client
	// ModelHeaderSource 在每次提交 events 时提供普通目标 Header；不用于读取、文件或 SSE。
	ModelHeaderSource ModelHeaderSource
	HTTPClient        *http.Client
	RequestTimeout    time.Duration
	StreamTimeout     time.Duration
	// UploadTimeout 限制单次 Files 上传，默认 2 分钟。
	UploadTimeout time.Duration
	// FilePollInterval / FilePollTimeout 控制等待文件进入 active 的轮询，默认 2 秒 / 2 分钟。
	FilePollInterval time.Duration
	FilePollTimeout  time.Duration
}

// APIError 投影 SDK 的非成功状态；5xx 也可能由传输失败合成，不能据此认定写入结果。
// 错误文本不包含响应原文或密钥。
type APIError struct {
	Status    int
	Code      string
	RequestID string
}

// Error 返回可安全记录的状态摘要。
func (e *APIError) Error() string {
	return fmt.Sprintf("ma api status=%d code=%s request_id=%s", e.Status, e.Code, e.RequestID)
}

// HTTPStatus 返回 HTTP 状态码，供上层在不导入适配器时分类。
func (e *APIError) HTTPStatus() int { return e.Status }

// ErrorCode 返回平台错误码（如 OperationDenied.SessionTerminated），供上层分类。
func (e *APIError) ErrorCode() string { return e.Code }

var _ core.APIErrorCode = (*APIError)(nil)

// Client 仅访问公开 /api/v3 接口。
type Client struct {
	sdk *arkruntime.Client
	// sessionScope 只在按 Session 绑定的副本中设置，不改变共享客户端。
	sessionScope      *RequestScope
	modelHeaderSource ModelHeaderSource
	baseURL           url.URL
	httpClient        *http.Client
	requestTimeout    time.Duration
	streamTimeout     time.Duration
	uploadTimeout     time.Duration
	downloadHTTP      *http.Client
	filePollInterval  time.Duration
	filePollTimeout   time.Duration
}

var (
	_ core.MAClient     = (*Client)(nil)
	_ core.FileUploader = (*Client)(nil)
)

// NewClient 创建公开 MA 客户端；baseURL 可以是服务根或 /api/v3。
func NewClient(baseURL, key string, options ClientOptions) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid ma base url")
	}
	path := strings.TrimSuffix(u.Path, "/")
	if path != "" && path != "/api/v3" {
		return nil, errors.New("invalid ma base path")
	}
	if key == "" && options.SDKClient == nil {
		return nil, errors.New("ma api key required")
	}
	if options.RequestTimeout < 0 || options.StreamTimeout < 0 {
		return nil, errors.New("invalid ma client limit")
	}
	u.Path = "/api/v3"
	u.RawPath = ""
	if options.SDKClient != nil && options.HTTPClient == nil {
		options.HTTPClient = options.SDKClient.HTTPClient()
	}
	client := http.Client{}
	if options.HTTPClient != nil {
		client = *options.HTTPClient
	}
	result := &Client{sdk: options.SDKClient, modelHeaderSource: options.ModelHeaderSource, baseURL: *u, httpClient: &client, requestTimeout: options.RequestTimeout, streamTimeout: options.StreamTimeout}
	if result.requestTimeout == 0 {
		result.requestTimeout = defaultRequestTimeout
	}
	if result.streamTimeout == 0 {
		result.streamTimeout = defaultStreamTimeout
	}
	result.uploadTimeout, result.filePollInterval, result.filePollTimeout = options.UploadTimeout, options.FilePollInterval, options.FilePollTimeout
	// 下载预签名地址的客户端复用 transport，但最多跟随 3 次同协议重定向（对象存储常见）。
	download := client
	download.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 || req.URL.User != nil {
			return http.ErrUseLastResponse
		}
		return nil
	}
	result.downloadHTTP = &download
	if result.uploadTimeout <= 0 {
		result.uploadTimeout = 2 * time.Minute
	}
	if result.filePollInterval <= 0 {
		result.filePollInterval = 2 * time.Second
	}
	if result.filePollTimeout <= 0 {
		result.filePollTimeout = 2 * time.Minute
	}
	if result.sdk == nil {
		// Runtime reconciles unknown writes; a transport replay could duplicate
		// an accepted input or Vault before that reconciliation can run.
		result.sdk = arkruntime.NewClientWithApiKey(key, arkruntime.WithBaseUrl(result.baseURL.String()), arkruntime.WithHTTPClient(result.httpClient), arkruntime.WithRetryTimes(0))
	}
	return result, nil
}

func sessionPath(id string) (string, error) {
	if id == "" || id == "." || id == ".." || strings.ContainsAny(id, "/\\") {
		return "", errors.New("invalid ma session id")
	}
	return "/sessions/" + id, nil
}

type wireSession struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Agent  struct {
		ID      string      `json:"id"`
		Version json.Number `json:"version"`
		Model   struct {
			ID string `json:"id"`
		} `json:"model"`
	} `json:"agent"`
	EnvironmentID string `json:"environment_id"`
	Environment   struct {
		Config struct {
			Type string `json:"type"`
		} `json:"config"`
	} `json:"environment"`
	VaultIDs []string `json:"vault_ids"`
	Tags     []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"tags"`
}

func (s wireSession) project() core.Session {
	tags := make(map[string]string, len(s.Tags))
	for _, tag := range s.Tags {
		tags[tag.Key] = tag.Value
	}
	return core.Session{ID: s.ID, Status: s.Status, AgentID: s.Agent.ID, AgentVersion: string(s.Agent.Version), ModelEndpoint: s.Agent.Model.ID, EnvironmentID: s.EnvironmentID, EnvironmentType: s.Environment.Config.Type, VaultIDs: s.VaultIDs, Tags: tags}
}

func agentVersion(value string) (int64, error) {
	value = strings.TrimPrefix(value, "v")
	version, err := strconv.ParseInt(value, 10, 64)
	if err != nil || version <= 0 {
		return 0, errors.New("invalid ma agent version")
	}
	return version, nil
}

// CreateSession 按持久化意图创建会话；Channel 不刷新 API key，传输行为由 SDK 配置决定。
func (c *Client) CreateSession(ctx context.Context, spec core.CreateSessionSpec) (core.Session, error) {
	if strings.TrimSpace(spec.CreationKey) == "" {
		return core.Session{}, errors.New("ma creation key required")
	}
	version, err := agentVersion(spec.Binding.AgentVersion)
	if err != nil {
		return core.Session{}, err
	}
	memories := appendUnique(spec.Binding.MemoryIDs, spec.Extras.MemoryIDs)
	resources := make([]map[string]string, 0, len(memories))
	for _, id := range memories {
		resources = append(resources, map[string]string{"type": "memory_store", "memory_store_id": id, "access": "read_write"})
	}
	// 保留标签最后写入：Hook 标签已在 core 校验不含 channel_ 前缀，这里再按键去重兜底。
	tags := make([]map[string]string, 0, len(spec.Extras.Tags)+1)
	keys := make([]string, 0, len(spec.Extras.Tags))
	for key := range spec.Extras.Tags {
		if !strings.HasPrefix(strings.ToLower(key), "channel_") {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		tags = append(tags, map[string]string{"key": key, "value": spec.Extras.Tags[key]})
	}
	tags = append(tags, map[string]string{"key": "channel_session_key", "value": spec.LookupKey})
	if spec.Binding.ChannelID != "" {
		tags = append(tags, map[string]string{"key": "channel_id", "value": spec.Binding.ChannelID}, map[string]string{"key": "channel_binding_epoch", "value": strconv.FormatUint(spec.Binding.BindingEpoch, 10)})
	}
	body := map[string]any{
		"agent":     map[string]any{"type": "agent", "id": spec.Binding.AgentID, "version": version},
		"vault_ids": appendUnique(spec.Binding.VaultIDs, spec.Extras.VaultIDs),
		"resources": resources,
		"tags":      tags,
	}
	// 环境二选一：EnvironmentID 仍用于读取 base env；存在冻结后的 override 时只发
	// environment_with_overrides，不再下发 environment_id（平台据此识别覆盖创建）。
	if spec.EnvironmentOverride != nil {
		if strings.TrimSpace(spec.Binding.EnvironmentID) == "" || (spec.EnvironmentOverride.Type != "cloud" && spec.EnvironmentOverride.Type != "self_hosted") {
			// 旧快照保留原始类型；未知类型明确拒绝，不猜测改写或发出不可解释的请求。
			return core.Session{}, &APIError{Status: http.StatusBadRequest, Code: "InvalidEnvironmentConfig"}
		}
		body["environment"] = map[string]any{
			"type": "environment_with_overrides",
			"id":   spec.Binding.EnvironmentID,
			"config": map[string]any{
				"type": spec.EnvironmentOverride.Type,
				"env":  spec.EnvironmentOverride.Env,
			},
		}
	} else {
		body["environment_id"] = spec.Binding.EnvironmentID
	}
	request, err := sdkBody[session.CreateSessionRequest](body)
	if err != nil {
		return core.Session{}, err
	}
	result, err := requestSDK(c, ctx, http.MethodPost, "/sessions", sdkCallOptions{IdempotencyKey: spec.CreationKey}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*session.Session, error) {
		return sdk.CreateSession(ctx, request, options...)
	})
	if err != nil {
		return core.Session{}, err
	}
	var wire wireSession
	if err := sdkProjection(result, &wire); err != nil {
		return core.Session{}, err
	}
	return wire.project(), nil
}

// appendUnique 返回 base 后接 extra 中未出现过的元素；base 为空切片时结果非 nil。
func appendUnique(base, extra []string) []string {
	seen := make(map[string]bool, len(base)+len(extra))
	out := make([]string, 0, len(base)+len(extra))
	for _, item := range append(append([]string{}, base...), extra...) {
		if !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	return out
}

// GetSession 读取公开会话快照。
func (c *Client) GetSession(ctx context.Context, id string) (core.Session, error) {
	path, err := sessionPath(id)
	if err != nil {
		return core.Session{}, err
	}
	result, err := requestSDK(c, ctx, http.MethodGet, path, sdkCallOptions{}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*session.Session, error) {
		return sdk.GetSession(ctx, id, options...)
	})
	if err != nil {
		return core.Session{}, err
	}
	var wire wireSession
	if err := sdkProjection(result, &wire); err != nil {
		return core.Session{}, err
	}
	return wire.project(), nil
}

// ListSessions 按稳定路由标签获取一页会话。
func (c *Client) ListSessions(ctx context.Context, lookupKey, page string) (core.SessionPage, error) {
	tags, _ := json.Marshal([]map[string]string{{"key": "channel_session_key", "value": lookupKey}})
	query := url.Values{"tags": {string(tags)}, "limit": {"100"}, "order": {"asc"}}
	if page != "" {
		query.Set("page", page)
	}
	var wire struct {
		Data     []wireSession `json:"data"`
		NextPage string        `json:"next_page"`
	}
	pageResult, err := requestSDK(c, ctx, http.MethodGet, "/sessions", sdkCallOptions{}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*session.ListSessionsResponse, error) {
		// Keep all query parameters in one SDK option set; the current SDK
		// concatenates WithQuery after an already-encoded params query.
		for name := range query {
			options = append(options, arkruntime.WithQuery(name, query.Get(name)))
		}
		return sdk.ListSessions(ctx, nil, options...)
	})
	if err != nil {
		return core.SessionPage{}, err
	}
	if err := sdkProjection(pageResult, &wire); err != nil {
		return core.SessionPage{}, err
	}
	result := core.SessionPage{Sessions: make([]core.Session, 0, len(wire.Data)), NextPage: wire.NextPage}
	for _, s := range wire.Data {
		result.Sessions = append(result.Sessions, s.project())
	}
	return result, nil
}

type wireEvent struct {
	ID              string          `json:"id"`
	Type            string          `json:"type"`
	SessionThreadID string          `json:"session_thread_id"`
	ProcessedAt     string          `json:"processed_at"`
	Content         json.RawMessage `json:"content"`
	StopReason      json.RawMessage `json:"stop_reason"`
	Error           struct {
		RetryStatus json.RawMessage `json:"retry_status"`
	} `json:"error"`
	Name                string          `json:"name"`
	MCPServerName       string          `json:"mcp_server_name"`
	ToolUseID           string          `json:"tool_use_id"`
	CustomToolUseID     string          `json:"custom_tool_use_id"`
	IsError             bool            `json:"is_error"`
	EvaluatedPermission json.RawMessage `json:"evaluated_permission"`
	Input               json.RawMessage `json:"input"`
	EventID             string          `json:"event_id"`
	Event               struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	} `json:"event"`
	Delta struct {
		Type    string         `json:"type"`
		Index   int            `json:"index"`
		Content core.TextBlock `json:"content"`
	} `json:"delta"`
}

// textBlocks 解析内容块数组：每块保留 type，文本块额外保留 text；非文本块（图片、文档）只留类型，
// 供上层区分。content 不是数组时返回 nil。
func textBlocks(raw json.RawMessage) []core.TextBlock {
	if len(raw) == 0 {
		return nil
	}
	var items []json.RawMessage
	if json.Unmarshal(raw, &items) != nil {
		return nil
	}
	blocks := make([]core.TextBlock, 0, len(items))
	for _, item := range items {
		var block struct {
			Type string          `json:"type"`
			Text json.RawMessage `json:"text"`
		}
		if json.Unmarshal(item, &block) != nil {
			return nil
		}
		var text string
		if block.Type == "text" && json.Unmarshal(block.Text, &text) != nil {
			return nil
		}
		blocks = append(blocks, core.TextBlock{Type: block.Type, Text: text})
	}
	return blocks
}

// stopEventIDs 读取 requires_action stop_reason 中的 event_ids。
func stopEventIDs(raw json.RawMessage) []string {
	var value struct {
		EventIDs []string `json:"event_ids"`
	}
	if json.Unmarshal(raw, &value) != nil {
		return nil
	}
	return value.EventIDs
}

// rawString 返回 JSON 字符串字段的值；input 在现网 wire 上可能是 JSON 字符串或对象。
func rawString(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text
	}
	return string(raw)
}

func fieldType(raw json.RawMessage) string {
	var value struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(raw, &value) == nil && value.Type != "" {
		return value.Type
	}
	var plain string
	_ = json.Unmarshal(raw, &plain)
	return plain
}

func projectEvent(raw json.RawMessage) (core.Event, error) {
	var wire wireEvent
	if err := json.Unmarshal(raw, &wire); err != nil {
		return core.Event{}, errors.New("invalid ma event json")
	}
	content := textBlocks(wire.Content)
	if content == nil && len(wire.Content) > 0 && string(wire.Content) != "null" && strings.HasSuffix(wire.Type, ".message") {
		// 消息事件的 content 必须是块数组；其他形状属于契约变化，不能静默当作空文本。
		return core.Event{}, errors.New("invalid ma event content")
	}
	event := core.Event{ID: wire.ID, Type: wire.Type, SessionThreadID: wire.SessionThreadID, Content: content, StopReason: fieldType(wire.StopReason), RetryStatus: fieldType(wire.Error.RetryStatus),
		ToolName: wire.Name, MCPServerName: wire.MCPServerName, ToolUseID: wire.ToolUseID, IsError: wire.IsError, Permission: fieldType(wire.EvaluatedPermission), ToolInput: rawString(wire.Input), Raw: append(json.RawMessage(nil), raw...)}
	if event.ToolUseID == "" {
		event.ToolUseID = wire.CustomToolUseID
	}
	if at, err := time.Parse(time.RFC3339Nano, wire.ProcessedAt); err == nil {
		event.ProcessedAt = &at
	}
	if event.StopReason == "requires_action" {
		event.ActionEventIDs = stopEventIDs(wire.StopReason)
	}
	switch wire.Type {
	case "event_start":
		if wire.Event.ID == "" {
			return core.Event{}, errors.New("invalid ma preview start")
		}
		event.Preview = &core.EventPreview{EventID: wire.Event.ID, EventType: wire.Event.Type}
	case "event_delta":
		if wire.EventID == "" || wire.Delta.Index < 0 {
			return core.Event{}, errors.New("invalid ma preview delta")
		}
		if wire.Delta.Type == "content_delta" && wire.Delta.Content.Type == "text" {
			event.Preview = &core.EventPreview{EventID: wire.EventID, Index: wire.Delta.Index, Text: wire.Delta.Content.Text}
		}
	}
	return event, nil
}

func projectEvents(raw []json.RawMessage) ([]core.Event, error) {
	result := make([]core.Event, 0, len(raw))
	for _, item := range raw {
		event, err := projectEvent(item)
		if err != nil {
			return nil, err
		}
		result = append(result, event)
	}
	return result, nil
}

// SendMessage 发送稳定 ID 的用户输入；此方法不会自动重试 POST。
//
// content 依次为可选的背景文本块、非空的用户正文文本块与附件块。附件块按事件协议引用 Files 的
// file_id：image→image.source.file，document→document.source.file，video→video.source.file；
// notice 块渲染为文本块。
func (c *Client) SendMessage(ctx context.Context, sessionID, eventID string, input core.UserInput) (core.SentResponse, error) {
	content := make([]map[string]any, 0, len(input.Blocks)+2)
	if input.Context != "" {
		content = append(content, map[string]any{"type": "text", "text": input.Context})
	}
	if input.Text != "" {
		content = append(content, map[string]any{"type": "text", "text": input.Text})
	}
	for _, block := range input.Blocks {
		if block.Type == core.InputBlockNotice {
			content = append(content, map[string]any{"type": "text", "text": input.AttachmentText(block)})
			continue
		}
		if block.FileID == "" {
			return core.SentResponse{}, errors.New("ma input block missing file id")
		}
		source := map[string]any{"type": "file", "file_id": block.FileID}
		switch block.Type {
		case "image", "video":
			content = append(content, map[string]any{"type": block.Type, "source": source})
		case "document":
			item := map[string]any{"type": "document", "source": source}
			if block.Title != "" {
				item["title"] = block.Title
			}
			content = append(content, item)
		default:
			return core.SentResponse{}, errors.New("ma input block type unsupported")
		}
		if hint := input.AttachmentText(block); hint != "" {
			content = append(content, map[string]any{"type": "text", "text": hint})
		}
	}
	return c.sendEvent(ctx, sessionID, map[string]any{"id": eventID, "type": "user.message", "content": content})
}

// maxUploadBytes 是单次 Files 上传的硬上限；调用方的限额应更小。
const maxUploadBytes = 512 << 20

type fileObject struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func sanitizeFileName(name string) string {
	name = strings.Map(func(r rune) rune {
		if r < 0x20 || r == '"' || r == '\\' || r == '/' {
			return '_'
		}
		return r
	}, name)
	if len(name) > 200 {
		end := 200
		for end > 0 && !utf8.RuneStart(name[end]) {
			end--
		}
		name = name[:end]
	}
	return name
}

// waitFileActive 轮询文件处理状态；processing 期间 file_id 不可被模型引用。
func (c *Client) waitFileActive(ctx context.Context, object fileObject) (string, error) {
	deadline := time.Now().Add(c.filePollTimeout)
	for {
		switch object.Status {
		case "", "active", "uploaded", "processed":
			if object.ID == "" {
				return "", errors.New("ma upload returned no file id")
			}
			return object.ID, nil
		case "failed", "error":
			return "", errors.New("ma file processing failed")
		}
		if !time.Now().Before(deadline) {
			return "", errors.New("ma file not ready in time")
		}
		timer := time.NewTimer(c.filePollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-timer.C:
		}
		path, err := filePath(object.ID)
		if err != nil {
			return "", err
		}
		result, err := requestSDK(c, ctx, http.MethodGet, path, sdkCallOptions{}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*file.FileObject, error) {
			return sdk.RetrieveFile(ctx, object.ID, options...)
		})
		if err != nil {
			return "", err
		}
		if err := sdkProjection(result, &object); err != nil {
			return "", err
		}
	}
}

func filePath(id string) (string, error) {
	if id == "" || id == "." || id == ".." || strings.ContainsAny(id, "/?#%") {
		return "", errors.New("invalid ma file id")
	}
	return "/files/" + url.PathEscape(id), nil
}

// SendInterrupt 发送稳定 ID 的 user.interrupt；适配层不重放，调用方 SDK 的传输配置保持不变。
func (c *Client) SendInterrupt(ctx context.Context, sessionID, eventID string) (core.SentResponse, error) {
	return c.sendEvent(ctx, sessionID, map[string]any{"id": eventID, "type": "user.interrupt"})
}

// SendToolConfirmation 提交工具调用的允许/拒绝；适配层不重放，调用方 SDK 的传输配置保持不变。
func (c *Client) SendToolConfirmation(ctx context.Context, sessionID string, confirmation core.ToolConfirmation) (core.SentResponse, error) {
	if confirmation.EventID == "" || confirmation.ToolUseID == "" || confirmation.Result != "allow" && confirmation.Result != "deny" {
		return core.SentResponse{}, errors.New("ma tool confirmation invalid")
	}
	event := map[string]any{"id": confirmation.EventID, "type": "user.tool_confirmation", "tool_use_id": confirmation.ToolUseID, "result": confirmation.Result}
	if confirmation.Result == "deny" && confirmation.DenyMessage != "" {
		event["deny_message"] = confirmation.DenyMessage
	}
	if confirmation.SessionThreadID != "" {
		event["session_thread_id"] = confirmation.SessionThreadID
	}
	return c.sendEvent(ctx, sessionID, event)
}

func (c *Client) sendEvent(ctx context.Context, sessionID string, event map[string]any) (core.SentResponse, error) {
	path, err := sessionPath(sessionID)
	if err != nil {
		return core.SentResponse{}, err
	}
	result, err := requestSDK(c, ctx, http.MethodPost, path+"/events", sdkCallOptions{}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*session.SendSessionEventsResponse, error) {
		options = append(options, arkruntime.WithExtraBody(map[string]interface{}{"events": []map[string]any{event}}))
		return sdk.SendSessionEvents(ctx, sessionID, &session.SendSessionEventsRequest{}, options...)
	})
	if err != nil {
		return core.SentResponse{}, err
	}
	var raw []json.RawMessage
	if err := sdkProjection(result.Data, &raw); err != nil {
		return core.SentResponse{}, err
	}
	events, err := projectEvents(raw)
	return core.SentResponse{Events: events}, err
}

// maxEventPageLimit 与 MA event 服务的 listLimitMax 一致；更大值会被服务端截断。
const maxEventPageLimit = 200

// ListEvents 按查询条件获取一页 SDK typed events；调用方负责跨页去重与终止条件。
func (c *Client) ListEvents(ctx context.Context, sessionID string, q core.EventQuery) (core.EventPage, error) {
	path, err := sessionPath(sessionID)
	if err != nil {
		return core.EventPage{}, err
	}
	limit := q.Limit
	if limit <= 0 || limit > maxEventPageLimit {
		limit = 100
	}
	result, err := requestSDK(c, ctx, http.MethodGet, path+"/events", sdkCallOptions{}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*session.ListSessionEventsResponse, error) {
		params := &session.SessionEventsListEventsParams{Limit: session.NewOptInt32(int32(limit)), Order: session.NewOptListSessionsOrder(session.ListSessionsOrderAsc), Types: q.Types}
		if q.Page != "" {
			params.Page = session.NewOptString(q.Page)
		}
		if q.CreatedAtGte != nil {
			params.CreatedAtGte = session.NewOptString(q.CreatedAtGte.UTC().Format(time.RFC3339))
		}
		return sdk.ListSessionEvents(ctx, sessionID, params, options...)
	})
	if err != nil {
		return core.EventPage{}, err
	}
	events := make([]core.Event, 0, len(result.Events))
	for _, item := range result.Events {
		event, err := projectSDKEvent(item)
		if err != nil {
			return core.EventPage{}, err
		}
		events = append(events, event)
	}
	next, _ := result.NextPage.Get()
	return core.EventPage{Events: events, NextPage: next}, nil
}
