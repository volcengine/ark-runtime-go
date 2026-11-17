// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package conformance 提供各实现共用的契约测试：Store 契约套件、MA client 契约套件，以及按
// 现网公开 MA wire 形状实现的模拟服务。新的 Store（如 Hosted SQL）或 MA client 实现只需在
// 自己的测试中调用 RunStore / RunMAClient，即可证明满足 Runtime 依赖的全部语义。
package conformance

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// Upload 是一次 Files 上传的记录。
type Upload struct {
	Purpose  string
	FileName string
	MimeType string
	Bytes    int
}

// MAServer 提供当前公开 MA 资源与事件形状的模拟服务，发送事件回包采用 data 字段。
// 读写字段前须持有内嵌锁；ServeHTTP 自身会加锁。
type MAServer struct {
	// ModelEndpoint 是Session冻结Agent的实际模型endpoint。
	ModelEndpoint string
	sync.Mutex
	CreateCount      int
	createdSession   map[string]any
	SendCount        int
	InterruptCount   int
	ListSessionCount int
	// InputID 是最近一次 user.message 的事件 ID，InputIDs 是全部发送过的 ID。
	InputID  string
	InputIDs []string
	// InterruptIDs 是收到的全部 user.interrupt 事件 ID。
	InterruptIDs []string
	// Confirmations 是收到的全部 user.tool_confirmation 事件原文。
	Confirmations []map[string]any
	// ConfirmationEvents 在收到确认时按 result（allow/deny）追加到历史，模拟 Agent 恢复执行。
	ConfirmationEvents map[string][]map[string]any
	// Uploads 记录每次 Files 上传的 purpose、文件名、MIME 与字节数；UploadCode 非 0 时上传失败。
	Uploads    []Upload
	UploadCode int
	// LastContent 是最近一条 user.message 的 content 原文。
	LastContent []any
	// SessionResources 按 session_id 记录已挂载资源，模拟 file 资源的创建与幂等查询。
	SessionResources map[string][]map[string]any
	// ResourceCreateCode 非 0 时资源创建返回该状态码（模拟结果未知），第二次调用自动清零。
	ResourceCreateCode int
	// ResourceConflictPath 非空时该挂载路径返回 409。
	ResourceConflictPath string
	// Events 是会话历史；GET events 按 types 过滤后分页返回。
	Events []map[string]any
	// InterruptEvents 在每次收到 user.interrupt 时追加到历史，模拟 loop 中断后发布的收尾事件。
	InterruptEvents []map[string]any
	// RecordInterrupt 把中断回显写入历史；回显本身不证明后台取消已经执行完。
	RecordInterrupt bool

	CreateCode             int
	CreateErrorCode        string
	CreatePersistThenError bool
	DuplicateCreateProof   bool
	CreateWrongVault       bool
	CreateSessionID        string
	CreationKey            string
	LookupKey              string
	SessionStatus          map[string]string

	SendCode             int
	SendErrorCode        string
	SendCodes            []int
	SendErrorCodes       []string
	SendNoProof          bool
	SendBadEcho          bool
	SendPersistThenError bool

	PageSize         int
	LastTypes        []string
	LastCreatedAtGte string
	// CreateTags / CreateVaultIDs / CreateResources 记录最近一次创建会话的请求内容。
	CreateTags      map[string]string
	CreateVaultIDs  []string
	CreateResources []map[string]any
	// CreateEnvironment 记录最近一次创建会话的 environment 载荷（environment_with_overrides）。
	CreateEnvironment map[string]any
	// EnvironmentType / EnvironmentEnv 是 GET /environments/:id 的夹具返回。
	EnvironmentType string
	EnvironmentEnv  map[string]string
	// EnvironmentGetCount 记录基础环境读取次数，以验证创建重试不重读。
	EnvironmentGetCount int
}

func (f *MAServer) sessionJSON(id string) map[string]any {
	status := "idle"
	if f.SessionStatus != nil && f.SessionStatus[id] != "" {
		status = f.SessionStatus[id]
	}
	vaults := append([]string(nil), f.CreateVaultIDs...)
	if vaults == nil {
		vaults = []string{}
	}
	session := map[string]any{"id": id, "type": "session", "status": status, "agent": map[string]any{"type": "agent", "id": "agent", "version": 1}, "environment_id": "env", "vault_ids": vaults, "created_at": "2026-10-13T00:00:00Z", "updated_at": "2026-10-13T00:00:00Z"}
	if f.ModelEndpoint != "" {
		session["agent"].(map[string]any)["model"] = map[string]any{"id": f.ModelEndpoint}
	}
	if f.LookupKey != "" {
		session["tags"] = []map[string]string{{"key": "channel_session_key", "value": f.LookupKey}}
	}
	return session
}

// acceptedSessionJSON 从创建效果快照生成查询结果，后续未受理的请求不得改写创建证明。
func (f *MAServer) acceptedSessionJSON(id string) map[string]any {
	session := make(map[string]any, len(f.createdSession))
	for key, value := range f.createdSession {
		session[key] = value
	}
	session["id"] = id
	if status := f.SessionStatus[id]; status != "" {
		session["status"] = status
	}
	return session
}

func (f *MAServer) writeError(w http.ResponseWriter, status int, code string) {
	if code == "" {
		code = "unknown"
	}
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `{"error":{"code":%q}}`, code)
}

func (f *MAServer) nextSendFailure() (int, string) {
	if len(f.SendCodes) > 0 {
		code := f.SendCodes[0]
		f.SendCodes = f.SendCodes[1:]
		errorCode := ""
		if len(f.SendErrorCodes) > 0 {
			errorCode = f.SendErrorCodes[0]
			f.SendErrorCodes = f.SendErrorCodes[1:]
		}
		return code, errorCode
	}
	return f.SendCode, f.SendErrorCode
}

// ServeHTTP 实现 Runtime 用到的全部公开 MA 路由。
func (f *MAServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.Lock()
	defer f.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/api/v3/sessions":
		f.CreateCount++
		f.CreationKey = r.Header.Get("Idempotency-Key")
		var body struct {
			Tags []struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			} `json:"tags"`
			VaultIDs    []string         `json:"vault_ids"`
			Resources   []map[string]any `json:"resources"`
			Environment map[string]any   `json:"environment"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.CreateTags = map[string]string{}
		for _, tag := range body.Tags {
			f.CreateTags[tag.Key] = tag.Value
			if tag.Key == "channel_session_key" {
				f.LookupKey = tag.Value
			}
		}
		f.CreateVaultIDs = body.VaultIDs
		f.CreateResources = body.Resources
		f.CreateEnvironment = body.Environment
		id := f.CreateSessionID
		if id == "" {
			id = "sesn-demo"
		}
		if f.CreateCode == 0 || f.CreatePersistThenError {
			// 创建效果独立于调用方是否收到响应；普通成功请求也必须支持后续查账。
			f.createdSession = f.sessionJSON(id)
		}
		if f.CreateCode != 0 {
			// CreatePersistThenError 时创建已生效但回包失败：ListSessions 可以找回。
			f.writeError(w, f.CreateCode, f.CreateErrorCode)
			return
		}
		session := f.sessionJSON(id)
		if f.CreateWrongVault {
			session["vault_ids"] = []string{"foreign"}
		}
		_ = json.NewEncoder(w).Encode(session)
	case r.Method == http.MethodPost && r.URL.Path == "/api/v3/files":
		if f.UploadCode != 0 {
			f.writeError(w, f.UploadCode, "")
			return
		}
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			f.writeError(w, http.StatusBadRequest, "InvalidParameter")
			return
		}
		file, header, err := r.FormFile("file")
		if err != nil {
			f.writeError(w, http.StatusBadRequest, "InvalidParameter")
			return
		}
		data, _ := io.ReadAll(file)
		_ = file.Close()
		f.Uploads = append(f.Uploads, Upload{Purpose: r.FormValue("purpose"), FileName: header.Filename, MimeType: header.Header.Get("Content-Type"), Bytes: len(data)})
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "file", "id": fmt.Sprintf("file-%d", len(f.Uploads)), "status": "processing", "purpose": r.FormValue("purpose"), "filename": header.Filename, "created_at": 0, "expire_at": -1})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v3/files/"):
		id := strings.TrimPrefix(r.URL.Path, "/api/v3/files/")
		meta := map[string]any{"object": "file", "id": id, "status": "active", "purpose": "user_data", "filename": "fixture", "created_at": 0, "expire_at": -1}
		if r.Header.Get("X-Ark-PreSignedURL-ExpiresAfter") != "" {
			meta["download_url"] = "https://files.example.com/" + id + "?signature=fixture"
		}
		_ = json.NewEncoder(w).Encode(meta)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/v3/sessions/") && strings.HasSuffix(r.URL.Path, "/resources"):
		var body struct {
			FileID    string `json:"file_id"`
			Type      string `json:"type"`
			MountPath string `json:"mount_path"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		sessionID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v3/sessions/"), "/resources")
		if f.ResourceConflictPath != "" && body.MountPath == f.ResourceConflictPath {
			f.writeError(w, http.StatusConflict, "Resource.AlreadyExists")
			return
		}
		if f.ResourceCreateCode != 0 {
			code := f.ResourceCreateCode
			f.ResourceCreateCode = 0
			f.writeError(w, code, "ServiceUnavailable")
			return
		}
		for _, resource := range f.SessionResources[sessionID] {
			if resource["mount_path"] == core.MountedActualPath(body.MountPath) {
				// Console 以路径找回既有资源，wire file_id 是复制后重新注册的 ID。
				_ = json.NewEncoder(w).Encode(resource)
				return
			}
		}
		if f.SessionResources == nil {
			f.SessionResources = map[string][]map[string]any{}
		}
		resource := map[string]any{
			"id":         fmt.Sprintf("rsrc-%d", len(f.SessionResources[sessionID])+1),
			"type":       body.Type,
			"file_id":    "registered-" + body.FileID,
			"mount_path": core.MountedActualPath(body.MountPath),
		}
		f.SessionResources[sessionID] = append(f.SessionResources[sessionID], resource)
		_ = json.NewEncoder(w).Encode(resource)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v3/sessions/") && strings.HasSuffix(r.URL.Path, "/resources"):
		sessionID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v3/sessions/"), "/resources")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": f.SessionResources[sessionID]})
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/v3/sessions/") && strings.HasSuffix(r.URL.Path, "/events"):
		var raw struct {
			Events []map[string]any `json:"events"`
		}
		var body struct {
			Events []struct {
				ID      string `json:"id"`
				Type    string `json:"type"`
				Content []struct {
					Text string `json:"text"`
				} `json:"content"`
			} `json:"events"`
		}
		data, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(data, &raw)
		_ = json.Unmarshal(data, &body)
		if len(raw.Events) > 0 && raw.Events[0]["type"] == "user.tool_confirmation" {
			confirmation := raw.Events[0]
			f.Confirmations = append(f.Confirmations, confirmation)
			f.Events = append(f.Events, confirmation)
			f.Events = append(f.Events, f.ConfirmationEvents[fmt.Sprint(confirmation["result"])]...)
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{confirmation}})
			return
		}
		if len(body.Events) > 0 && body.Events[0].Type == "user.interrupt" {
			f.InterruptCount++
			f.InterruptIDs = append(f.InterruptIDs, body.Events[0].ID)
			if f.RecordInterrupt {
				f.Events = append(f.Events, raw.Events[0])
			}
			f.Events = append(f.Events, f.InterruptEvents...)
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{map[string]any{"id": body.Events[0].ID, "type": "user.interrupt"}}})
			return
		}
		f.SendCount++
		if len(body.Events) > 0 {
			f.InputID = body.Events[0].ID
			f.InputIDs = append(f.InputIDs, f.InputID)
			content, _ := raw.Events[0]["content"].([]any)
			f.LastContent = content
		}
		status, errorCode := f.nextSendFailure()
		if status != 0 && !f.SendPersistThenError {
			f.writeError(w, status, errorCode)
			return
		}
		// 回显保留请求中的全部内容块（附件块原样），与现网 caller event 回显一致。
		var content any = []map[string]string{{"type": "text", "text": ""}}
		if len(raw.Events) > 0 && raw.Events[0]["content"] != nil {
			content = raw.Events[0]["content"]
		}
		input := map[string]any{"id": f.InputID, "type": "user.message", "content": content}
		f.Events = append(f.Events, input)
		if status != 0 {
			f.writeError(w, status, errorCode)
			return
		}
		switch {
		case f.SendNoProof:
			_, _ = w.Write([]byte(`{"data":[]}`))
		case f.SendBadEcho:
			_, _ = fmt.Fprintf(w, `{"data":[{"id":%q,"type":"user.message","content":[{"type":"text","text":"wrong"}]}]}`, f.InputID)
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{input}})
		}
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v3/sessions/") && strings.HasSuffix(r.URL.Path, "/events"):
		f.LastTypes = r.URL.Query()["types"]
		f.LastCreatedAtGte = r.URL.Query().Get("created_at[gte]")
		filtered := f.Events
		if len(f.LastTypes) > 0 {
			allowed := map[string]bool{}
			for _, t := range f.LastTypes {
				allowed[t] = true
			}
			filtered = nil
			for _, event := range f.Events {
				if allowed[fmt.Sprint(event["type"])] {
					filtered = append(filtered, event)
				}
			}
		}
		start := 0
		if r.URL.Query().Get("page") != "" {
			_, _ = fmt.Sscanf(r.URL.Query().Get("page"), "%d", &start)
		}
		end := len(filtered)
		if f.PageSize > 0 && end-start > f.PageSize {
			end = start + f.PageSize
		}
		if start > end {
			start = end
		}
		next := ""
		if end < len(filtered) {
			next = fmt.Sprint(end)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": filtered[start:end], "next_page": next})
	case r.Method == http.MethodGet && r.URL.Path == "/api/v3/sessions":
		f.ListSessionCount++
		if f.createdSession != nil {
			sessions := []map[string]any{f.acceptedSessionJSON(fmt.Sprint(f.createdSession["id"]))}
			if f.DuplicateCreateProof {
				other := f.acceptedSessionJSON("sesn-other")
				sessions = append(sessions, other)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": sessions})
		} else {
			_, _ = w.Write([]byte(`{"data":[]}`))
		}
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v3/sessions/") &&
		(strings.HasSuffix(r.URL.Path, "/events/stream") || strings.Contains(r.URL.Path, "/threads/") && strings.HasSuffix(r.URL.Path, "/stream")):
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(": eof\n\n"))
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v3/sessions/"):
		id := strings.TrimPrefix(r.URL.Path, "/api/v3/sessions/")
		if f.SessionStatus != nil && f.SessionStatus[id] == "deleted" {
			f.writeError(w, http.StatusNotFound, "InvalidSession.NotFound")
			return
		}
		if f.createdSession != nil && f.createdSession["id"] == id {
			_ = json.NewEncoder(w).Encode(f.acceptedSessionJSON(id))
		} else {
			_ = json.NewEncoder(w).Encode(f.sessionJSON(id))
		}
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v3/environments/"):
		id := strings.TrimPrefix(r.URL.Path, "/api/v3/environments/")
		f.EnvironmentGetCount++
		envType := f.EnvironmentType
		if envType == "" {
			envType = "cloud"
		}
		env := map[string]any{}
		for k, v := range f.EnvironmentEnv {
			env[k] = v
		}
		config := map[string]any{"type": envType}
		if envType != "self_hosted" {
			config["env"] = env
		}
		response := map[string]any{
			"id":     id,
			"type":   "environment",
			"config": config,
			"name":   "fixture", "created_at": "2026-10-13T00:00:00Z", "updated_at": "2026-10-13T00:00:00Z",
		}
		_ = json.NewEncoder(w).Encode(response)
	default:
		http.NotFound(w, r)
	}
}

// AppendAfterInput 在第一条输入被消费后追加事件，模拟 Agent 在本轮输入之后产出结果。
func (f *MAServer) AppendAfterInput(t *testing.T, events ...map[string]any) {
	t.Helper()
	f.AppendAfterInputCount(t, 1, events...)
}

// AppendAfterInputCount 在第 n 次发送输入后追加事件。
func (f *MAServer) AppendAfterInputCount(t *testing.T, n int, events ...map[string]any) {
	t.Helper()
	started := time.Now()
	defer func() {
		if t.Failed() {
			f.Lock()
			t.Logf("DIAG append-wait n=%d inputs=%d creates=%d sends=%d interrupts=%d waited=%s", n, len(f.InputIDs), f.CreateCount, f.SendCount, f.InterruptCount, time.Since(started))
			f.Unlock()
		}
	}()
	require.Eventually(t, func() bool {
		f.Lock()
		defer f.Unlock()
		return len(f.InputIDs) >= n
	}, 15*time.Second, 5*time.Millisecond)
	f.Lock()
	f.Events = append(f.Events, events...)
	f.Unlock()
}

// AnswerEvents 按现网 wire 形状构造一轮结果：agent.message 与 end_turn idle 都不带关联字段。
func AnswerEvents(text string) []map[string]any {
	return []map[string]any{
		{"id": "answer-" + text, "type": "agent.message", "content": []map[string]string{{"type": "text", "text": text}}},
		{"id": "idle-" + text, "type": "session.status_idle", "stop_reason": map[string]string{"type": "end_turn"}},
	}
}
