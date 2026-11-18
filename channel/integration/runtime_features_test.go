// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

var errUnknownDelivery = errors.New("unknown provider result")

func httpHandler(f *conformance.MAServer) http.Handler { return f }

// recordingObserver 记录 Runtime 上报的观测事件。
type recordingObserver struct {
	mu    sync.Mutex
	items []core.Observation
}

func (o *recordingObserver) Observe(_ context.Context, item core.Observation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.items = append(o.items, item)
}

func (o *recordingObserver) has(name, status string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, item := range o.items {
		if item.Name == name && item.Status == status {
			return true
		}
	}
	return false
}

func TestRuntimeReportsLifecycleObservations(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	client, err := newTestMA(server.URL, "fixture-key", ma.ClientOptions{RequestTimeout: time.Second, StreamTimeout: time.Second})
	require.NoError(t, err)
	observer := &recordingObserver{}
	options := testRuntimeOptions()
	options.Observer = observer
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: runtimeBinding(), MAClient: client, Provider: p}}, options)
	require.NoError(t, err)
	mustEnqueue(t, runtime, "binding", runtimeMessage())
	mustEnqueue(t, runtime, "binding", runtimeMessage())
	runUntilCleanup(t, runtime)
	f.AppendAfterInput(t, conformance.AnswerEvents("answer")...)
	waitSettledExternal(t, store, core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Eventually(t, func() bool { return observer.has(core.EventDeliveryResult, "done") }, 2*time.Second, 10*time.Millisecond)
	for _, want := range [][2]string{{core.EventIngress, "accepted"}, {core.EventIngress, "duplicate"}, {core.EventSessionCreate, "created"}, {core.EventTurnSubmit, "accepted"}, {core.EventTurnSettle, "done"}} {
		require.True(t, observer.has(want[0], want[1]), "missing observation %v", want)
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	for _, item := range observer.items {
		if item.Name == core.EventTurnSettle {
			require.Positive(t, item.Duration, "settle carries end-to-end latency")
			require.NotEmpty(t, item.TurnID)
			require.Equal(t, "sesn-demo", item.SessionID)
		}
	}
}

type panickingObserver struct{}

func (panickingObserver) Observe(context.Context, core.Observation) { panic("observer bug") }

func TestRuntimeObserverPanicNeverBreaksProcessing(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	h := newHarnessWith(t, f, p, runtimeBinding(), func() core.RuntimeOptions {
		options := testRuntimeOptions()
		options.Observer = panickingObserver{}
		return options
	}())
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("answer")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
}

func TestRuntimeResetConversationStartsNewSessionOnNextMessage(t *testing.T) {
	f, p := &conformance.MAServer{CreateSessionID: "sesn-fresh"}, &runtimeProvider{}
	h := newHarness(t, f, p)
	old := seedConversation(t, h.store, "sesn-old")
	reset, err := h.store.ResetConversation(context.Background(), old.ID)
	require.NoError(t, err)
	require.Equal(t, core.SessionBinding{State: core.SessionReserved, Generation: 2}, reset.Session)
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("after reset")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "after reset", state.export.Deliveries[0].Body.Text)
	conversation, err := h.store.GetConversation(context.Background(), old.ID)
	require.NoError(t, err)
	require.Equal(t, "sesn-fresh", conversation.Session.SessionID)
	require.Equal(t, uint64(2), conversation.Session.Generation, "the reserved generation is used, not skipped")
	f.Lock()
	require.Equal(t, 1, f.CreateCount)
	f.Unlock()
}

func TestRuntimePruneOnceRemovesExpiredRecords(t *testing.T) {
	now := time.Now()
	var mu sync.Mutex
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	store, err := newEmbeddedStore(t, clock)
	require.NoError(t, err)
	seedConversation(t, store, "sesn-old")
	client, err := newTestMA("https://invalid.local", "fixture-key", ma.ClientOptions{})
	require.NoError(t, err)
	observer := &recordingObserver{}
	options := testRuntimeOptions()
	options.Clock, options.Observer = clock, observer
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: runtimeBinding(), MAClient: client, Provider: &runtimeProvider{}}}, options)
	require.NoError(t, err)
	require.Equal(t, core.PruneResult{}, runtime.PruneOnce(context.Background()), "records inside the retention window are kept")
	mu.Lock()
	now = now.Add(8 * 24 * time.Hour)
	mu.Unlock()
	removed := runtime.PruneOnce(context.Background())
	require.Equal(t, 1, removed.Turns)
	require.True(t, observer.has(core.EventCleanup, "pruned"))
	export, err := store.Export(context.Background())
	require.NoError(t, err)
	require.Empty(t, export.Turns)
	require.Len(t, export.Conversations, 1, "conversations are never pruned")
}

func TestRuntimeRetentionBounds(t *testing.T) {
	for _, retention := range []time.Duration{time.Hour, 400 * 24 * time.Hour} {
		options := testRuntimeOptions()
		options.Retention = retention
		_, err := options.Normalize()
		require.ErrorIs(t, err, core.ErrRuntimeConfig)
	}
}

// ---------------------------------------------------------------------------
// 过程卡片

type progressProvider struct {
	runtimeProvider
	pmu     sync.Mutex
	updates []core.ProgressUpdate
	handles []string
	fail    bool
}

func (p *progressProvider) ReportProgress(_ context.Context, handle string, update core.ProgressUpdate) (string, error) {
	p.pmu.Lock()
	defer p.pmu.Unlock()
	p.updates = append(p.updates, update)
	p.handles = append(p.handles, handle)
	if p.fail {
		return "", errors.New("card api down")
	}
	return "card-1", nil
}

func (p *progressProvider) snapshot() ([]core.ProgressUpdate, []string) {
	p.pmu.Lock()
	defer p.pmu.Unlock()
	return append([]core.ProgressUpdate(nil), p.updates...), append([]string(nil), p.handles...)
}

func progressHarness(t *testing.T, f *conformance.MAServer, p *progressProvider) *rtHarness {
	binding := runtimeBinding()
	binding.ShowProgress = true
	options := testRuntimeOptions()
	options.ProgressInterval = time.Millisecond
	return newHarnessWith(t, f, p, binding, options)
}

func TestRuntimeReportsProgressCardLifecycle(t *testing.T) {
	f, p := &conformance.MAServer{}, &progressProvider{}
	h := progressHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, map[string]any{"id": "tool-1", "type": "agent.tool_use", "name": "bash", "input": `{"command":"secret-arg"}`})
	require.Eventually(t, func() bool {
		updates, _ := p.snapshot()
		return len(updates) >= 1 && len(updates[len(updates)-1].Steps) == 1
	}, 2*time.Second, 5*time.Millisecond)
	f.Lock()
	f.Events = append(f.Events, map[string]any{"id": "result-1", "type": "agent.tool_result", "tool_use_id": "tool-1"})
	f.Events = append(f.Events, conformance.AnswerEvents("done")...)
	f.Unlock()
	h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	updates, handles := p.snapshot()
	require.GreaterOrEqual(t, len(updates), 2)
	require.Empty(t, handles[0], "the first report creates the card")
	for _, handle := range handles[1:] {
		require.Equal(t, "card-1", handle, "later reports patch the same card")
	}
	last := updates[len(updates)-1]
	require.Equal(t, core.ProgressDone, last.State)
	require.Equal(t, "bash", last.Steps[0].Name)
	encoded, _ := json.Marshal(updates)
	require.NotContains(t, string(encoded), "secret-arg", "tool arguments never reach the progress card")
}

func TestRuntimeProgressFailureNeverBlocksAnswer(t *testing.T) {
	f, p := &conformance.MAServer{}, &progressProvider{fail: true}
	h := progressHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, map[string]any{"id": "tool-1", "type": "agent.tool_use", "name": "bash"})
	require.Eventually(t, func() bool { updates, _ := p.snapshot(); return len(updates) > 0 }, 2*time.Second, 5*time.Millisecond)
	f.Lock()
	f.Events = append(f.Events, conformance.AnswerEvents("still answered")...)
	f.Unlock()
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "still answered", state.export.Deliveries[0].Body.Text)
}

func TestRuntimeProgressDisabledByDefault(t *testing.T) {
	f, p := &conformance.MAServer{}, &progressProvider{}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, append([]map[string]any{{"id": "tool", "type": "agent.tool_use", "name": "bash"}}, conformance.AnswerEvents("x")...)...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	updates, _ := p.snapshot()
	require.Empty(t, updates, "progress is opt-in per binding")
}

// ---------------------------------------------------------------------------
// 显式审批

type approvalProvider struct {
	runtimeProvider
	amu      sync.Mutex
	requests []core.ApprovalRequest
}

func (p *approvalProvider) PromptApproval(_ context.Context, request core.ApprovalRequest) error {
	p.amu.Lock()
	defer p.amu.Unlock()
	p.requests = append(p.requests, request)
	return nil
}

func (p *approvalProvider) prompted() []core.ApprovalRequest {
	p.amu.Lock()
	defer p.amu.Unlock()
	return append([]core.ApprovalRequest(nil), p.requests...)
}

func approvalHarness(t *testing.T, f *conformance.MAServer, p *approvalProvider, approvals bool, options core.RuntimeOptions) *rtHarness {
	binding := runtimeBinding()
	binding.Approvals = approvals
	return newHarnessWith(t, f, p, binding, options)
}

func askEvents() []map[string]any {
	return []map[string]any{
		{"id": "tool-ask", "type": "agent.tool_use", "name": "delete_repo", "input": `{"repo":"prod"}`, "evaluated_permission": "ask"},
		{"id": "idle-ask", "type": "session.status_idle", "stop_reason": map[string]any{"type": "requires_action", "event_ids": []string{"tool-ask"}}},
	}
}

func TestRuntimeApprovalAllowResumesTurn(t *testing.T) {
	f, p := &conformance.MAServer{ConfirmationEvents: map[string][]map[string]any{
		"allow": append([]map[string]any{{"id": "running", "type": "session.status_running"}, {"id": "result-ask", "type": "agent.tool_result", "tool_use_id": "tool-ask"}}, conformance.AnswerEvents("deleted")...),
	}}, &approvalProvider{}
	h := approvalHarness(t, f, p, true, testRuntimeOptions())
	ingested := h.enqueue(runtimeMessage())
	h.run()
	events := askEvents()
	events[0]["input"] = `{"repo":"prod","token":"fixture-approval-secret"}`
	f.AppendAfterInput(t, events...)
	require.Eventually(t, func() bool { return len(p.prompted()) == 1 }, 2*time.Second, 5*time.Millisecond)
	request := p.prompted()[0]
	require.Equal(t, "tool-ask", request.ToolUseID)
	require.Equal(t, "delete_repo", request.ToolName)
	require.Equal(t, "user", request.ApproverID)
	require.Contains(t, request.InputPreview, "prod")
	require.Contains(t, request.InputPreview, "[REDACTED]")
	require.NotContains(t, request.InputPreview, "fixture-approval-secret")
	require.ErrorIs(t, h.runtime.SubmitApproval(context.Background(), core.ApprovalDecision{ChatID: "chat", TurnID: ingested.Turn.ID, ToolUseID: "tool-ask", Allow: true, DeciderID: "someone-else"}), core.ErrApprovalForbidden)
	require.ErrorIs(t, h.runtime.SubmitApproval(context.Background(), core.ApprovalDecision{ChatID: "chat", TurnID: ingested.Turn.ID, ToolUseID: "other-tool", Allow: true, DeciderID: "user"}), core.ErrApprovalExpired)
	require.NoError(t, h.runtime.SubmitApproval(context.Background(), core.ApprovalDecision{ChatID: "chat", TurnID: ingested.Turn.ID, ToolUseID: "tool-ask", Allow: true, DeciderID: "user"}))
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "deleted", state.export.Deliveries[0].Body.Text)
	f.Lock()
	require.NotEmpty(t, f.Confirmations)
	// 用户点击与 Worker 查账可以并发补发；同一决定必须始终携带相同的防重事件 ID。
	for _, confirmation := range f.Confirmations {
		require.Equal(t, f.Confirmations[0], confirmation)
		require.Equal(t, "allow", confirmation["result"])
		require.Equal(t, "tool-ask", confirmation["tool_use_id"])
		require.NotEmpty(t, confirmation["id"])
	}
	require.Zero(t, f.InterruptCount, "an approved turn is not interrupted")
	f.Unlock()
	require.Len(t, p.prompted(), 1, "the same tool call is prompted once")
	require.ErrorIs(t, h.runtime.SubmitApproval(context.Background(), core.ApprovalDecision{ChatID: "chat", TurnID: ingested.Turn.ID, ToolUseID: "tool-ask", Allow: true, DeciderID: "user"}), core.ErrApprovalExpired, "late clicks after settlement are rejected")
}

func TestRuntimeApprovalDenyIsForwarded(t *testing.T) {
	f, p := &conformance.MAServer{ConfirmationEvents: map[string][]map[string]any{
		"deny": append([]map[string]any{{"id": "running", "type": "session.status_running"}}, conformance.AnswerEvents("好的，不删除")...),
	}}, &approvalProvider{}
	h := approvalHarness(t, f, p, true, testRuntimeOptions())
	ingested := h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, askEvents()...)
	require.Eventually(t, func() bool { return len(p.prompted()) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.NoError(t, h.runtime.SubmitApproval(context.Background(), core.ApprovalDecision{ChatID: "chat", TurnID: ingested.Turn.ID, ToolUseID: "tool-ask", Allow: false, DeciderID: "user", DenyReason: "不允许"}))
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "好的，不删除", state.export.Deliveries[0].Body.Text)
	f.Lock()
	require.Equal(t, "deny", f.Confirmations[0]["result"])
	require.Equal(t, "不允许", f.Confirmations[0]["deny_message"])
	f.Unlock()
}

func TestRuntimeApprovalDisabledFallsBackToTimeout(t *testing.T) {
	f, p := &conformance.MAServer{}, &approvalProvider{}
	options := deadlineRuntimeOptions()
	options.MessageDeadline = 3 * time.Second
	h := approvalHarness(t, f, p, false, options)
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, askEvents()...)
	state := h.waitSettledWithinExternal(t, h.store, core.TurnFailed, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "requires_action_timeout", state.turn.Outcome.ErrorKind)
	require.Empty(t, p.prompted(), "approvals are opt-in per binding")
}

func TestPendingApprovalsSkipsAllowedAndCustomTools(t *testing.T) {
	f, p := &conformance.MAServer{}, &approvalProvider{}
	h := approvalHarness(t, f, p, true, testRuntimeOptions())
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t,
		map[string]any{"id": "custom", "type": "agent.custom_tool_use", "name": "crm_lookup"},
		map[string]any{"id": "allowed", "type": "agent.tool_use", "name": "bash", "evaluated_permission": "allow"},
		map[string]any{"id": "idle", "type": "session.status_idle", "stop_reason": map[string]any{"type": "requires_action", "event_ids": []string{"custom", "allowed"}}})
	time.Sleep(200 * time.Millisecond)
	require.Empty(t, p.prompted(), "custom tools are answered by the customer server, not the end user")
}

// ---------------------------------------------------------------------------
// 附件

type attachmentProvider struct {
	runtimeProvider
	fmu     sync.Mutex
	fetched int
	files   map[string]string
	// mimes 按附件 Key 覆盖下载返回的 MIME；未配置时 image 为 image/png，其余为 application/pdf。
	mimes map[string]string
	fail  error
}

func (p *attachmentProvider) FetchAttachment(_ context.Context, _ core.Message, attachment core.Attachment) (core.DownloadedAttachment, error) {
	p.fmu.Lock()
	defer p.fmu.Unlock()
	p.fetched++
	if p.fail != nil {
		return core.DownloadedAttachment{}, p.fail
	}
	content := p.files[attachment.Key]
	mime := "application/pdf"
	if attachment.Kind == "image" {
		mime = "image/png"
	}
	if override, ok := p.mimes[attachment.Key]; ok {
		mime = override
	}
	return core.DownloadedAttachment{Body: io.NopCloser(strings.NewReader(content)), FileName: attachment.FileName, MimeType: mime, Size: int64(len(content))}, nil
}

func attachmentHarness(t *testing.T, f *conformance.MAServer, p *attachmentProvider, limits core.AttachmentLimits, options core.RuntimeOptions) *rtHarness {
	t.Helper()
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	client, err := newTestMA(server.URL, "fixture-key", ma.ClientOptions{RequestTimeout: time.Second, StreamTimeout: time.Second, FilePollInterval: time.Millisecond})
	require.NoError(t, err)
	options.Attachments = limits
	return &rtHarness{t: t, store: store, fixture: f, runtime: mustRuntime(t, store, runtimeBinding(), client, p, options)}
}

func TestRuntimeUploadsAttachmentsAndFreezesBlocks(t *testing.T) {
	f, p := &conformance.MAServer{}, &attachmentProvider{files: map[string]string{"img": "png", "doc": "%PDF"}}
	h := attachmentHarness(t, f, p, core.AttachmentLimits{}, testRuntimeOptions())
	message := runtimeMessage()
	message.Text = "看看"
	message.Attachments = []core.Attachment{{Kind: "image", Key: "img"}, {Kind: "file", Key: "doc", FileName: "report.pdf"}}
	h.enqueue(message)
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("看到了")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "看到了", state.export.Deliveries[0].Body.Text)
	require.Equal(t, []core.InputBlock{{Type: "image", Key: "img", FileID: "file-1", Title: "image", MimeType: "image/png"}, {Type: "document", Key: "doc", FileID: "file-2", Title: "report.pdf", MimeType: "application/pdf"}}, state.turn.Input.Blocks)
	f.Lock()
	defer f.Unlock()
	require.Len(t, f.Uploads, 2)
	require.Equal(t, "user_data", f.Uploads[0].Purpose)
	require.Equal(t, "report.pdf", f.Uploads[1].FileName)
	require.Len(t, f.LastContent, 3, "text + image + document")
	image := f.LastContent[1].(map[string]any)
	require.Equal(t, "image", image["type"])
	require.Equal(t, map[string]any{"type": "file", "file_id": "file-1"}, image["source"])
}

func TestRuntimeUnreadableAttachmentBecomesNotice(t *testing.T) {
	f := &conformance.MAServer{}
	p := &attachmentProvider{
		files: map[string]string{"img": "png", "sheet": "PK-xlsx", "doc": "%PDF"},
		mimes: map[string]string{"sheet": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
	}
	h := attachmentHarness(t, f, p, core.AttachmentLimits{}, testRuntimeOptions())
	message := runtimeMessage()
	message.Text = "帮我看看"
	message.Attachments = []core.Attachment{{Kind: "image", Key: "img"}, {Kind: "file", Key: "sheet", FileName: "预算\n表.xlsx"}, {Kind: "file", Key: "doc", FileName: "report.pdf"}}
	h.enqueue(message)
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("收到")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "收到", state.export.Deliveries[0].Body.Text, "the echo check must accept the notice text")
	require.Equal(t, []core.InputBlock{
		{Type: "image", Key: "img", FileID: "file-1", Title: "image", MimeType: "image/png"},
		{Type: "notice", Key: "sheet", FileID: "file-2", Title: "预算 表.xlsx", MimeType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
		{Type: "document", Key: "doc", FileID: "file-3", Title: "report.pdf", MimeType: "application/pdf"},
	}, state.turn.Input.Blocks)
	f.Lock()
	defer f.Unlock()
	require.Len(t, f.Uploads, 3, "ordinary files upload as agent assets without becoming document input")
	require.Equal(t, "agent", f.Uploads[1].Purpose)
	require.Equal(t, "report.pdf", f.Uploads[2].FileName)
	require.Len(t, f.LastContent, 4, "text + image + notice + document, in attachment order")
	notice := f.LastContent[2].(map[string]any)
	require.Equal(t, "text", notice["type"])
	require.Equal(t, core.InputBlock{Type: "notice", Title: "预算 表.xlsx"}.NoticeText(""), notice["text"])
	require.Contains(t, notice["text"], "预算 表.xlsx")
}

func TestRuntimeAttachmentLimitsRejectWithReply(t *testing.T) {
	for _, tc := range []struct {
		name   string
		count  int
		size   int
		limits core.AttachmentLimits
		want   []string
	}{
		{name: "default count", count: 21, size: 1, want: []string{"附件数量超出限制", "21", "20"}},
		{name: "single file", count: 1, size: 2048, limits: core.AttachmentLimits{MaxBytes: 1024, MaxTotalBytes: 1024}, want: []string{"单个附件大小超出限制", "第 1 个", "2048", "1024"}},
		{name: "total", count: 2, size: 600, limits: core.AttachmentLimits{MaxBytes: 1024, MaxTotalBytes: 1100}, want: []string{"附件总大小超出限制", "1200", "1100"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, p := &conformance.MAServer{}, &attachmentProvider{files: map[string]string{"big": strings.Repeat("x", tc.size)}}
			h := attachmentHarness(t, f, p, tc.limits, testRuntimeOptions())
			message := runtimeMessage()
			for range tc.count {
				message.Attachments = append(message.Attachments, core.Attachment{Kind: "file", Key: "big", FileName: "big.bin"})
			}
			h.enqueue(message)
			h.run()
			state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 6*time.Second)
			require.Equal(t, "attachment_rejected", state.turn.Outcome.ErrorKind)
			for _, want := range tc.want {
				require.Contains(t, state.export.Deliveries[0].Body.Text, want)
			}
			f.Lock()
			require.Zero(t, f.SendCount, "a rejected attachment never reaches the agent")
			if tc.count == 1 || tc.count > 20 {
				require.Empty(t, f.Uploads)
			}
			f.Unlock()
		})
	}
}

func TestRuntimeAttachmentFetchFailureRetriesThenGivesUp(t *testing.T) {
	f, p := &conformance.MAServer{}, &attachmentProvider{fail: errors.New("feishu down")}
	h := attachmentHarness(t, f, p, core.AttachmentLimits{}, deadlineRuntimeOptions())
	message := runtimeMessage()
	message.Attachments = []core.Attachment{{Kind: "image", Key: "img"}}
	h.enqueue(message)
	h.run()
	state := h.waitSettledWithinExternal(t, h.store, core.TurnFailed, core.DeliveryDone, 6*time.Second)
	// 截止前最后一次领取可能在 GetSession 处耗尽时限，结算原因取最后一次未完成阶段。
	require.Contains(t, []string{"attachment_unavailable", "session_lookup_error"}, state.turn.Outcome.ErrorKind)
	require.NotEmpty(t, state.export.Deliveries[0].Body.Text)
	p.fmu.Lock()
	require.Greater(t, p.fetched, 1, "transient fetch failures are retried within the deadline")
	p.fmu.Unlock()
	f.Lock()
	require.Zero(t, f.SendCount)
	f.Unlock()
}

func TestStatusOnlyProgressOmitsToolsButKeepsLifecycle(t *testing.T) {
	f, p := &conformance.MAServer{}, &progressProvider{}
	b := runtimeBinding()
	b.ProgressMode = "status_only"
	b.ShowProgress = false
	h := newHarnessWith(t, f, p, b, testRuntimeOptions())
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, map[string]any{"id": "tool", "type": "agent.tool_use", "name": "private-tool"})
	require.Eventually(t, func() bool { updates, _ := p.snapshot(); return len(updates) > 0 }, time.Second, 5*time.Millisecond)
	f.Lock()
	f.Events = append(f.Events, conformance.AnswerEvents("done")...)
	f.Unlock()
	h.waitSettled(core.TurnDone, core.DeliveryDone, 3*time.Second)
	updates, _ := p.snapshot()
	require.Equal(t, core.ProgressDone, updates[len(updates)-1].State)
	for _, u := range updates {
		require.Empty(t, u.Steps)
	}
}
