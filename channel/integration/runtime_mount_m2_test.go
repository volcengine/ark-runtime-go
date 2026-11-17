// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

// TestRuntimeMountFileNameTraversalRejected 覆盖 M2-02 TestFileNameTraversalRejected：
// 含目录分隔符/穿越片段的附件名不得作为挂载路径，本轮按 mount_rejected 明确失败，
// 且没有产生任何 Session 资源写入。
func TestRuntimeMountFileNameTraversalRejected(t *testing.T) {
	for _, name := range []string{"../secret.csv", "a/b.csv", "a\\b.csv"} {
		t.Run(name, func(t *testing.T) {
			f := &conformance.MAServer{}
			p := &attachmentProvider{files: map[string]string{"evil": "x"}, mimes: map[string]string{"evil": "text/csv"}}
			h := mountHarness(t, f, p)
			m := mountMessage()
			m.Attachments = []core.Attachment{{Kind: "file", Key: "evil", FileName: name}}
			h.enqueue(m)
			h.run()
			state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 6*time.Second)
			require.Equal(t, "mount_rejected", state.turn.Outcome.ErrorKind)
			f.Lock()
			defer f.Unlock()
			require.Empty(t, f.SessionResources)
		})
	}
}

// TestRuntimeMountEveryAttachment 验证所有附件都有工具可读路径，图片与视频保留模型输入块，
// PDF 与 Office 以路径文本提交，避免未知底模模态导致拒绝整轮输入。
func TestRuntimeMountEveryAttachment(t *testing.T) {
	f := &conformance.MAServer{}
	p := &attachmentProvider{files: map[string]string{"img": "png", "sheet": "PK", "pdf": "%PDF", "video": "mp4"}, mimes: map[string]string{"sheet": "application/vnd.ms-excel", "video": "video/mp4"}}
	h := mountHarness(t, f, p)
	m := mountMessage()
	m.Attachments = []core.Attachment{
		{Kind: "image", Key: "img"},
		{Kind: "file", Key: "sheet", FileName: "data.xls"},
		{Kind: "file", Key: "pdf", FileName: "report.pdf"},
		{Kind: "file", Key: "video", FileName: "clip.mp4"},
	}
	h.enqueue(m)
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Len(t, state.turn.Input.Mounts, 4)
	require.Equal(t, "data.xls", state.turn.Input.Blocks[1].Title)
	require.Equal(t, []string{"image", "notice", "document", "video"}, []string{state.turn.Input.Blocks[0].Type, state.turn.Input.Blocks[1].Type, state.turn.Input.Blocks[2].Type, state.turn.Input.Blocks[3].Type})
	texts := strings.Join(lastContentTexts(f), "")
	require.NotContains(t, texts, "当前版本暂不支持直接读取该格式")
	for _, mounted := range state.turn.Input.Mounts {
		require.Contains(t, texts, mounted.ActualPath)
	}
	f.Lock()
	defer f.Unlock()
	require.Len(t, f.SessionResources["sesn-demo"], 4)
	require.Equal(t, "user_data", f.Uploads[0].Purpose)
	require.Equal(t, "user_data", f.Uploads[2].Purpose)
}

// TestRuntimeMountSelfHostedSkips 覆盖 M2-02 TestSelfHostedEnvironmentSkipsMount：
// self_hosted 环境不支持资源挂载，运行时冻结 skipped 记录并沿用说明文本，本轮不失败，
// 也不尝试写任何资源。
func TestRuntimeMountSelfHostedSkips(t *testing.T) {
	f := &conformance.MAServer{EnvironmentType: "self_hosted"}
	p := &attachmentProvider{files: map[string]string{"sheet": "PK"}, mimes: map[string]string{"sheet": "application/vnd.ms-excel"}}
	h := mountHarness(t, f, p)
	h.enqueue(mountMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Len(t, state.turn.Input.Mounts, 1)
	require.True(t, state.turn.Input.Mounts[0].Skipped)
	f.Lock()
	defer f.Unlock()
	require.Empty(t, f.SessionResources)
}

// TestRuntimeMountRecordFrozenWithFileKey 验证挂载冻结记录携带 (message_id, file_key)，
// 用于跨重启对账与排查。
func TestRuntimeMountRecordFrozenWithFileKey(t *testing.T) {
	f := &conformance.MAServer{}
	p := &attachmentProvider{files: map[string]string{"sheet": "PK"}, mimes: map[string]string{"sheet": "application/vnd.ms-excel"}}
	h := mountHarness(t, f, p)
	m := mountMessage()
	m.MessageID = "om-frozen"
	h.enqueue(m)
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Len(t, state.turn.Input.Mounts, 1)
	require.Equal(t, "om-frozen", state.turn.Input.Mounts[0].MessageID)
	require.Equal(t, "sheet", state.turn.Input.Mounts[0].Key)
	raw, err := json.Marshal(state.turn.Input.Mounts[0])
	require.NoError(t, err)
	require.Contains(t, string(raw), `"session_id":"sesn-demo"`)
}

func TestRuntimeMountSameNamesUseAttachmentKeys(t *testing.T) {
	f := &conformance.MAServer{}
	p := &attachmentProvider{files: map[string]string{"first": "A", "second": "B"}, mimes: map[string]string{"first": "text/csv", "second": "text/csv"}}
	h := mountHarness(t, f, p)
	m := mountMessage()
	m.Attachments = []core.Attachment{{Kind: "file", Key: "first", FileName: "data.csv"}, {Kind: "file", Key: "second", FileName: "data.csv"}}
	h.enqueue(m)
	h.run()
	require.Eventually(t, func() bool {
		f.Lock()
		defer f.Unlock()
		return len(f.SessionResources["sesn-demo"]) == 2
	}, 2*time.Second, 5*time.Millisecond, "different attachment keys must not share a mount path")
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Len(t, state.turn.Input.Mounts, 2)
	require.NotEqual(t, state.turn.Input.Mounts[0].MountPath, state.turn.Input.Mounts[1].MountPath)
}

// mountFreezeFaultStore 模拟远端挂载完成后、冻结本地结果前进程退出。
type mountFreezeFaultStore struct {
	core.Store
	fired atomic.Bool
	stop  context.CancelFunc
}

func (s *mountFreezeFaultStore) UpdateTurn(ctx context.Context, lease core.Lease, update core.TurnUpdate) (core.Turn, error) {
	if update.Input.MountsFrozen && s.fired.CompareAndSwap(false, true) {
		s.stop()
		return core.Turn{}, context.DeadlineExceeded
	}
	return s.Store.UpdateTurn(ctx, lease, update)
}

// mountHTTPHistory 记录实际资源请求，并可模拟挂载成功后响应丢失。
type mountHTTPHistory struct {
	fixture *conformance.MAServer
	mu      sync.Mutex
	methods []string
	lose    bool
}

func (h *mountHTTPHistory) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasSuffix(r.URL.Path, "/resources") {
		h.fixture.ServeHTTP(w, r)
		return
	}
	h.mu.Lock()
	h.methods = append(h.methods, r.Method)
	lose := h.lose && r.Method == http.MethodPost
	h.lose = h.lose && !lose
	h.mu.Unlock()
	if lose {
		recorder := httptest.NewRecorder()
		h.fixture.ServeHTTP(recorder, r)
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	h.fixture.ServeHTTP(w, r)
}

func (h *mountHTTPHistory) takeMethods() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	methods := append([]string(nil), h.methods...)
	h.methods = nil
	return methods
}

func TestRuntimeMountFreezeFailureRestartsWithList(t *testing.T) {
	backing, err := newEmbeddedStore(t)
	require.NoError(t, err)
	f := &conformance.MAServer{}
	history := &mountHTTPHistory{fixture: f}
	server := httptest.NewServer(history)
	t.Cleanup(server.Close)
	client, err := newTestMA(server.URL, "fixture-key", ma.ClientOptions{FilePollInterval: time.Millisecond})
	require.NoError(t, err)
	p := &attachmentProvider{files: map[string]string{"sheet": "PK"}, mimes: map[string]string{"sheet": "application/vnd.ms-excel"}}
	options := testRuntimeOptions()
	options.Workers, options.MessageDeadline = 1, 5*time.Second
	connections := []core.Connection{{Binding: mountBinding(), MAClient: client, Provider: p}}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	t.Cleanup(cancel)
	fault := &mountFreezeFaultStore{Store: backing, stop: cancel}
	first, err := core.NewRuntime(fault, connections, options)
	require.NoError(t, err)
	ingested := mustEnqueue(t, first, "binding", mountMessage())
	require.NoError(t, first.Run(ctx))
	before, err := backing.GetTurn(context.Background(), ingested.Turn.ID)
	require.NoError(t, err)
	require.True(t, fault.fired.Load(), "mount freeze not reached: %+v", before)
	require.False(t, before.Input.MountsFrozen)
	history.takeMethods()
	f.Lock()
	require.Zero(t, f.SendCount, "unfrozen paths must never be submitted")
	f.Unlock()
	second, err := core.NewRuntime(backing, connections, options)
	require.NoError(t, err)
	runUntilCleanup(t, second)
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	state := waitLatest(t, backing, core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, []string{http.MethodGet}, history.takeMethods(), "restart must reconcile before issuing another POST")
	require.Len(t, state.turn.Input.Mounts, 1)
	require.Equal(t, "registered-file-1", state.turn.Input.Mounts[0].RegisteredFileID)
	require.True(t, state.turn.Input.Mounts[0].ReconciledByPath)
	require.Contains(t, strings.Join(lastContentTexts(f), ""), state.turn.Input.Mounts[0].ActualPath)
}

func TestRuntimeMountUnknownResponseReconciles(t *testing.T) {
	backing, err := newEmbeddedStore(t)
	require.NoError(t, err)
	f := &conformance.MAServer{}
	history := &mountHTTPHistory{fixture: f, lose: true}
	server := httptest.NewServer(history)
	t.Cleanup(server.Close)
	client, err := newTestMA(server.URL, "fixture-key", ma.ClientOptions{FilePollInterval: time.Millisecond})
	require.NoError(t, err)
	p := &attachmentProvider{files: map[string]string{"sheet": "PK"}, mimes: map[string]string{"sheet": "application/vnd.ms-excel"}}
	runtime, err := core.NewRuntime(backing, []core.Connection{{Binding: mountBinding(), MAClient: client, Provider: p}}, testRuntimeOptions())
	require.NoError(t, err)
	mustEnqueue(t, runtime, "binding", mountMessage())
	runUntilCleanup(t, runtime)
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	state := waitLatest(t, backing, core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, []string{http.MethodGet, http.MethodPost, http.MethodGet}, history.takeMethods())
	require.Len(t, state.turn.Input.Mounts, 1)
	require.Equal(t, "registered-file-1", state.turn.Input.Mounts[0].RegisteredFileID)
	require.True(t, state.turn.Input.Mounts[0].ReconciledByPath)
	f.Lock()
	defer f.Unlock()
	require.Len(t, f.SessionResources["sesn-demo"], 1)
}

func TestRemountOnNewGeneration(t *testing.T) {
	f := &conformance.MAServer{SendCodes: []int{http.StatusNotFound}, SendErrorCodes: []string{"InvalidSession"}}
	p := &attachmentProvider{files: map[string]string{"sheet": "PK"}, mimes: map[string]string{"sheet": "application/vnd.ms-excel"}}
	h := mountHarness(t, f, p)
	var creates atomic.Uint32
	h.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/v3/sessions" {
			f.Lock()
			f.CreateSessionID = fmt.Sprintf("sesn-%d", creates.Add(1))
			f.Unlock()
		}
		f.ServeHTTP(w, r)
	})
	h.enqueue(mountMessage())
	h.run()
	f.AppendAfterInputCount(t, 2, conformance.AnswerEvents("ok")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, uint64(2), state.turn.Session.Generation)
	require.Len(t, state.turn.Input.Mounts, 1)
	require.Equal(t, state.turn.Session.SessionID, state.turn.Input.Mounts[0].SessionID)
	require.Equal(t, "file-1", state.turn.Input.Mounts[0].FileID)
	f.Lock()
	defer f.Unlock()
	require.Len(t, f.Uploads, 1, "a new generation reuses the uploaded file")
	require.Len(t, f.SessionResources, 2, "each generation gets its own mount")
}

// mountlessClient 保留附件与环境读取端口，明确不实现 ResourceMounter。
type mountlessClient struct {
	core.MAClient
	core.FileUploader
	core.EnvironmentReader
	core.FileURLResolver
}

func TestRuntimeMountSelfHostedWithoutResourceMounter(t *testing.T) {
	backing, err := newEmbeddedStore(t)
	require.NoError(t, err)
	f := &conformance.MAServer{EnvironmentType: "self_hosted"}
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	client, err := newTestMA(server.URL, "fixture-key", ma.ClientOptions{FilePollInterval: time.Millisecond})
	require.NoError(t, err)
	p := &attachmentProvider{files: map[string]string{"sheet": "PK"}, mimes: map[string]string{"sheet": "application/vnd.ms-excel"}}
	withoutMounts := mountlessClient{MAClient: client, FileUploader: client, EnvironmentReader: client, FileURLResolver: client}
	runtime, err := core.NewRuntime(backing, []core.Connection{{Binding: mountBinding(), MAClient: withoutMounts, Provider: p}}, testRuntimeOptions())
	require.NoError(t, err)
	mustEnqueue(t, runtime, "binding", mountMessage())
	runUntilCleanup(t, runtime)
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	state := waitLatest(t, backing, core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Len(t, state.turn.Input.Mounts, 1)
	require.True(t, state.turn.Input.Mounts[0].Skipped)
	require.Equal(t, "source-message", state.turn.Input.Mounts[0].MessageID)
	f.Lock()
	defer f.Unlock()
	require.Empty(t, f.SessionResources)
}
