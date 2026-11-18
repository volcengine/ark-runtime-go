// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func mountMessage() core.Message {
	m := runtimeMessage()
	m.Text = "帮我处理"
	m.Attachments = []core.Attachment{{Kind: "file", Key: "sheet", FileName: "预算表.xlsx"}}
	return m
}

func mountBinding() core.Binding {
	binding := runtimeBinding()
	binding.FileMounts = true
	return binding
}

// mountHarness 构造开启文件挂载、带自定义夹具的 Runtime。
func mountHarness(t *testing.T, f *conformance.MAServer, p *attachmentProvider) *rtHarness {
	t.Helper()
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	client, err := newTestMA(server.URL, "fixture-key", ma.ClientOptions{RequestTimeout: time.Second, StreamTimeout: time.Second, FilePollInterval: time.Millisecond})
	require.NoError(t, err)
	options := testRuntimeOptions()
	options.Attachments = core.AttachmentLimits{}
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: mountBinding(), MAClient: client, Provider: p}}, options)
	require.NoError(t, err)
	return &rtHarness{t: t, runtime: runtime, store: store, fixture: f, server: server}
}

func TestRuntimeMountsUnreadableFileBeforeSubmit(t *testing.T) {
	f := &conformance.MAServer{}
	p := &attachmentProvider{files: map[string]string{"sheet": "PK-xlsx"}, mimes: map[string]string{"sheet": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}}
	h := mountHarness(t, f, p)
	h.enqueue(mountMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Len(t, state.turn.Input.Mounts, 1)
	mounted := state.turn.Input.Mounts[0]
	require.False(t, mounted.Skipped)
	require.Equal(t, "file-1", mounted.FileID)
	require.Contains(t, mounted.MountPath, "预算表.xlsx")
	require.Equal(t, core.MountedActualPath(mounted.MountPath), mounted.ActualPath)
	require.Equal(t, "agent", f.Uploads[0].Purpose)
	require.Len(t, f.SessionResources["sesn-demo"], 1)
	notice := f.LastContent[1].(map[string]any)
	require.Contains(t, notice["text"], mounted.ActualPath)
}

func TestRuntimeMountUnknownReconcilesWithoutDuplicate(t *testing.T) {
	f := &conformance.MAServer{}
	p := &attachmentProvider{files: map[string]string{"sheet": "PK"}, mimes: map[string]string{"sheet": "application/vnd.ms-excel"}}
	h := mountHarness(t, f, p)
	h.enqueue(mountMessage())
	h.run()
	// 等待第一次挂载完成后模拟“第一次 POST 实际成功但响应丢失”：对账能查到同一挂载。
	require.Eventually(t, func() bool { f.Lock(); defer f.Unlock(); return len(f.SessionResources["sesn-demo"]) == 1 }, 3*time.Second, 5*time.Millisecond)
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Len(t, state.turn.Input.Mounts, 1)
	require.False(t, state.turn.Input.Mounts[0].Skipped)
}

func TestRuntimeMountSkippedWhenBindingDisabled(t *testing.T) {
	f := &conformance.MAServer{}
	p := &attachmentProvider{files: map[string]string{"sheet": "PK"}, mimes: map[string]string{"sheet": "application/vnd.ms-excel"}}
	h := attachmentHarness(t, f, p, core.AttachmentLimits{}, testRuntimeOptions())
	h.enqueue(mountMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.True(t, state.turn.Input.MountsFrozen)
	require.Empty(t, state.turn.Input.Mounts)
	require.Empty(t, f.SessionResources)
	notice := f.LastContent[1].(map[string]any)
	require.NotContains(t, notice["text"], "/mnt/session/uploads/")
}
