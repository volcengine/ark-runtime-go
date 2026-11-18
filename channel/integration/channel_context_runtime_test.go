// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"encoding/xml"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type sourceContextProvider struct {
	runtimeProvider
	details core.MessageContext
	err     error
}

func (*sourceContextProvider) ChannelProvider() string { return "feishu" }
func (p *sourceContextProvider) ResolveMessageContext(context.Context, core.Message) (core.MessageContext, error) {
	return p.details, p.err
}

func TestRuntimeChannelContextSurvivesOptionalLookupFailure(t *testing.T) {
	f := &conformance.MAServer{}
	p := &sourceContextProvider{err: errors.New("contact permission denied")}
	h := newHarnessWith(t, f, p, runtimeBinding(), testRuntimeOptions())
	message := runtimeMessage()
	message.ParentID, message.ThreadID = "om-parent", "omt-thread"
	message.Metadata = &core.MessageMetadata{RootMessageID: "om-root", MessageType: "text", SentAt: "2026-10-27T00:00:00Z"}
	h.enqueue(message)
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	texts := lastContentTexts(f)
	require.Len(t, texts, 2)
	require.Equal(t, "hello", texts[1])
	require.Contains(t, texts[0], `<ChannelContext version="1" provider="feishu"`)
	for _, field := range []string{`sender_open_id="user"`, `chat_id="chat"`, `thread_id="omt-thread"`, `root_message_id="om-root"`, `parent_message_id="om-parent"`, `context_scope="thread"`, `reference_status="unavailable"`} {
		require.Contains(t, texts[0], field)
	}
	var rendered struct {
		Policies []string `xml:"ContextReadingPolicy"`
	}
	require.NoError(t, xml.Unmarshal([]byte("<Context>"+texts[0]+"</Context>"), &rendered))
	require.Len(t, rendered.Policies, 1, "阅读规则必须独立分区")
	require.NotEmpty(t, strings.TrimSpace(rendered.Policies[0]))
	require.LessOrEqual(t, len([]rune(rendered.Policies[0])), 160, "每轮固定说明保持简短")
	f.Lock()
	sends := f.SendCount
	f.Unlock()
	require.Equal(t, 1, sends, "上下文和正文属于同一个输入事件")
	require.Equal(t, texts[0], state.turn.Input.Context)
}

func TestRuntimeChannelContextUpdatesSenderWithinSharedSession(t *testing.T) {
	f := &conformance.MAServer{}
	p := &sourceContextProvider{details: core.MessageContext{ChatMode: "group"}}
	h := newHarnessWith(t, f, p, runtimeBinding(), testRuntimeOptions())
	h.run()
	first := runtimeMessage()
	first.ChatType, first.SenderID = "group", "ou_a"
	h.enqueue(first)
	f.AppendAfterInput(t, conformance.AnswerEvents("first")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	firstSession := state.turn.Session.SessionID
	second := first
	second.MessageID, second.ProviderEventID, second.SenderID = "message-2", "event-2", "ou_b"
	h.enqueue(second)
	require.Eventually(t, func() bool { f.Lock(); defer f.Unlock(); return f.SendCount == 2 }, 4*time.Second, time.Millisecond)
	texts := lastContentTexts(f)
	require.Len(t, texts, 2)
	require.Contains(t, texts[0], `sender_open_id="ou_b"`)
	require.NotContains(t, texts[0], `sender_open_id="ou_a"`)
	f.Lock()
	require.Equal(t, 1, f.CreateCount)
	f.Unlock()
	require.NotEmpty(t, firstSession)
}

func TestRuntimeChannelContextEscapesNamesAndBoundsMentions(t *testing.T) {
	f := &conformance.MAServer{}
	p := &sourceContextProvider{details: core.MessageContext{ChatName: `群"><Injected/>` + "\x00", SenderName: strings.Repeat("名字<&", 1000), ChatMode: "topic"}}
	h := newHarnessWith(t, f, p, runtimeBinding(), testRuntimeOptions())
	t.Cleanup(func() {
		if t.Failed() {
			exported, err := h.store.Export(context.Background())
			for _, turn := range exported.Turns {
				t.Logf("metadata stress turn: phase=%s status=%s wait=%s outcome=%+v err=%v", turn.Phase, turn.Status, turn.WaitReason, turn.Outcome, err)
			}
		}
	})
	m := runtimeMessage()
	m.Metadata = &core.MessageMetadata{}
	for range 100 {
		m.Metadata.Mentions = append(m.Metadata.Mentions, core.Mention{Key: "@_user_2", OpenID: "ou_other", Name: strings.Repeat("<&", 10)})
	}
	h.enqueue(m)
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	texts := lastContentTexts(f)
	require.Len(t, texts, 2)
	require.LessOrEqual(t, len([]rune(texts[0])), 4096)
	require.NotContains(t, texts[0], "<Injected/>")
	var root struct{ XMLName xml.Name }
	require.NoError(t, xml.Unmarshal([]byte("<Context>"+texts[0]+"</Context>"), &root))
	require.Contains(t, texts[0], `truncated="true"`)
}

func TestRuntimeChannelContextFrozenAcrossRetryAndRestart(t *testing.T) {
	f := &conformance.MAServer{SendCode: 429}
	p := &sourceContextProvider{details: core.MessageContext{SenderName: "冻结姓名"}}
	h := newHarnessWith(t, f, p, runtimeBinding(), testRuntimeOptions())
	accepted := h.enqueue(runtimeMessage())
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- h.runtime.Run(ctx) }()
	// 等待明确的 429 拒绝完成持久化；只观察服务端收到请求就取消，会把场景变成未知写入。
	require.Eventually(t, func() bool {
		turn, err := h.store.GetTurn(t.Context(), accepted.Turn.ID)
		return err == nil && turn != nil && turn.Status == core.TurnWaiting && turn.WaitReason == "send_throttled" && turn.Submit.SubmittedAt.IsZero()
	}, 4*time.Second, time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	before, err := h.store.GetTurn(t.Context(), accepted.Turn.ID)
	require.NoError(t, err)
	require.True(t, before.Input.ContextFrozen)
	require.True(t, before.Submit.SubmittedAt.IsZero())
	require.Contains(t, before.Input.Context, "冻结姓名")
	f.Lock()
	attempts := f.SendCount
	f.SendCode = 0
	f.Unlock()
	h.runtime = mustRuntime(t, h.store, runtimeBinding(), newMAForHarness(t, f), &sourceContextProvider{details: core.MessageContext{SenderName: "已改名"}}, testRuntimeOptions())
	h.run()
	f.AppendAfterInputCount(t, attempts+1, conformance.AnswerEvents("restored")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, before.Input.Context, state.turn.Input.Context)
	require.Equal(t, before.Input.Context, lastContentTexts(f)[0])
	require.NotContains(t, state.turn.Input.Context, "已改名")
}
