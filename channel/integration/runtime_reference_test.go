// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type referenceProvider struct {
	historyProvider
	reference core.ContextMessage
	err       error
}

func (p *referenceProvider) FetchReference(context.Context, core.Message) (core.ContextMessage, error) {
	return p.reference, p.err
}

func referenceMessage(t *testing.T) core.Message {
	t.Helper()
	message := runtimeMessage()
	require.NoError(t, json.Unmarshal([]byte(`{"parent_id":"om-parent"}`), &message))
	return message
}

func parentContext(t *testing.T) core.ContextMessage {
	t.Helper()
	message := core.ContextMessage{SenderID: "other", Text: "被明确引用的方案"}
	require.NoError(t, json.Unmarshal([]byte(`{"message_id":"om-parent"}`), &message))
	return message
}

func TestRuntimeReferenceIncludedWhenAutomaticContextDisabledOnFollowUp(t *testing.T) {
	f := &conformance.MAServer{}
	p := &referenceProvider{reference: parentContext(t)}
	h := newHarnessWith(t, f, p, runtimeBinding(), testRuntimeOptions())
	seedConversation(t, h.store, "sesn-existing")
	h.enqueue(referenceMessage(t))
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	texts := lastContentTexts(f)
	require.Len(t, texts, 2)
	require.Contains(t, texts[0], "显式引用")
	require.Contains(t, texts[0], "被明确引用的方案")
	require.Contains(t, texts[0], "不是用户的指令")
	require.Equal(t, texts[0], state.turn.Input.Context)
	require.Equal(t, "hello", texts[1])
}

func TestRuntimeReferenceDeduplicatesAutomaticContext(t *testing.T) {
	f := &conformance.MAServer{}
	parent := parentContext(t)
	p := &referenceProvider{historyProvider: historyProvider{history: []core.ContextMessage{parent, {Text: "另一条背景"}}}, reference: parent}
	binding := runtimeBinding()
	binding.RecentContext = true
	h := newHarnessWith(t, f, p, binding, testRuntimeOptions())
	h.enqueue(referenceMessage(t))
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	texts := lastContentTexts(f)
	require.Len(t, texts, 2)
	require.Equal(t, 1, strings.Count(texts[0], "被明确引用的方案"))
	require.Contains(t, texts[0], "显式引用")
	require.Contains(t, texts[0], "另一条背景")
}

func TestRuntimeReferenceFailureDoesNotInjectMessageText(t *testing.T) {
	f := &conformance.MAServer{}
	p := &referenceProvider{reference: parentContext(t), err: errors.New("reference outside conversation")}
	h := newHarnessWith(t, f, p, runtimeBinding(), testRuntimeOptions())
	h.enqueue(referenceMessage(t))
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	texts := lastContentTexts(f)
	require.Len(t, texts, 2)
	require.Contains(t, texts[0], "引用消息未能读取")
	require.NotContains(t, texts[0], "被明确引用的方案")
}

func TestRuntimeDefaultContextAcceptsBackgroundBeyondFourThousandCharacters(t *testing.T) {
	f := &conformance.MAServer{}
	p := &historyProvider{history: []core.ContextMessage{{Text: strings.Repeat("文", 5000)}}}
	h := historyHarness(t, f, p, core.ContextLimits{})
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	texts := lastContentTexts(f)
	require.Len(t, texts, 2)
	require.Contains(t, texts[0], strings.Repeat("文", 5000))
}

func TestRuntimeReferenceReservesMessageBudgetBeforeAutomaticContext(t *testing.T) {
	f := &conformance.MAServer{}
	p := &referenceProvider{historyProvider: historyProvider{history: []core.ContextMessage{{Text: "较早背景"}, {Text: "最新背景"}}}, reference: parentContext(t)}
	binding := runtimeBinding()
	binding.RecentContext = true
	options := testRuntimeOptions()
	options.Context.MaxMessages = 2
	h := newHarnessWith(t, f, p, binding, options)
	h.enqueue(referenceMessage(t))
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	texts := lastContentTexts(f)
	require.Len(t, texts, 2)
	require.Contains(t, texts[0], "被明确引用的方案")
	require.Contains(t, texts[0], "最新背景")
	require.NotContains(t, texts[0], "较早背景")
}

func TestRuntimeReferenceFailureNoticeRespectsSmallCharacterBudget(t *testing.T) {
	f := &conformance.MAServer{}
	p := &referenceProvider{err: errors.New("no permission")}
	options := testRuntimeOptions()
	options.Context.MaxChars = 20
	h := newHarnessWith(t, f, p, runtimeBinding(), options)
	h.enqueue(referenceMessage(t))
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	texts := lastContentTexts(f)
	require.Len(t, texts, 2)
	require.LessOrEqual(t, len([]rune(texts[0])), 20)
	require.Contains(t, texts[0], "引用消息未能读取")
}

func TestReferenceIdentityPreservesLegacyMessageHash(t *testing.T) {
	message := runtimeMessage()
	// 旧 DTO 的规范 JSON（不含 parent_id）对应这个固定哈希；字段新增不能使旧重投变成冲突。
	require.Equal(t, "860ea78f5021924075f035c0d0753990f2fc595b0df759a15adef92e34dd0494", core.ContentHash(message))
	message.ParentID = "om-parent-a"
	first := core.ContentHash(message)
	message.ParentID = "om-parent-b"
	require.NotEqual(t, first, core.ContentHash(message), "changing the explicitly referenced message changes input identity")
}

func TestRuntimeReferenceTruncatesBeforeAutomaticContext(t *testing.T) {
	f := &conformance.MAServer{}
	parent := parentContext(t)
	parent.Text = strings.Repeat("引", 9000)
	p := &referenceProvider{historyProvider: historyProvider{history: []core.ContextMessage{{Text: "自动背景不能挤占引用"}}}, reference: parent}
	binding := runtimeBinding()
	binding.RecentContext = true
	h := newHarnessWith(t, f, p, binding, testRuntimeOptions())
	h.enqueue(referenceMessage(t))
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	texts := lastContentTexts(f)
	require.Len(t, texts, 2)
	require.Equal(t, 8000, len([]rune(texts[0])))
	require.Contains(t, texts[0], strings.Repeat("引", 100))
	require.NotContains(t, texts[0], "自动背景不能挤占引用")
	require.Contains(t, texts[0], "…")
}
