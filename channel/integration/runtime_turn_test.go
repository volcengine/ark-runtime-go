// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestRuntimePersistsAnswerAndDeliversOnce(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("answer text")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "answer-answer text", "answer-"+state.export.Deliveries[0].Body.Text)
	require.Equal(t, "reply", state.export.Deliveries[0].ReplyTarget.Mode)
	require.Equal(t, "source-message", state.export.Deliveries[0].ReplyTarget.MessageID)
	f.Lock()
	require.Equal(t, 1, f.CreateCount)
	require.Equal(t, 1, f.SendCount)
	require.Zero(t, f.InterruptCount)
	require.Contains(t, f.LastTypes, "agent.message")
	require.Contains(t, f.LastTypes, "agent.thinking", "thinking is effective activity even when it is not part of the answer")
	require.NotEmpty(t, f.LastCreatedAtGte, "history scan must be bounded by submission time")
	f.Unlock()
	require.Equal(t, 1, p.count())
}

func TestRuntimeAnswerIsLastMessageAfterLastToolUse(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t,
		map[string]any{"id": "narrate", "type": "agent.message", "content": []map[string]string{{"type": "text", "text": "我先查一下"}}},
		map[string]any{"id": "tool", "type": "agent.tool_use", "name": "bash"},
		map[string]any{"id": "result", "type": "agent.tool_result"},
		map[string]any{"id": "final-1", "type": "agent.message", "content": []map[string]string{{"type": "text", "text": "结论一"}}},
		map[string]any{"id": "final-2", "type": "agent.message", "content": []map[string]string{{"type": "text", "text": "结论二"}}},
		map[string]any{"id": "idle", "type": "session.status_idle", "stop_reason": map[string]string{"type": "end_turn"}})
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "结论二", state.export.Deliveries[0].Body.Text)
}

func TestRuntimeIgnoresPreviousTurnAndChildThread(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	f.Events = append(f.Events,
		map[string]any{"id": "old-input", "type": "user.message", "content": []map[string]string{{"type": "text", "text": "old"}}},
		map[string]any{"id": "old-answer", "type": "agent.message", "content": []map[string]string{{"type": "text", "text": "stale"}}},
		map[string]any{"id": "old-idle", "type": "session.status_idle", "stop_reason": map[string]string{"type": "end_turn"}})
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, map[string]any{"id": "child-idle", "type": "session.thread_status_idle", "session_thread_id": "sthr-child", "stop_reason": map[string]string{"type": "end_turn"}})
	time.Sleep(100 * time.Millisecond)
	export, err := h.store.Export(context.Background())
	require.NoError(t, err)
	require.Empty(t, export.Deliveries, "neither the previous turn nor a child thread may settle the current turn")
	f.Lock()
	f.Events = append(f.Events, conformance.AnswerEvents("fresh")...)
	f.Unlock()
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "fresh", state.export.Deliveries[0].Body.Text)
}

func TestRuntimeEndTurnWithoutTextCompletesWithoutReply(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, map[string]any{"id": "idle", "type": "session.status_idle", "stop_reason": map[string]string{"type": "end_turn"}})
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Empty(t, state.export.Deliveries)
	require.Equal(t, 0, p.count())
}

func TestRuntimeGroupTopLevelUsesThreadReplyTarget(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	h := newHarness(t, f, p)
	message := runtimeMessage()
	message.ChatType, message.Mentioned = "group", true
	h.enqueue(message)
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("group answer")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	delivery := state.export.Deliveries[0]
	require.Equal(t, "thread", delivery.ReplyTarget.Mode)
	require.Equal(t, message.MessageID, delivery.ReplyTarget.MessageID)
	require.Equal(t, message.ChatID, delivery.ReplyTarget.ChatID)
}

func TestRuntimeStopWithoutActiveTurnRepliesInSameTransaction(t *testing.T) {
	for _, tc := range []struct {
		name, chatType, threadID string
		mentioned                bool
	}{
		{"p2p", "p2p", "", false},
		{"group", "group", "", true},
		{"group thread", "group", "thread-1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &conformance.MAServer{}
			p := &runtimeCallbackProvider{}
			message := runtimeMessage()
			message.ChatType, message.ThreadID, message.Mentioned = tc.chatType, tc.threadID, tc.mentioned
			message.Text = " /stop "
			store, err := newEmbeddedStore(t)
			require.NoError(t, err)
			p.store = store
			p.messages = []core.Message{message, message}
			p.acks = make(chan runtimeCallbackACK, 2)
			h := harnessWithStore(t, store, f, p)
			h.run()
			for range 2 {
				select {
				case ack := <-p.acks:
					require.NoError(t, ack.err)
					require.Empty(t, ack.turnIDs, "a stop without a target never enters the turn queue")
				case <-time.After(time.Second):
					t.Fatal("provider callback did not ACK")
				}
			}
			state := h.waitAllSettled(core.DeliveryDone, 2*time.Second)
			require.Empty(t, state.turns)
			require.Len(t, state.export.Controls, 1)
			require.Len(t, state.export.Deliveries, 1)
			require.Equal(t, "当前没有进行中的任务。", state.export.Deliveries[0].Body.Text)
			f.Lock()
			require.Zero(t, f.CreateCount)
			require.Zero(t, f.SendCount)
			require.Zero(t, f.InterruptCount)
			f.Unlock()
		})
	}
}

func TestRuntimeStopKeepsExistingConversation(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	h := newHarness(t, f, p)
	seeded := seedConversation(t, h.store, "sesn-existing")
	stop := runtimeMessage()
	stop.ProviderEventID, stop.MessageID = "event-stop", "message-stop"
	stop.Text = "/stop"
	result := h.enqueue(stop)
	require.Nil(t, result.Turn)
	h.run()
	state := h.waitAllSettled(core.DeliveryDone, 2*time.Second)
	require.Equal(t, "当前没有进行中的任务。", state.export.Deliveries[0].Body.Text)
	conversation, err := h.store.GetConversation(context.Background(), seeded.ID)
	require.NoError(t, err)
	require.Equal(t, seeded.Session, conversation.Session, "/stop never touches the session binding")
	f.Lock()
	require.Zero(t, f.CreateCount)
	require.Zero(t, f.SendCount)
	require.Zero(t, f.InterruptCount, "nothing is running, nothing to interrupt")
	f.Unlock()
}

func stopMessage(id string) core.Message {
	m := runtimeMessage()
	m.ProviderEventID, m.MessageID, m.Text = "event-"+id, "message-"+id, "/stop"
	return m
}

func TestRuntimeStopInterruptsRunningTurnAndConversationContinues(t *testing.T) {
	for _, tc := range []struct {
		name, chatType, threadID string
		mentioned                bool
	}{
		{"p2p", "p2p", "", false},
		{"group", "group", "", true},
		{"group thread", "group", "thread-1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, p := &conformance.MAServer{
				RecordInterrupt: true,
				InterruptEvents: []map[string]any{{"id": "stopped-idle", "type": "session.status_idle", "stop_reason": map[string]string{"type": "end_turn"}}},
			}, &runtimeProvider{}
			h := newHarness(t, f, p)
			message := runtimeMessage()
			message.ChatType, message.ThreadID, message.Mentioned = tc.chatType, tc.threadID, tc.mentioned
			first := h.enqueue(message)
			h.run()
			h.phaseEventually(core.PhaseObserve)
			command := message
			command.ProviderEventID, command.MessageID, command.Text = "event-stop", "message-stop", "/stop"
			stop := h.enqueue(command)
			require.NotNil(t, stop.Control)
			require.Nil(t, stop.Turn)
			require.True(t, h.enqueue(command).Duplicate)
			require.Equal(t, first.Turn.ID, stop.Control.TargetTurnID)
			require.Eventually(t, func() bool { turn := h.turnByID(first.Turn.ID); return turn.Status == core.TurnStopped }, time.Second, 5*time.Millisecond)
			require.Eventually(t, func() bool {
				ops, err := h.store.ListRemoteOperations(context.Background(), core.RemoteOperationQuery{TurnID: first.Turn.ID, OpenOnly: true})
				return err == nil && len(ops) == 0
			}, time.Second, 5*time.Millisecond, "matching interrupt echo and idle must release the same-session barrier")

			follow := message
			follow.ProviderEventID, follow.MessageID, follow.Text = "event-follow", "message-follow", "继续"
			h.enqueue(follow)
			f.AppendAfterInputCount(t, 2, conformance.AnswerEvents("follow answer")...)
			state := h.waitAllSettled(core.DeliveryDone, 4*time.Second)
			turns := map[string]core.Turn{}
			for _, turn := range state.turns {
				turns[turn.ID] = turn
			}
			require.Len(t, state.turns, 2, "the stop command never creates a turn")
			require.Len(t, state.export.Controls, 1, "duplicate callbacks share one control")
			stopped := turns[first.Turn.ID]
			require.Equal(t, core.TurnStopped, stopped.Status, "stopping has its own terminal status")
			require.Empty(t, stopped.Outcome.ErrorKind)
			texts := textsBySource(state)
			require.Equal(t, "已停止。", texts[core.DeliverySource{Kind: core.SourceTurn, ID: first.Turn.ID}.String()])
			require.Contains(t, texts, core.DeliverySource{Kind: core.SourceTurn, ID: first.Turn.ID}.String())
			require.Len(t, texts, 2, "the stopped turn and the follow-up each reply once; the control command stays silent")
			for _, control := range state.export.Controls {
				require.Empty(t, texts[control.ID], "a control that hits a target never produces its own delivery")
			}
			f.Lock()
			interrupts, sends, creates := f.InterruptCount, f.SendCount, f.CreateCount
			f.Unlock()
			require.Equal(t, 1, interrupts)
			require.Equal(t, 2, sends, "the stopped input is never resent")
			require.Equal(t, 1, creates, "continuing after stop must preserve the session and its context")
		})
	}
}

func TestRuntimeStopWakesWaitingTurnWithoutInterrupt(t *testing.T) {
	f, p := &conformance.MAServer{SessionStatus: map[string]string{"sesn-existing": "running"}}, &runtimeProvider{}
	h := newHarness(t, f, p)
	seedConversation(t, h.store, "sesn-existing")
	first := h.enqueue(runtimeMessage())
	h.run()
	require.Eventually(t, func() bool {
		turn := h.turnByID(first.Turn.ID)
		return turn.Status == core.TurnWaiting && turn.WaitReason == "session_busy"
	}, 2*time.Second, 5*time.Millisecond)
	h.enqueue(stopMessage("stop"))
	state := h.waitAllSettled(core.DeliveryDone, 4*time.Second)
	require.Len(t, state.export.Deliveries, 1)
	require.Equal(t, "已停止。", state.export.Deliveries[0].Body.Text)
	require.Equal(t, first.Turn.ID, state.export.Deliveries[0].Source.ID)
	f.Lock()
	require.Zero(t, f.SendCount)
	require.Zero(t, f.InterruptCount)
	f.Unlock()
}

func TestRuntimeGroupStopDoesNotStopAnotherThread(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	h := newHarness(t, f, p)
	message := runtimeMessage()
	message.ChatType, message.Mentioned, message.ThreadID = "group", true, "thread-1"
	first := h.enqueue(message)
	h.run()
	h.phaseEventually(core.PhaseObserve)

	command := message
	command.ProviderEventID, command.MessageID, command.Text = "event-stop", "message-stop", "/stop"
	command.ThreadID = "thread-2"
	stop := h.enqueue(command)
	require.NotNil(t, stop.Control)
	require.Empty(t, stop.Control.TargetTurnID)
	require.Nil(t, stop.Turn)
	require.True(t, h.turnByID(first.Turn.ID).StopRequestedAt.IsZero())

	command.ProviderEventID, command.MessageID, command.ThreadID = "event-stop-current", "message-stop-current", "thread-1"
	current := h.enqueue(command)
	require.NotNil(t, current.Control)
	require.Equal(t, first.Turn.ID, current.Control.TargetTurnID)
	state := h.waitAllSettled(core.DeliveryDone, 4*time.Second)
	require.Len(t, state.turns, 1)
	require.Equal(t, core.TurnStopped, state.turns[0].Status)
	texts := textsBySource(state)
	require.Equal(t, "当前没有进行中的任务。", texts[core.DeliverySource{Kind: core.SourceControl, ID: stop.Control.ID}.String()])
	require.Equal(t, "已停止。", texts[core.DeliverySource{Kind: core.SourceTurn, ID: first.Turn.ID}.String()])
	f.Lock()
	interrupts, sends := f.InterruptCount, f.SendCount
	f.Unlock()
	require.Equal(t, 1, interrupts)
	require.Equal(t, 1, sends)
}

func TestRuntimeUnmentionedGroupStopIsPlainText(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	h := newHarness(t, f, p)
	message := runtimeMessage()
	message.ChatType, message.Mentioned, message.Text = "group", false, "/stop"
	h.enqueue(message)
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("agent saw stop")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "agent saw stop", state.export.Deliveries[0].Body.Text)
	f.Lock()
	require.Equal(t, 1, f.SendCount)
	require.Zero(t, f.InterruptCount)
	f.Unlock()
}

func TestRuntimeUnknownSendNeverRepeatsPostAndEndsWithReply(t *testing.T) {
	f, p := &conformance.MAServer{SendCode: 503}, &runtimeProvider{}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "send_unconfirmed", state.turn.Outcome.ErrorKind)
	require.Equal(t, "未能确认这次请求的处理结果，请稍后重试。", state.export.Deliveries[0].Body.Text)
	f.Lock()
	require.Equal(t, 1, f.SendCount, "unknown send must never be re-posted")
	require.Equal(t, 1, f.InterruptCount, "giving up must interrupt a possibly queued input")
	f.Unlock()
}

func TestRuntimeUnknownSendAdoptsInputConsumedLater(t *testing.T) {
	f, p := &conformance.MAServer{SendCode: 503, SendPersistThenError: true}, &runtimeProvider{}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("recovered")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "recovered", state.export.Deliveries[0].Body.Text)
	f.Lock()
	require.Equal(t, 1, f.SendCount)
	f.Unlock()
}

func TestRuntimeUnknownSendAdoptsAfterRestart(t *testing.T) {
	f, p := &conformance.MAServer{SendCode: 503, SendPersistThenError: true}, &runtimeProvider{}
	// Keep the durable waiting checkpoint ineligible until the test restarts.
	// Polling a 10ms wall-clock state races with the next worker claiming it.
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	clock := func() time.Time { return time.Unix(0, now.Load()) }
	store, err := newEmbeddedStore(t, clock)
	require.NoError(t, err)
	h := harnessWithStore(t, store, f, p, func(o *core.RuntimeOptions) { o.Clock = clock })
	h.enqueue(runtimeMessage())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.runtime.Run(ctx) }()
	require.Eventually(t, func() bool {
		export, err := store.Export(context.Background())
		return err == nil && len(export.Turns) == 1 && export.Turns[0].Status == core.TurnWaiting && export.Turns[0].WaitReason == "send_unknown"
	}, 2*time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	f.AppendAfterInput(t, conformance.AnswerEvents("after restart")...)
	now.Add(int64(time.Second))
	restarted := newRuntimeWithStore(t, store, f, p, func(o *core.RuntimeOptions) { o.Clock = clock })
	runUntilCleanup(t, restarted)
	state := waitSettledExternal(t, store, core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "after restart", state.export.Deliveries[0].Body.Text)
	f.Lock()
	require.Equal(t, 1, f.SendCount)
	f.Unlock()
}

func TestRuntimeUnknownCreateAdoptsTaggedSessionWithoutProofFields(t *testing.T) {
	f, p := &conformance.MAServer{CreateCode: 503, CreatePersistThenError: true}, &runtimeProvider{}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("created once")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "created once", state.export.Deliveries[0].Body.Text)
	require.Len(t, state.export.Conversations, 1)
	require.Equal(t, "sesn-demo", state.export.Conversations[0].Session.SessionID)
	f.Lock()
	require.Equal(t, 1, f.CreateCount, "a tagged session must be adopted instead of creating another")
	require.Equal(t, 1, f.SendCount)
	f.Unlock()
}

func TestRuntimeUnknownCreateNeverRepeatsPostAfterGrace(t *testing.T) {
	f, p := &conformance.MAServer{CreateCode: 503}, &runtimeProvider{}
	h := newDeadlineHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 4*time.Second)
	require.Equal(t, "create_unconfirmed", state.turn.Outcome.ErrorKind)
	f.Lock()
	defer f.Unlock()
	require.Equal(t, 1, f.CreateCount, "empty lookup and elapsed grace never prove no creation")
	require.Greater(t, f.ListSessionCount, 1)
	require.Zero(t, f.SendCount)
}

func TestRuntimeUnknownCreateRejectsAmbiguousProof(t *testing.T) {
	f, p := &conformance.MAServer{CreateCode: 503, CreatePersistThenError: true, DuplicateCreateProof: true}, &runtimeProvider{}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "create_proof_conflict", state.turn.Outcome.ErrorKind)
	require.Equal(t, core.SessionNone, state.export.Conversations[0].Session.State)
	f.Lock()
	require.Equal(t, 1, f.CreateCount)
	require.Zero(t, f.SendCount)
	f.Unlock()
}

func TestRuntimeQuotaExceededRetriesWithoutLookup(t *testing.T) {
	f, p := &conformance.MAServer{CreateCode: 429, CreateErrorCode: "SessionQuotaExceeded"}, &runtimeProvider{}
	clock := conformance.NewClock(time.Now())
	options := testRuntimeOptions()
	options.Clock = clock.Now
	options.MessageDeadline = time.Minute
	options.TurnTimeout = 10 * time.Second
	options.LeaseTTL = 20 * time.Second
	binding := runtimeBinding()
	binding.MaxTurnDuration = 10 * time.Second
	h := newHarnessWith(t, f, p, binding, options)
	first := h.enqueue(runtimeMessage())
	h.run()
	// 每次只在明确429已被客户端处理并持久化后推进逻辑时间，避免把在途超时混入拒绝契约。
	for attempt := uint64(1); attempt <= 2; attempt++ {
		require.Eventually(t, func() bool {
			turn, err := h.store.GetTurn(context.Background(), first.Turn.ID)
			if err != nil || turn == nil || turn.Status != core.TurnWaiting || turn.WaitReason != "create_throttled" || turn.Schedule.Attempt != attempt || turn.Submit.WriteAttempts != 0 {
				return false
			}
			ops, err := h.store.ListRemoteOperations(context.Background(), core.RemoteOperationQuery{TurnID: first.Turn.ID})
			return err == nil && len(ops) == 1 && ops[0].Resolved && !ops[0].Counted && ops[0].Proof != nil && ops[0].Proof.Kind == core.RemoteNotAccepted
		}, 2*time.Second, 5*time.Millisecond)
		if attempt == 1 {
			clock.Advance(options.PollInterval)
		}
	}
	f.Lock()
	lookups, creates := f.ListSessionCount, f.CreateCount
	f.Unlock()
	require.Zero(t, lookups, "explicitly confirmed 429s must not trigger lookup")
	require.Equal(t, 2, creates)
	clock.Advance(options.MessageDeadline + time.Second)
	state := h.waitSettledWithinExternal(t, h.store, core.TurnFailed, core.DeliveryDone, 2*time.Second)
	require.Empty(t, state.export.Conversations[0].Session.SessionID)
	require.Zero(t, state.turn.Submit.WriteAttempts)
	require.NotEmpty(t, state.export.Deliveries[0].Body.Text, "the user must be told the request ended")
	f.Lock()
	lookups, creates = f.ListSessionCount, f.CreateCount
	f.Unlock()
	ops, err := h.store.ListRemoteOperations(context.Background(), core.RemoteOperationQuery{OpenOnly: true})
	require.NoError(t, err)
	require.Empty(t, ops)
	require.Zero(t, lookups, "repeated confirmed 429s must never trigger lookup")
	require.Equal(t, 2, creates, "logical deadline must be diagnosed before any new HTTP request")
}

func TestRuntimeTerminatedSessionIsRenewedWithNotice(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status string
		send   int
		code   string
	}{
		{name: "status terminated", status: "terminated"},
		{name: "send forbidden", send: 403, code: "OperationDenied.SessionTerminated"},
		{name: "send not found", send: 404, code: "InvalidSession.NotFound"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, p := &conformance.MAServer{CreateSessionID: "sesn-new"}, &runtimeProvider{}
			if tc.status != "" {
				f.SessionStatus = map[string]string{"sesn-old": tc.status}
			}
			if tc.send != 0 {
				f.SendCodes = []int{tc.send}
				f.SendErrorCodes = []string{tc.code}
			}
			h := newHarness(t, f, p)
			seeded := seedConversation(t, h.store, "sesn-old")
			h.enqueue(runtimeMessage())
			h.run()
			require.Eventually(t, func() bool {
				f.Lock()
				defer f.Unlock()
				return f.CreateCount == 1 && f.InputID != "" && (tc.send == 0 || f.SendCount == 2)
			}, 2*time.Second, 10*time.Millisecond)
			f.Lock()
			f.Events = append(f.Events, conformance.AnswerEvents("new answer")...)
			f.Unlock()
			state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
			require.Equal(t, "之前的会话已结束，已为你开始新的会话。\n\nnew answer", state.export.Deliveries[0].Body.Text)
			conversation, err := h.store.GetConversation(context.Background(), seeded.ID)
			require.NoError(t, err)
			require.Equal(t, "sesn-new", conversation.Session.SessionID)
			require.Equal(t, uint64(2), conversation.Session.Generation)
		})
	}
}

func TestRuntimeRequiresActionRejectedRetainsCleanupBarrier(t *testing.T) {
	f, p := &conformance.MAServer{SendCodes: []int{400}, SendErrorCodes: []string{"RequiresActionRejected"}}, &runtimeProvider{}
	h := newHarness(t, f, p)
	seedConversation(t, h.store, "sesn-demo")
	first := h.enqueue(runtimeMessage())
	h.run()
	state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 3*time.Second)
	require.Equal(t, "requires_action_rejected", state.turn.Outcome.ErrorKind)
	f.Lock()
	sends, interrupts := f.SendCount, f.InterruptCount
	f.Unlock()
	require.Equal(t, 1, sends, "an interrupt response cannot authorize another input on the same session")
	require.Equal(t, 1, interrupts)
	ops, err := h.store.ListRemoteOperations(context.Background(), core.RemoteOperationQuery{TurnID: first.Turn.ID, OpenOnly: true})
	require.NoError(t, err)
	require.Len(t, ops, 1)
	require.True(t, ops[0].CleanupStarted)
	require.True(t, ops[0].Counted)
}

func TestRuntimeRequiresActionTimesOutWithInterruptAndReply(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	h := newDeadlineHarness(t, f, p)
	first := h.enqueue(runtimeMessage())
	next := runtimeMessage()
	next.ProviderEventID, next.MessageID, next.Text = "next-event", "next-message", "next turn"
	h.enqueue(next)
	h.run()
	f.AppendAfterInput(t,
		map[string]any{"id": "tool", "type": "agent.custom_tool_use", "name": "approve"},
		map[string]any{"id": "pause", "type": "session.status_idle", "stop_reason": map[string]any{"type": "requires_action", "event_ids": []string{"tool"}}})
	require.Eventually(t, func() bool {
		turn := h.turnByID(first.Turn.ID)
		return turn.Status == core.TurnFailed && turn.Outcome.ErrorKind == "requires_action_timeout"
	}, 4*time.Second, 10*time.Millisecond)
	f.Lock()
	interrupts := f.InterruptCount
	f.SessionStatus = map[string]string{"sesn-demo": "terminated"}
	f.CreateSessionID = "sesn-second"
	f.Unlock()
	require.GreaterOrEqual(t, interrupts, 1)
	f.AppendAfterInputCount(t, 2, conformance.AnswerEvents("second")...)
	state := h.waitAllSettled(core.DeliveryDone, 4*time.Second)
	require.Contains(t, deliveryTexts(state), "Agent 需要额外确认或工具结果，当前渠道暂不支持该操作，本次请求已结束。")
	require.Contains(t, deliveryTexts(state), "之前的会话已结束，已为你开始新的会话。\n\nsecond")
}

func TestRuntimeRetryingErrorKeepsWaitingForEndTurn(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, map[string]any{"id": "retry", "type": "session.error", "error": map[string]any{"type": "model_overloaded_error", "retry_status": map[string]string{"type": "retrying"}}})
	time.Sleep(80 * time.Millisecond)
	f.Lock()
	f.Events = append(f.Events, conformance.AnswerEvents("after retry")...)
	f.Unlock()
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "after retry", state.export.Deliveries[0].Body.Text)
}

func TestRuntimeTerminalFailuresReplyToUser(t *testing.T) {
	for _, tc := range []struct {
		name  string
		event map[string]any
		kind  string
	}{
		{name: "idle retries exhausted", event: map[string]any{"id": "idle", "type": "session.status_idle", "stop_reason": map[string]string{"type": "retries_exhausted"}}, kind: "agent_failed"},
		{name: "error exhausted", event: map[string]any{"id": "error", "type": "session.error", "error": map[string]any{"retry_status": map[string]string{"type": "exhausted"}}}, kind: "agent_failed"},
		{name: "error terminal", event: map[string]any{"id": "error", "type": "session.error", "error": map[string]any{"retry_status": map[string]string{"type": "terminal"}}}, kind: "agent_failed"},
		{name: "unknown stop reason", event: map[string]any{"id": "idle", "type": "session.status_idle", "stop_reason": map[string]string{"type": "future_state"}}, kind: "unsupported_contract"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, p := &conformance.MAServer{}, &runtimeProvider{}
			h := newHarness(t, f, p)
			h.enqueue(runtimeMessage())
			h.run()
			f.AppendAfterInput(t, tc.event)
			state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 6*time.Second)
			require.Equal(t, tc.kind, state.turn.Outcome.ErrorKind)
			require.NotEmpty(t, state.export.Deliveries[0].Body.Text)
		})
	}
}

func TestRuntimeSilentTurnEndsAtIdleDeadline(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	h := newDeadlineHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	state := h.waitSettledWithinExternal(t, h.store, core.TurnFailed, core.DeliveryDone, 4*time.Second)
	require.Equal(t, "idle_timeout", state.turn.Outcome.ErrorKind)
	require.Equal(t, "任务长时间没有新进展，本次请求已结束，请稍后重试。", state.export.Deliveries[0].Body.Text)
	f.Lock()
	require.Equal(t, 1, f.SendCount)
	require.Equal(t, 1, f.InterruptCount)
	f.Unlock()
}

func TestRuntimeWaitsForBusySessionBeforeSending(t *testing.T) {
	f, p := &conformance.MAServer{SessionStatus: map[string]string{"sesn-demo": "running"}}, &runtimeProvider{}
	h := newHarness(t, f, p)
	seedConversation(t, h.store, "sesn-demo")
	h.enqueue(runtimeMessage())
	h.run()
	time.Sleep(100 * time.Millisecond)
	f.Lock()
	require.Zero(t, f.SendCount, "must not send while the previous turn is still running")
	f.SessionStatus["sesn-demo"] = "idle"
	f.Unlock()
	f.AppendAfterInput(t, conformance.AnswerEvents("after idle")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "after idle", state.export.Deliveries[0].Body.Text)
}

func TestRuntimeRejectionsReplyAndReleaseConversation(t *testing.T) {
	f, p := &conformance.MAServer{SendCode: 400, SendErrorCode: "InvalidPayload"}, &runtimeProvider{}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "send_rejected", state.turn.Outcome.ErrorKind)
	require.Equal(t, "抱歉，请求未被受理，请稍后重试或联系管理员。", state.export.Deliveries[0].Body.Text)
}

func TestRuntimeRejectsMismatchedSendEcho(t *testing.T) {
	f, p := &conformance.MAServer{SendBadEcho: true}, &runtimeProvider{}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "input_conflict", state.turn.Outcome.ErrorKind)
	f.Lock()
	require.Equal(t, 1, f.SendCount)
	f.Unlock()
}

func TestRuntimeDoesNotAdoptCreateResponseWithForeignVault(t *testing.T) {
	f, p := &conformance.MAServer{CreateWrongVault: true}, &runtimeProvider{}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "create_response_conflict", state.turn.Outcome.ErrorKind)
	require.Equal(t, core.SessionNone, state.export.Conversations[0].Session.State)
	f.Lock()
	require.Zero(t, f.SendCount)
	f.Unlock()
}

func TestRuntimeDoesNotAdoptUnroutableSessionID(t *testing.T) {
	f, p := &conformance.MAServer{CreateSessionID: "unexpected-id"}, &runtimeProvider{}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "create_response_conflict", state.turn.Outcome.ErrorKind)
	require.Empty(t, state.export.Conversations[0].Session.SessionID)
}
