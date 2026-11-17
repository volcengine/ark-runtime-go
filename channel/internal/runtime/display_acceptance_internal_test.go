// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func displayJob() *job {
	return &job{Turn: Turn{ID: "turn", BindingID: "binding", InputEventID: "input", Session: TurnSession{SessionID: "sesn-main"}}, binding: Binding{ID: "binding", ProgressMode: "status_only"}}
}

func displayMessage(id, text string) Event {
	return Event{ID: id, Type: "agent.message", Content: []TextBlock{{Type: "text", Text: text}}}
}

func TestCurrentWireAnswerSelection(t *testing.T) {
	input := Event{ID: "input", Type: "user.message"}
	end := Event{ID: "idle", Type: "session.status_idle", StopReason: "end_turn"}
	for _, tc := range []struct {
		name         string
		events       []Event
		status, text string
		waiting      bool
	}{
		{name: "multiple messages select last", events: []Event{{ID: "part", Type: "agent.message", Content: []TextBlock{{Type: "text", Text: "一"}, {Type: "text", Text: "二"}}}, displayMessage("part2", "三"), end}, status: "done", text: "三"},
		{name: "only text after last tool", events: []Event{displayMessage("draft", "过程"), {ID: "use", Type: "agent.tool_use", ToolInput: "private arguments"}, displayMessage("early", "未完成"), {ID: "result", Type: "agent.tool_result", Content: []TextBlock{{Type: "text", Text: "private result"}}}, displayMessage("answer", "答案"), end}, status: "done", text: "答案"},
		{name: "thinking excluded", events: []Event{{ID: "thought", Type: "agent.thinking", Content: []TextBlock{{Type: "text", Text: "private thinking"}}}, displayMessage("answer", "答案"), end}, status: "done", text: "答案"},
		{name: "child text and terminal excluded", events: []Event{{ID: "child", Type: "agent.message", SessionThreadID: "sthr-child", Content: []TextBlock{{Type: "text", Text: "child private"}}}, {ID: "child-end", Type: "session.status_idle", SessionThreadID: "sthr-child", StopReason: "end_turn"}, displayMessage("answer", "答案"), end}, status: "done", text: "答案"},
		{name: "failure does not expose partial answer", events: []Event{displayMessage("partial", "partial private"), {ID: "failure", Type: "session.error", RetryStatus: "terminal"}}, status: "agent_failed"},
		{name: "cancel does not expose partial answer", events: []Event{displayMessage("partial", "partial private"), {ID: "cancel", Type: "session.status_terminated"}}, status: "session_gone"},
		{name: "requires action remains pending", events: []Event{displayMessage("partial", "pending private"), {ID: "pause", Type: "session.status_idle", StopReason: "requires_action"}}, waiting: true},
		{name: "unknown block is diagnostic", events: []Event{{ID: "mixed", Type: "agent.message", Content: []TextBlock{{Type: "text", Text: "safe"}, {Type: "thinking", Text: "private"}}}, end}, status: "unsupported_contract"},
		{name: "unknown stop is diagnostic", events: []Event{displayMessage("partial", "private"), {ID: "unknown", Type: "session.status_idle", StopReason: "future_reason"}}, status: "unsupported_contract"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outcome := judgeTurn(append([]Event{input}, tc.events...), displayJob())
			require.Equal(t, tc.status, outcome.Status)
			require.Equal(t, tc.text, outcome.Text)
			require.Equal(t, tc.waiting, outcome.WaitingAction)
		})
	}
}

func TestAnswerRejectsAmbiguousInputBoundary(t *testing.T) {
	for _, tc := range []struct {
		name   string
		events []Event
		want   string
	}{
		{name: "input belongs to child", events: []Event{{ID: "input", Type: "user.message", SessionThreadID: "sthr-child"}, displayMessage("answer", "private child reply"), {ID: "idle", Type: "session.status_idle", StopReason: "end_turn"}}, want: "input_conflict"},
		{name: "another input before terminal", events: []Event{{ID: "input", Type: "user.message"}, displayMessage("partial", "partial"), {ID: "next-input", Type: "user.message"}, displayMessage("other-answer", "other private reply"), {ID: "idle", Type: "session.status_idle", StopReason: "end_turn"}}, want: "unsupported_contract"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outcome := judgeTurn(tc.events, displayJob())
			require.Equal(t, tc.want, outcome.Status)
			require.Empty(t, outcome.Text)
		})
	}
}

func TestProgressKeepsInputAndTerminalBoundaries(t *testing.T) {
	events := []Event{{ID: "before", Type: "agent.tool_use", ToolName: "private before"}, {ID: "input", Type: "user.message"}, {ID: "own", Type: "agent.tool_use", ToolName: "safe"}, {ID: "child", Type: "agent.tool_use", SessionThreadID: "sthr-child", ToolName: "private child"}, {ID: "idle", Type: "session.status_idle", StopReason: "end_turn"}, {ID: "later", Type: "agent.tool_use", ToolName: "private later"}}
	require.Equal(t, []ProgressStep{{EventID: "own", Kind: "tool", Name: "safe"}}, progressFromEvents(events, displayJob()))
}

type displayProgressProvider struct{ updates []ProgressUpdate }

func (*displayProgressProvider) Start(context.Context, func(context.Context, Message) error) error {
	return nil
}
func (*displayProgressProvider) Deliver(context.Context, Delivery) (string, error) {
	return "answer", nil
}
func (p *displayProgressProvider) ReportProgress(_ context.Context, _ string, u ProgressUpdate) (string, error) {
	p.updates = append(p.updates, u)
	return "card", nil
}

func TestStatusOnlyFastTerminalStillCreatesStatusCard(t *testing.T) {
	for _, state := range []ProgressState{ProgressDone, ProgressFailed, ProgressStopped} {
		t.Run(string(state), func(t *testing.T) {
			p := &displayProgressProvider{}
			j := displayJob()
			j.connection = Connection{Binding: j.binding, Provider: p}
			r := &Runtime{options: RuntimeOptions{Clock: time.Now, DeliveryTimeout: time.Second}, progress: map[string]*progressState{}, connections: map[string]Connection{"binding": j.connection}}
			r.reportProgress(j, []ProgressStep{{Name: "private tool"}}, state, "")
			require.Len(t, p.updates, 1, "即刻完成/失败/停止也必须展示最终状态")
			require.Equal(t, state, p.updates[0].State)
			encoded, err := json.Marshal(p.updates)
			require.NoError(t, err)
			require.NotContains(t, string(encoded), "private tool")
		})
	}
}
