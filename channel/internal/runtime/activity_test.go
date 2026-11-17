// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestActivityOnlyNewWorkRenewsIdleWindow(t *testing.T) {
	start := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	j := &job{Turn: Turn{InputEventID: "input", Session: TurnSession{SessionID: "sesn-main"}, Schedule: Schedule{StartedAt: start}, Activity: &TurnActivity{LastProgressAt: start, Initialized: true}}}
	input := Event{ID: "input", Type: "user.message"}
	tracker := newActivityTracker(j)
	tracker.confirm([]Event{input}, j, start)
	toolAt := start.Add(40 * time.Minute)
	events := []Event{input, {ID: "tool", Type: "agent.tool_use", ProcessedAt: &toolAt}}
	tracker.confirm(events, j, toolAt)
	require.Equal(t, toolAt, tracker.snapshot().LastProgressAt)
	j.Activity = ptrActivity(tracker.snapshot())
	tracker = newActivityTracker(j)
	tracker.confirm(append(events, Event{ID: "heartbeat", Type: "session.status_running"}, Event{ID: "child", Type: "agent.tool_result", SessionThreadID: "sthr-child"}), j, start.Add(70*time.Minute))
	require.Equal(t, toolAt, tracker.snapshot().LastProgressAt)
	require.Len(t, tracker.snapshot().SeenEvents, 1)
}

func ptrActivity(a TurnActivity) *TurnActivity { return &a }

func TestActivityStreamReplayAcrossRestartCannotRenew(t *testing.T) {
	at := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	j := &job{Turn: Turn{InputEventID: "input", Session: TurnSession{SessionID: "sesn-main"}, Schedule: Schedule{StartedAt: at}, Activity: &TurnActivity{LastProgressAt: at, Initialized: true}}}
	tracker := newActivityTracker(j)
	tracker.confirm([]Event{{ID: "input", Type: "user.message"}}, j, at)
	start := Event{Type: "event_start", SessionThreadID: "sthr-main", Preview: &EventPreview{EventID: "thinking", EventType: "agent.thinking"}}
	delta := Event{Type: "event_delta", SessionThreadID: "sthr-main", Preview: &EventPreview{EventID: "thinking", Text: "推理"}}
	tracker.accept(start, at.Add(time.Minute))
	tracker.accept(delta, at.Add(20*time.Minute))
	require.Equal(t, at.Add(20*time.Minute), tracker.snapshot().LastProgressAt)
	j.Activity = ptrActivity(tracker.snapshot())
	tracker = newActivityTracker(j)
	tracker.confirm([]Event{{ID: "input", Type: "user.message"}}, j, at.Add(40*time.Minute))
	tracker.accept(start, at.Add(40*time.Minute))
	tracker.accept(delta, at.Add(40*time.Minute))
	require.Equal(t, at.Add(20*time.Minute), tracker.snapshot().LastProgressAt)
	tracker.accept(delta, at.Add(50*time.Minute))
	require.Equal(t, at.Add(50*time.Minute), tracker.snapshot().LastProgressAt, "only bytes beyond the persisted high water mark count")
}

func TestLegacyActivityUsesEventTimeInsteadOfReadTime(t *testing.T) {
	at := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	j := &job{Turn: Turn{InputEventID: "input", Session: TurnSession{SessionID: "sesn-main"}, Schedule: Schedule{StartedAt: at}}}
	tracker := newActivityTracker(j)
	tracker.confirm([]Event{{ID: "input", Type: "user.message"}, {ID: "old", Type: "agent.tool_use"}}, j, at.Add(2*time.Hour))
	require.Equal(t, at, tracker.snapshot().LastProgressAt)
	recent := at.Add(90 * time.Minute)
	tracker.confirm([]Event{{ID: "input", Type: "user.message"}, {ID: "old", Type: "agent.tool_use"}, {ID: "result", Type: "agent.tool_result", ProcessedAt: &recent}}, j, at.Add(2*time.Hour))
	require.Equal(t, recent, tracker.snapshot().LastProgressAt)
}

func TestLiveActivityContinuationWithoutStartRenewsAfterTakeover(t *testing.T) {
	at := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	j := &job{Turn: Turn{InputEventID: "input", Session: TurnSession{SessionID: "sesn-main"}, Schedule: Schedule{StartedAt: at}, Activity: &TurnActivity{LastProgressAt: at, Initialized: true}}}
	tracker := newActivityTracker(j)
	tracker.confirm([]Event{{ID: "input", Type: "user.message"}}, j, at)
	tracker.accept(Event{Type: "event_delta", SessionThreadID: "sthr-main", Preview: &EventPreview{Live: true, EventID: "live-tail", Text: "新推理"}}, at.Add(50*time.Minute))
	require.Equal(t, at.Add(50*time.Minute), tracker.snapshot().LastProgressAt)
}

func TestLatePrefixActivityIsNotLostByHistoryPosition(t *testing.T) {
	at := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	j := &job{Turn: Turn{InputEventID: "input", Session: TurnSession{SessionID: "sesn-main"}, Schedule: Schedule{StartedAt: at}, Activity: &TurnActivity{LastProgressAt: at, Initialized: true}}}
	input := Event{ID: "input", Type: "user.message"}
	tracker := newActivityTracker(j)
	tracker.confirm([]Event{input, {ID: "a", Type: "agent.tool_use"}, {ID: "b", Type: "agent.tool_result"}}, j, at.Add(10*time.Minute))
	recent := at.Add(50 * time.Minute)
	tracker.confirm([]Event{input, {ID: "late", Type: "agent.tool_use", ProcessedAt: &recent}, {ID: "a", Type: "agent.tool_use"}, {ID: "b", Type: "agent.tool_result"}}, j, recent)
	require.Equal(t, recent, tracker.snapshot().LastProgressAt)
	require.Len(t, tracker.snapshot().SeenEvents, 3)
}

func TestContinuationActivityUsesNewInputWithFewerEvents(t *testing.T) {
	at := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	j := &job{Turn: Turn{InputEventID: "original", Session: TurnSession{SessionID: "sesn-main"}, Schedule: Schedule{StartedAt: at}, Activity: &TurnActivity{SessionID: "sesn-main", InputEventID: "original", Initialized: true, LastProgressAt: at, SeenEvents: map[string]bool{"old-1": true, "old-2": true, "old-3": true}}}}
	j.InputEventID = "continuation"
	tracker := newActivityTracker(j)
	tracker.confirm([]Event{{ID: "original", Type: "user.message"}, {ID: "old-tool", Type: "agent.tool_use"}, {ID: "continuation", Type: "user.message"}, {ID: "new-tool", Type: "agent.tool_use"}}, j, at.Add(50*time.Minute))
	state := tracker.snapshot()
	require.Equal(t, at.Add(50*time.Minute), state.LastProgressAt)
	require.Equal(t, "continuation", state.InputEventID)
	require.Len(t, state.SeenEvents, 1)
}
