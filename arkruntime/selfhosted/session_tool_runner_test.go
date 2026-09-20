// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package selfhosted

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime/toolset"
)

const runnerTestCallID = "call-id"

type runnerTestAPI struct {
	listCalls int
	listEvent Event
	sent      []Event
}

func (a *runnerTestAPI) PollWork(context.Context, PollWorkRequest) (*WorkItem, error) {
	return nil, nil
}

func (a *runnerTestAPI) AckWork(context.Context, AckWorkRequest) error { return nil }

func (a *runnerTestAPI) HeartbeatWork(context.Context, HeartbeatWorkRequest) (*HeartbeatResponse, error) {
	return nil, nil
}

func (a *runnerTestAPI) StopWork(context.Context, StopWorkRequest) error { return nil }

func (a *runnerTestAPI) GetSession(context.Context, GetSessionRequest) (*Session, error) {
	return nil, nil
}

func (a *runnerTestAPI) ListEvents(context.Context, ListEventsRequest) (*ListEventsResponse, error) {
	a.listCalls++
	if a.listCalls == 1 {
		return nil, errors.New("temporary list failure")
	}
	return &ListEventsResponse{Events: []Event{a.listEvent}}, nil
}

func (a *runnerTestAPI) SendEvent(_ context.Context, req SendEventRequest) error {
	a.sent = append(a.sent, req.Event)
	return nil
}

func (a *runnerTestAPI) OpenSkill(context.Context, OpenSkillRequest) (*SkillContent, error) {
	return nil, nil
}

type runnerTestTool struct {
	calls int
}

func (t *runnerTestTool) Name() string { return "custom" }

func (t *runnerTestTool) Execute(context.Context, json.RawMessage) toolset.Result {
	t.calls++
	return toolset.TextResult("ok")
}

type runnerBlockingTool struct {
	started  chan struct{}
	canceled chan struct{}
}

func (t *runnerBlockingTool) Name() string { return "blocking" }

func (t *runnerBlockingTool) Execute(ctx context.Context, _ json.RawMessage) toolset.Result {
	close(t.started)
	<-ctx.Done()
	close(t.canceled)
	return toolset.ErrorResult(ctx.Err().Error())
}

type runnerNamedBlockingTool struct {
	name     string
	started  chan struct{}
	canceled chan struct{}
}

func (t *runnerNamedBlockingTool) Name() string { return t.name }

func (t *runnerNamedBlockingTool) Execute(ctx context.Context, _ json.RawMessage) toolset.Result {
	close(t.started)
	<-ctx.Done()
	close(t.canceled)
	return toolset.ErrorResult(ctx.Err().Error())
}

type runnerFailingMarkSentStore struct {
	markCalls int
}

type runnerRecordingStore struct {
	discarded []string
	marked    []string
}

func (s *runnerRecordingStore) Recover() (map[string]Event, map[string]bool, error) {
	return nil, nil, nil
}

func (s *runnerRecordingStore) Begin(string, Event) (ToolCallStoreDecision, error) {
	return ToolCallStoreDecision{}, nil
}

func (s *runnerRecordingStore) SaveResult(string, Event) error { return nil }

func (s *runnerRecordingStore) MarkSent(callID string) error {
	s.marked = append(s.marked, callID)
	return nil
}

func (s *runnerRecordingStore) Discard(callID string) error {
	s.discarded = append(s.discarded, callID)
	return nil
}

func (s *runnerFailingMarkSentStore) Recover() (map[string]Event, map[string]bool, error) {
	return nil, nil, nil
}

func (s *runnerFailingMarkSentStore) Begin(string, Event) (ToolCallStoreDecision, error) {
	return ToolCallStoreDecision{}, nil
}

func (s *runnerFailingMarkSentStore) SaveResult(string, Event) error { return nil }

func (s *runnerFailingMarkSentStore) MarkSent(string) error {
	s.markCalls++
	return errors.New("mark sent failed")
}

func TestSessionToolRunnerReconcileRetriesWithoutLosingToolUse(t *testing.T) {
	tool := &runnerTestTool{}
	api := &runnerTestAPI{listEvent: Event{
		ID:              "event-id",
		Type:            EventTypeAgentCustomToolUse,
		Name:            tool.Name(),
		ToolUseID:       runnerTestCallID,
		SessionThreadID: "thread-id",
		Input:           RawJSON(`{}`),
	}}
	runner := NewSessionToolRunner(context.Background(), api, "session-id", SessionToolRunnerOptions{
		CustomTools: map[string]toolset.Tool{tool.Name(): tool},
		Logger:      log.New(io.Discard, "", 0),
		ToolTimeout: time.Second,
	})
	runner.events = make(chan ToolCallResult, 1)
	state := &toolRunnerState{
		runner:         runner,
		processed:      map[string]bool{},
		seen:           map[string]bool{},
		answered:       map[string]bool{},
		pendingResults: map[string]Event{},
		pendingAsk:     map[string]Event{},
		confirmations:  map[string]Event{},
		externalTools:  map[string]Event{},
	}

	if err := state.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	finishRunnerTestExecution(t, state)
	if api.listCalls != 2 || tool.calls != 1 || len(api.sent) != 1 {
		t.Fatalf("list_calls=%d tool_calls=%d sent=%d", api.listCalls, tool.calls, len(api.sent))
	}
	if api.sent[0].CustomToolUseID != runnerTestCallID {
		t.Fatalf("sent event=%+v", api.sent[0])
	}
}

func TestSessionToolRunnerConvertsToolPanicToErrorResult(t *testing.T) {
	runner := NewSessionToolRunner(context.Background(), &runnerTestAPI{}, "session-id", SessionToolRunnerOptions{
		Logger:      log.New(io.Discard, "", 0),
		ToolTimeout: time.Second,
	})
	state := &toolRunnerState{runner: runner}
	result := state.executeWithTimeout(context.Background(), Event{
		ID:        "event-id",
		ToolUseID: runnerTestCallID,
		Name:      "panicking-tool",
	}, func(context.Context) toolset.Result {
		panic("boom")
	})
	if !result.IsError || len(result.Content) != 1 || result.Content[0].Text != "tool execution panicked: boom" {
		t.Fatalf("result=%+v", result)
	}
}

func TestRunnerContentBlocksPreservesStructuredSource(t *testing.T) {
	source := map[string]any{
		"type":       "base64",
		"media_type": "image/png",
		"data":       "aW1hZ2U=",
	}
	blocks := runnerContentBlocks([]toolset.ContentBlock{{
		Type:    "image",
		Source:  source,
		Title:   "result",
		Context: "generated by MCP",
	}})
	if len(blocks) != 1 || blocks[0].Type != "image" || blocks[0].Title != "result" || blocks[0].Context != "generated by MCP" {
		t.Fatalf("unexpected blocks: %+v", blocks)
	}
	if got, ok := blocks[0].Source.(map[string]any); !ok || got["data"] != "aW1hZ2U=" {
		t.Fatalf("unexpected source: %#v", blocks[0].Source)
	}
}

func TestContentBlockMarshalPreservesEmptyText(t *testing.T) {
	raw, err := json.Marshal(ContentBlock{Type: "text"})
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"type":"text","text":""}` {
		t.Fatalf("unexpected text block JSON: %s", raw)
	}
}

func TestSessionToolRunnerReplayDoesNotResetIdleDeadline(t *testing.T) {
	maxIdle := time.Second
	runner := NewSessionToolRunner(context.Background(), &runnerTestAPI{}, "session-id", SessionToolRunnerOptions{
		MaxIdle: &maxIdle,
		Logger:  log.New(io.Discard, "", 0),
	})
	state := &toolRunnerState{
		runner: runner,
		seen:   map[string]bool{"replayed-event": true},
	}
	state.armIdle()
	armedAt := state.idleArmedAt
	if err := state.handleStreamEvent(context.Background(), Event{ID: "replayed-event", Type: "user.message"}); err != nil {
		t.Fatal(err)
	}
	if state.idleArmedAt != armedAt {
		t.Fatalf("idle deadline changed from %s to %s", armedAt, state.idleArmedAt)
	}
}

func TestSessionToolRunnerMarkSentFailureDoesNotRetainDeliveredResult(t *testing.T) {
	api := &runnerTestAPI{}
	store := &runnerFailingMarkSentStore{}
	runner := NewSessionToolRunner(context.Background(), api, "session-id", SessionToolRunnerOptions{
		ResultStore: store,
		Logger:      log.New(io.Discard, "", 0),
	})
	runner.events = make(chan ToolCallResult, 1)
	out := NewUserToolResultEvent(runnerTestCallID, []ContentBlock{{Type: "text", Text: "ok"}}, false, "thread-id")
	state := &toolRunnerState{
		runner:         runner,
		processed:      map[string]bool{},
		answered:       map[string]bool{},
		pendingResults: map[string]Event{runnerTestCallID: out},
	}
	if err := state.sendResult(context.Background(), runnerTestCallID, Event{Name: "bash"}, false, "", out); err != nil {
		t.Fatal(err)
	}
	if len(api.sent) != 1 || store.markCalls != 1 {
		t.Fatalf("sent=%d mark_calls=%d", len(api.sent), store.markCalls)
	}
	if !state.isAnswered(runnerTestCallID) || len(state.pendingResults) != 0 {
		t.Fatalf("answered=%v pending=%v", state.answered, state.pendingResults)
	}
}

func TestSessionToolRunnerFlushMarkSentFailureDoesNotRetainDeliveredResult(t *testing.T) {
	api := &runnerTestAPI{}
	store := &runnerFailingMarkSentStore{}
	runner := NewSessionToolRunner(context.Background(), api, "session-id", SessionToolRunnerOptions{
		ResultStore: store,
		Logger:      log.New(io.Discard, "", 0),
	})
	out := NewUserToolResultEvent(runnerTestCallID, []ContentBlock{{Type: "text", Text: "ok"}}, false, "thread-id")
	state := &toolRunnerState{
		runner:         runner,
		processed:      map[string]bool{},
		answered:       map[string]bool{},
		pendingResults: map[string]Event{runnerTestCallID: out},
	}
	if err := state.flushResults(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(api.sent) != 1 || store.markCalls != 1 {
		t.Fatalf("sent=%d mark_calls=%d", len(api.sent), store.markCalls)
	}
	if !state.isAnswered(runnerTestCallID) || len(state.pendingResults) != 0 {
		t.Fatalf("answered=%v pending=%v", state.answered, state.pendingResults)
	}
}

func TestSessionToolRunnerPermissionDenyDoesNotPostToolResult(t *testing.T) {
	api := &runnerTestAPI{}
	tools, err := toolset.NewDefault(toolset.Options{Workdir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer tools.Close()
	runner := NewSessionToolRunner(context.Background(), api, "session-id", SessionToolRunnerOptions{
		Tools:  tools,
		Logger: log.New(io.Discard, "", 0),
	})
	runner.events = make(chan ToolCallResult, 1)
	state := &toolRunnerState{
		runner:         runner,
		processed:      map[string]bool{},
		answered:       map[string]bool{},
		pendingResults: map[string]Event{},
		pendingAsk:     map[string]Event{},
		confirmations:  map[string]Event{},
		externalTools:  map[string]Event{},
	}
	event := Event{
		ID:                  "event-id",
		Type:                EventTypeAgentToolUse,
		Name:                "read",
		ToolUseID:           runnerTestCallID,
		EvaluatedPermission: PermissionDeny,
	}
	if err := state.handleToolUse(context.Background(), event, false); err != nil {
		t.Fatal(err)
	}
	if len(api.sent) != 0 || !state.isAnswered(runnerTestCallID) {
		t.Fatalf("sent=%d answered=%v", len(api.sent), state.answered)
	}
	result := <-runner.events
	if result.Posted || result.Confirmation != ConfirmationDeny {
		t.Fatalf("result=%+v", result)
	}
}

func TestSessionToolRunnerConflictIsRetryable(t *testing.T) {
	err := &APIError{StatusCode: http.StatusConflict, Message: "temporary conflict"}
	if isFatal4xxStatus(err) {
		t.Fatal("409 conflict must remain retryable")
	}
}

func TestSessionToolRunnerFiltersRecoveredResultsAgainstCurrentBlockers(t *testing.T) {
	tool := &runnerTestTool{}
	api := &runnerTestAPI{}
	store := &runnerRecordingStore{}
	runner := NewSessionToolRunner(context.Background(), api, "session-id", SessionToolRunnerOptions{
		CustomTools: map[string]toolset.Tool{tool.Name(): tool},
		ResultStore: store,
		Logger:      log.New(io.Discard, "", 0),
	})
	runner.events = make(chan ToolCallResult, 1)
	foreign := NewUserCustomToolResultEvent("foreign-call", nil, false, "")
	stale := NewUserCustomToolResultEvent("stale-call", nil, false, "")
	state := &toolRunnerState{
		runner:           runner,
		processed:        map[string]bool{},
		seen:             map[string]bool{},
		answered:         map[string]bool{},
		pendingResults:   map[string]Event{"foreign-call": foreign, "stale-call": stale},
		recoveredResults: map[string]bool{"foreign-call": true, "stale-call": true},
		pendingAsk:       map[string]Event{},
		confirmations:    map[string]Event{},
		externalTools:    map[string]Event{},
	}
	events := []Event{
		{ID: "stale-call", Type: EventTypeAgentCustomToolUse, Name: tool.Name(), Input: RawJSON(`{}`)},
		{ID: "current-call", Type: EventTypeAgentCustomToolUse, Name: tool.Name(), Input: RawJSON(`{}`)},
		{ID: "idle", Type: EventTypeSessionStatusIdle, StopReason: &SessionStopReason{
			Type: SessionStopReasonRequiresAction, EventIDs: []string{"current-call"},
		}},
	}

	if err := state.processListedEvents(context.Background(), events, true); err != nil {
		t.Fatal(err)
	}
	finishRunnerTestExecution(t, state)
	if tool.calls != 1 || len(api.sent) != 1 || api.sent[0].CustomToolUseID != "current-call" {
		t.Fatalf("tool_calls=%d sent=%+v", tool.calls, api.sent)
	}
	if len(state.pendingResults) != 0 || len(state.recoveredResults) != 0 {
		t.Fatalf("pending=%v recovered=%v", state.pendingResults, state.recoveredResults)
	}
	if len(store.discarded) != 2 {
		t.Fatalf("discarded=%v", store.discarded)
	}
}

func TestSessionToolRunnerInterruptCancelsActiveToolWithoutPostingResult(t *testing.T) {
	tool := &runnerBlockingTool{started: make(chan struct{}), canceled: make(chan struct{})}
	api := &runnerTestAPI{}
	store := &runnerRecordingStore{}
	runner := NewSessionToolRunner(context.Background(), api, "session-id", SessionToolRunnerOptions{
		CustomTools: map[string]toolset.Tool{tool.Name(): tool},
		ResultStore: store,
		Logger:      log.New(io.Discard, "", 0),
		ToolTimeout: time.Second,
	})
	runner.events = make(chan ToolCallResult, 1)
	state := newRunnerTestState(runner)
	toolUse := Event{
		ID:              runnerTestCallID,
		Type:            EventTypeAgentCustomToolUse,
		Name:            tool.Name(),
		ToolUseID:       runnerTestCallID,
		SessionThreadID: "thread-id",
		Input:           RawJSON(`{}`),
	}
	if err := state.handleStreamEvent(context.Background(), toolUse); err != nil {
		t.Fatal(err)
	}
	select {
	case <-tool.started:
	case <-time.After(time.Second):
		t.Fatal("tool did not start")
	}
	if err := state.handleStreamEvent(context.Background(), Event{
		ID:              "interrupt-id",
		Type:            EventTypeUserInterrupt,
		SessionThreadID: "thread-id",
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-tool.canceled:
	case <-time.After(time.Second):
		t.Fatal("tool context was not canceled")
	}
	finishRunnerTestExecution(t, state)

	if len(api.sent) != 0 || !state.isAnswered(runnerTestCallID) {
		t.Fatalf("sent=%d answered=%v", len(api.sent), state.answered)
	}
	if len(store.discarded) != 1 || store.discarded[0] != runnerTestCallID {
		t.Fatalf("discarded=%v", store.discarded)
	}
}

func TestSessionToolRunnerInterruptOnlyCancelsTargetThread(t *testing.T) {
	tool := &runnerBlockingTool{started: make(chan struct{}), canceled: make(chan struct{})}
	runner := NewSessionToolRunner(context.Background(), &runnerTestAPI{}, "session-id", SessionToolRunnerOptions{
		CustomTools: map[string]toolset.Tool{tool.Name(): tool},
		Logger:      log.New(io.Discard, "", 0),
		ToolTimeout: time.Second,
	})
	runner.events = make(chan ToolCallResult, 1)
	state := newRunnerTestState(runner)
	toolUse := Event{
		ID:              runnerTestCallID,
		Type:            EventTypeAgentCustomToolUse,
		Name:            tool.Name(),
		ToolUseID:       runnerTestCallID,
		SessionThreadID: "thread-a",
		Input:           RawJSON(`{}`),
	}
	if err := state.handleStreamEvent(context.Background(), toolUse); err != nil {
		t.Fatal(err)
	}
	select {
	case <-tool.started:
	case <-time.After(time.Second):
		t.Fatal("tool did not start")
	}
	if err := state.handleStreamEvent(context.Background(), Event{
		ID:              "other-interrupt",
		Type:            EventTypeUserInterrupt,
		SessionThreadID: "thread-b",
	}); err != nil {
		t.Fatal(err)
	}
	if state.isAnswered(runnerTestCallID) {
		t.Fatal("interrupt for another thread settled the active call")
	}
	select {
	case <-tool.canceled:
		t.Fatal("interrupt for another thread canceled the active call")
	default:
	}

	state.handleInterrupt(Event{Type: EventTypeUserInterrupt, SessionThreadID: "thread-a"})
	finishRunnerTestExecution(t, state)
}

func TestSessionToolRunnerReconcileDoesNotRedispatchInterruptedToolUse(t *testing.T) {
	tool := &runnerTestTool{}
	api := &runnerTestAPI{}
	runner := NewSessionToolRunner(context.Background(), api, "session-id", SessionToolRunnerOptions{
		CustomTools: map[string]toolset.Tool{tool.Name(): tool},
		Logger:      log.New(io.Discard, "", 0),
	})
	runner.events = make(chan ToolCallResult, 1)
	state := newRunnerTestState(runner)
	events := []Event{
		{
			ID:              runnerTestCallID,
			Type:            EventTypeAgentCustomToolUse,
			Name:            tool.Name(),
			ToolUseID:       runnerTestCallID,
			SessionThreadID: "thread-id",
			Input:           RawJSON(`{}`),
		},
		{ID: "interrupt-id", Type: EventTypeUserInterrupt, SessionThreadID: "thread-id"},
		{ID: "idle-id", Type: EventTypeSessionStatusIdle, StopReason: &SessionStopReason{Type: SessionStopReasonEndTurn}},
	}
	if err := state.processListedEvents(context.Background(), events, true); err != nil {
		t.Fatal(err)
	}

	if tool.calls != 0 || len(api.sent) != 0 || !state.isAnswered(runnerTestCallID) {
		t.Fatalf("tool_calls=%d sent=%d answered=%v", tool.calls, len(api.sent), state.answered)
	}
}

func TestSessionToolRunnerListInterruptCancelsToolFromEarlierPoll(t *testing.T) {
	tool := &runnerBlockingTool{started: make(chan struct{}), canceled: make(chan struct{})}
	runner := NewSessionToolRunner(context.Background(), &runnerTestAPI{}, "session-id", SessionToolRunnerOptions{
		CustomTools: map[string]toolset.Tool{tool.Name(): tool},
		Logger:      log.New(io.Discard, "", 0),
		ToolTimeout: time.Second,
	})
	runner.events = make(chan ToolCallResult, 1)
	state := newRunnerTestState(runner)
	toolUse := Event{
		ID: "cross-poll-call", Type: EventTypeAgentCustomToolUse, Name: tool.Name(),
		ToolUseID: "cross-poll-call", SessionThreadID: "thread-id", Input: RawJSON(`{}`),
	}
	if err := state.processListedEvents(context.Background(), []Event{toolUse}, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-tool.started:
	case <-time.After(time.Second):
		t.Fatal("tool did not start")
	}
	interrupt := Event{
		Type: EventTypeUserInterrupt, ProcessedAt: "2026-09-20T00:00:00Z", SessionThreadID: "thread-id",
	}
	if err := state.processListedEvents(context.Background(), []Event{toolUse, interrupt}, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-tool.canceled:
	case <-time.After(time.Second):
		t.Fatal("cross-poll interrupt did not cancel tool")
	}
	finishRunnerTestExecution(t, state)
	if !state.isAnswered("cross-poll-call") {
		t.Fatal("cross-poll interrupt did not settle tool call")
	}
}

func TestSessionToolRunnerSerialQueueInterruptRemovesMatchingToolOnly(t *testing.T) {
	activeTool := &runnerNamedBlockingTool{name: "active", started: make(chan struct{}), canceled: make(chan struct{})}
	queuedTool := &runnerNamedBlockingTool{name: "queued", started: make(chan struct{}), canceled: make(chan struct{})}
	runner := NewSessionToolRunner(context.Background(), &runnerTestAPI{}, "session-id", SessionToolRunnerOptions{
		CustomTools: map[string]toolset.Tool{activeTool.Name(): activeTool, queuedTool.Name(): queuedTool},
		Logger:      log.New(io.Discard, "", 0),
		ToolTimeout: time.Second,
	})
	runner.events = make(chan ToolCallResult, 2)
	state := newRunnerTestState(runner)
	events := []Event{
		{ID: "active-call", Type: EventTypeAgentCustomToolUse, Name: activeTool.Name(), ToolUseID: "active-call", SessionThreadID: "thread-a", Input: RawJSON(`{}`)},
		{ID: "queued-call", Type: EventTypeAgentCustomToolUse, Name: queuedTool.Name(), ToolUseID: "queued-call", SessionThreadID: "thread-b", Input: RawJSON(`{}`)},
	}
	if err := state.processListedEvents(context.Background(), events, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-activeTool.started:
	case <-time.After(time.Second):
		t.Fatal("first tool did not start")
	}
	select {
	case <-queuedTool.started:
		t.Fatal("queued tool started before active tool finished")
	case <-time.After(50 * time.Millisecond):
	}
	state.handleInterrupt(Event{Type: EventTypeUserInterrupt, SessionThreadID: "thread-b"})
	if !state.isAnswered("queued-call") || state.isAnswered("active-call") {
		t.Fatalf("answered=%v", state.answered)
	}
	select {
	case <-activeTool.canceled:
		t.Fatal("interrupt for queued thread canceled active tool")
	default:
	}
	state.handleInterrupt(Event{Type: EventTypeUserInterrupt})
	finishRunnerTestExecution(t, state)
	select {
	case <-queuedTool.started:
		t.Fatal("interrupted queued tool was dispatched")
	default:
	}
}

func TestSessionToolRunnerListEndTurnArmsIdleAfterToolCompletes(t *testing.T) {
	maxIdle := time.Minute
	tool := &runnerTestTool{}
	runner := NewSessionToolRunner(context.Background(), &runnerTestAPI{}, "session-id", SessionToolRunnerOptions{
		CustomTools: map[string]toolset.Tool{tool.Name(): tool},
		Logger:      log.New(io.Discard, "", 0),
		MaxIdle:     &maxIdle,
	})
	runner.events = make(chan ToolCallResult, 1)
	state := newRunnerTestState(runner)
	toolUse := Event{ID: "idle-call", Type: EventTypeAgentCustomToolUse, Name: tool.Name(), ToolUseID: "idle-call", Input: RawJSON(`{}`)}
	if err := state.processListedEvents(context.Background(), []Event{toolUse}, false); err != nil {
		t.Fatal(err)
	}
	idle := Event{ID: "idle-end-turn", Type: EventTypeSessionStatusIdle, StopReason: &SessionStopReason{Type: SessionStopReasonEndTurn}}
	if err := state.processListedEvents(context.Background(), []Event{toolUse, idle}, false); err != nil {
		t.Fatal(err)
	}
	if !state.idleArmPending || !state.idleArmedAt.IsZero() {
		t.Fatalf("pending=%v armed_at=%v", state.idleArmPending, state.idleArmedAt)
	}
	finishRunnerTestExecution(t, state)
	if state.idleArmPending || state.idleArmedAt.IsZero() {
		t.Fatalf("pending=%v armed_at=%v", state.idleArmPending, state.idleArmedAt)
	}
}

func TestSessionToolRunnerListReplayOfAnonymousInterruptDoesNotCancelLaterToolUse(t *testing.T) {
	tool := &runnerBlockingTool{started: make(chan struct{}), canceled: make(chan struct{})}
	api := &runnerTestAPI{}
	runner := NewSessionToolRunner(context.Background(), api, "session-id", SessionToolRunnerOptions{
		CustomTools: map[string]toolset.Tool{tool.Name(): tool},
		Logger:      log.New(io.Discard, "", 0),
		ToolTimeout: time.Second,
	})
	runner.events = make(chan ToolCallResult, 1)
	state := newRunnerTestState(runner)
	events := []Event{
		{
			ID:              "old-call",
			Type:            EventTypeAgentCustomToolUse,
			Name:            tool.Name(),
			ToolUseID:       "old-call",
			SessionThreadID: "thread-id",
			Input:           RawJSON(`{}`),
		},
		{Type: EventTypeUserInterrupt, SessionThreadID: "thread-id"},
		{
			ID:              "new-call",
			Type:            EventTypeAgentCustomToolUse,
			Name:            tool.Name(),
			ToolUseID:       "new-call",
			SessionThreadID: "thread-id",
			Input:           RawJSON(`{}`),
		},
	}
	if err := state.processListedEvents(context.Background(), events, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-tool.started:
	case <-time.After(time.Second):
		t.Fatal("later tool did not start")
	}
	if err := state.processListedEvents(context.Background(), events, false); err != nil {
		t.Fatal(err)
	}
	if err := state.processListedEvents(context.Background(), events, true); err != nil {
		t.Fatal(err)
	}
	if state.isAnswered("new-call") {
		t.Fatal("replayed interrupt canceled a later tool call")
	}
	if _, retained := state.toolUseEvents["old-call"]; retained {
		t.Fatal("answered tool event was retained")
	}
	select {
	case <-tool.canceled:
		t.Fatal("later tool context was canceled by replay")
	default:
	}

	state.handleInterrupt(Event{Type: EventTypeUserInterrupt})
	finishRunnerTestExecution(t, state)
}

func newRunnerTestState(runner *SessionToolRunner) *toolRunnerState {
	return &toolRunnerState{
		runner:              runner,
		processed:           map[string]bool{},
		seen:                map[string]bool{},
		answered:            map[string]bool{},
		pendingResults:      map[string]Event{},
		recoveredResults:    map[string]bool{},
		pendingAsk:          map[string]Event{},
		confirmations:       map[string]Event{},
		externalTools:       map[string]Event{},
		toolUseEvents:       map[string]Event{},
		scheduled:           map[string]bool{},
		executionDone:       make(chan toolExecutionResult, sessionRunnerResultsBuffer),
		sessionToolUses:     map[string]bool{},
		toolUsesSinceStatus: map[string]bool{},
		blockingEventIDs:    map[string]bool{},
	}
}

func finishRunnerTestExecution(t *testing.T, state *toolRunnerState) {
	t.Helper()
	select {
	case result := <-state.executionDone:
		if err := state.finishToolExecution(context.Background(), result); err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("tool execution did not finish")
	}
}

func TestSessionToolRunnerResendsRecoveredCurrentBlockerWithoutExecuting(t *testing.T) {
	tool := &runnerTestTool{}
	api := &runnerTestAPI{}
	store := &runnerRecordingStore{}
	runner := NewSessionToolRunner(context.Background(), api, "session-id", SessionToolRunnerOptions{
		CustomTools: map[string]toolset.Tool{tool.Name(): tool},
		ResultStore: store,
		Logger:      log.New(io.Discard, "", 0),
	})
	runner.events = make(chan ToolCallResult, 1)
	result := NewUserCustomToolResultEvent("current-call", nil, false, "")
	state := &toolRunnerState{
		runner:           runner,
		processed:        map[string]bool{},
		seen:             map[string]bool{},
		answered:         map[string]bool{},
		pendingResults:   map[string]Event{"current-call": result},
		recoveredResults: map[string]bool{"current-call": true},
		pendingAsk:       map[string]Event{},
		confirmations:    map[string]Event{},
		externalTools:    map[string]Event{},
	}

	if err := state.processListedEvents(context.Background(), []Event{
		{ID: "current-call", Type: EventTypeAgentCustomToolUse, Name: tool.Name(), Input: RawJSON(`{}`)},
		{ID: "idle", Type: EventTypeSessionStatusIdle, StopReason: &SessionStopReason{
			Type: SessionStopReasonRequiresAction, EventIDs: []string{"current-call"},
		}},
	}, true); err != nil {
		t.Fatal(err)
	}
	if tool.calls != 0 || len(api.sent) != 1 || len(store.marked) != 1 {
		t.Fatalf("tool_calls=%d sent=%d marked=%v", tool.calls, len(api.sent), store.marked)
	}
	if len(store.discarded) != 0 {
		t.Fatalf("discarded=%v", store.discarded)
	}
}

func TestSessionToolRunnerWaitsToValidateRecoveredResultUntilStatusArrives(t *testing.T) {
	tool := &runnerTestTool{}
	api := &runnerTestAPI{}
	runner := NewSessionToolRunner(context.Background(), api, "session-id", SessionToolRunnerOptions{
		CustomTools: map[string]toolset.Tool{tool.Name(): tool},
		Logger:      log.New(io.Discard, "", 0),
	})
	result := NewUserCustomToolResultEvent("current-call", nil, false, "")
	state := &toolRunnerState{
		runner:           runner,
		processed:        map[string]bool{},
		seen:             map[string]bool{},
		answered:         map[string]bool{},
		pendingResults:   map[string]Event{"current-call": result},
		recoveredResults: map[string]bool{"current-call": true},
		pendingAsk:       map[string]Event{},
		confirmations:    map[string]Event{},
		externalTools:    map[string]Event{},
	}

	if err := state.processListedEvents(context.Background(), []Event{{
		ID: "current-call", Type: EventTypeAgentCustomToolUse, Name: tool.Name(), Input: RawJSON(`{}`),
	}}, true); err != nil {
		t.Fatal(err)
	}
	if tool.calls != 0 || len(api.sent) != 0 || !state.recoveredResults["current-call"] {
		t.Fatalf("tool_calls=%d sent=%d recovered=%v", tool.calls, len(api.sent), state.recoveredResults)
	}
}
