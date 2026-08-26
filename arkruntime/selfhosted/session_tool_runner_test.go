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

type runnerFailingMarkSentStore struct {
	markCalls int
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
		ToolUseID:       "call-id",
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
	if api.listCalls != 2 || tool.calls != 1 || len(api.sent) != 1 {
		t.Fatalf("list_calls=%d tool_calls=%d sent=%d", api.listCalls, tool.calls, len(api.sent))
	}
	if api.sent[0].CustomToolUseID != "call-id" {
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
		ToolUseID: "call-id",
		Name:      "panicking-tool",
	}, func(context.Context) toolset.Result {
		panic("boom")
	})
	if !result.IsError || len(result.Content) != 1 || result.Content[0].Text != "tool execution panicked: boom" {
		t.Fatalf("result=%+v", result)
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
	out := NewUserToolResultEvent("call-id", []ContentBlock{{Type: "text", Text: "ok"}}, false, "thread-id")
	state := &toolRunnerState{
		runner:         runner,
		processed:      map[string]bool{},
		answered:       map[string]bool{},
		pendingResults: map[string]Event{"call-id": out},
	}
	if err := state.sendResult(context.Background(), "call-id", Event{Name: "bash"}, false, "", out); err != nil {
		t.Fatal(err)
	}
	if len(api.sent) != 1 || store.markCalls != 1 {
		t.Fatalf("sent=%d mark_calls=%d", len(api.sent), store.markCalls)
	}
	if !state.isAnswered("call-id") || len(state.pendingResults) != 0 {
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
	out := NewUserToolResultEvent("call-id", []ContentBlock{{Type: "text", Text: "ok"}}, false, "thread-id")
	state := &toolRunnerState{
		runner:         runner,
		processed:      map[string]bool{},
		answered:       map[string]bool{},
		pendingResults: map[string]Event{"call-id": out},
	}
	if err := state.flushResults(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(api.sent) != 1 || store.markCalls != 1 {
		t.Fatalf("sent=%d mark_calls=%d", len(api.sent), store.markCalls)
	}
	if !state.isAnswered("call-id") || len(state.pendingResults) != 0 {
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
		ToolUseID:           "call-id",
		EvaluatedPermission: PermissionDeny,
	}
	if err := state.handleToolUse(context.Background(), event, false); err != nil {
		t.Fatal(err)
	}
	if len(api.sent) != 0 || !state.isAnswered("call-id") {
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
