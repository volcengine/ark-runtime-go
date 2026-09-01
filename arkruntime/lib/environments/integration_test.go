// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package environments

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	selfhosted "github.com/volcengine/ark-runtime-go/arkruntime/selfhosted"
	"github.com/volcengine/ark-runtime-go/arkruntime/tools/agenttoolset"
	"github.com/volcengine/ark-runtime-go/arkruntime/toolset"
)

type fakeEnvironmentWorkerAPI struct {
	mu        sync.Mutex
	events    []selfhosted.Event
	sent      []selfhosted.Event
	pollItem  *selfhosted.WorkItem
	ackCount  int
	stopCount int
	stops     []selfhosted.StopWorkRequest
	stopErr   error

	heartbeat  func(context.Context, selfhosted.HeartbeatWorkRequest) (*selfhosted.HeartbeatResponse, error)
	getSession func(context.Context, selfhosted.GetSessionRequest) (*selfhosted.Session, error)
	onStop     func()
}

type blockingWorkerTool struct{}

func (*blockingWorkerTool) Name() string { return "blocking" }

func (*blockingWorkerTool) Execute(ctx context.Context, _ json.RawMessage) toolset.Result {
	<-ctx.Done()
	return toolset.ErrorResult(ctx.Err().Error())
}

func (f *fakeEnvironmentWorkerAPI) PollWork(context.Context, selfhosted.PollWorkRequest) (*selfhosted.WorkItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	item := f.pollItem
	f.pollItem = nil
	return item, nil
}

func (f *fakeEnvironmentWorkerAPI) AckWork(context.Context, selfhosted.AckWorkRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ackCount++
	return nil
}

func (f *fakeEnvironmentWorkerAPI) HeartbeatWork(ctx context.Context, req selfhosted.HeartbeatWorkRequest) (*selfhosted.HeartbeatResponse, error) {
	if f.heartbeat != nil {
		return f.heartbeat(ctx, req)
	}
	return &selfhosted.HeartbeatResponse{
		LastHeartbeat: time.Now().UTC().Format(time.RFC3339Nano),
		LeaseExtended: true,
		State:         selfhosted.WorkStateActive,
	}, nil
}

func (f *fakeEnvironmentWorkerAPI) StopWork(_ context.Context, req selfhosted.StopWorkRequest) error {
	f.mu.Lock()
	f.stopCount++
	f.stops = append(f.stops, req)
	onStop := f.onStop
	f.mu.Unlock()
	if onStop != nil {
		onStop()
	}
	return f.stopErr
}

func (f *fakeEnvironmentWorkerAPI) GetSession(ctx context.Context, req selfhosted.GetSessionRequest) (*selfhosted.Session, error) {
	if f.getSession != nil {
		return f.getSession(ctx, req)
	}
	return &selfhosted.Session{ID: req.SessionID}, nil
}

func (f *fakeEnvironmentWorkerAPI) ListEvents(context.Context, selfhosted.ListEventsRequest) (*selfhosted.ListEventsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.events) == 0 {
		return &selfhosted.ListEventsResponse{}, nil
	}
	events := append([]selfhosted.Event(nil), f.events...)
	f.events = nil
	return &selfhosted.ListEventsResponse{Events: events}, nil
}

func (f *fakeEnvironmentWorkerAPI) SendEvent(_ context.Context, req selfhosted.SendEventRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, req.Event)
	return nil
}

func (f *fakeEnvironmentWorkerAPI) OpenSkill(context.Context, selfhosted.OpenSkillRequest) (*selfhosted.SkillContent, error) {
	return &selfhosted.SkillContent{Body: io.NopCloser(strings.NewReader(""))}, nil
}

func TestEnvironmentWorkerRunHandlesPolledWorkInProcess(t *testing.T) {
	root := t.TempDir()
	maxIdle := 30 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api := &fakeEnvironmentWorkerAPI{
		pollItem: newTestWorkItem("work_local", "env_local", "sess_local"),
		events: []selfhosted.Event{
			{
				ID:                  "toolu_local",
				Type:                selfhosted.EventTypeAgentToolUse,
				Name:                "bash",
				Input:               selfhosted.RawJSON(`{"command":"echo local-e2e"}`),
				EvaluatedPermission: selfhosted.PermissionAllow,
			},
			{
				ID:         "evt_idle",
				Type:       selfhosted.EventTypeSessionStatusIdle,
				StopReason: &selfhosted.SessionStopReason{Type: selfhosted.SessionStopReasonEndTurn},
			},
		},
		onStop: cancel,
	}
	w := NewEnvironmentWorker(api, EnvironmentWorkerOptions{
		EnvironmentID: "env_local",
		WorkerID:      "worker_local",
		Workdir:       root,
		MaxIdle:       &maxIdle,
	})
	if err := w.Run(ctx); err != nil {
		t.Fatalf("Run err = %v", err)
	}

	api.mu.Lock()
	defer api.mu.Unlock()
	if api.ackCount != 1 || api.stopCount != 1 {
		t.Fatalf("ack_count=%d stop_count=%d", api.ackCount, api.stopCount)
	}
	if force, ok := api.stops[0].Force.Get(); !ok || !force {
		t.Fatalf("worker stop should be force=true: %+v", api.stops[0])
	}
	if len(api.sent) != 1 {
		t.Fatalf("sent events = %+v", api.sent)
	}
	result := api.sent[0]
	if result.Type != selfhosted.EventTypeUserToolResult || result.ToolUseID != "toolu_local" {
		t.Fatalf("result event = %+v", result)
	}
	if result.IsError == nil || *result.IsError {
		t.Fatalf("result is_error = %v", result.IsError)
	}
	if len(result.Content) != 1 || !strings.Contains(result.Content[0].Text, "local-e2e") {
		t.Fatalf("result content = %+v", result.Content)
	}
}

func TestEnvironmentWorkerForwardsToolTimeout(t *testing.T) {
	maxIdle := 10 * time.Millisecond
	toolTimeout := 20 * time.Millisecond
	tool := &blockingWorkerTool{}
	api := &fakeEnvironmentWorkerAPI{
		events: []selfhosted.Event{
			{
				ID:              "toolu_timeout",
				Type:            selfhosted.EventTypeAgentCustomToolUse,
				Name:            tool.Name(),
				ToolUseID:       "call_timeout",
				Input:           selfhosted.RawJSON(`{}`),
				SessionThreadID: "thread_timeout",
			},
			{
				ID:         "evt_idle",
				Type:       selfhosted.EventTypeSessionStatusIdle,
				StopReason: &selfhosted.SessionStopReason{Type: selfhosted.SessionStopReasonEndTurn},
			},
		},
	}
	worker := NewEnvironmentWorker(api, EnvironmentWorkerOptions{
		EnvironmentID: "env_timeout",
		Workdir:       t.TempDir(),
		MaxIdle:       &maxIdle,
		ToolTimeout:   toolTimeout,
		CustomTools:   map[string]toolset.Tool{tool.Name(): tool},
	})
	if err := worker.HandleItem(context.Background(), HandleItemOptions{
		WorkID:        "work_timeout",
		EnvironmentID: "env_timeout",
		SessionID:     "sess_timeout",
	}); err != nil {
		t.Fatal(err)
	}

	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.sent) != 1 {
		t.Fatalf("sent events = %+v", api.sent)
	}
	result := api.sent[0]
	if result.IsError == nil || !*result.IsError {
		t.Fatalf("result is_error = %v", result.IsError)
	}
	if len(result.Content) != 1 || !strings.Contains(result.Content[0].Text, "tool execution timed out after 20ms") {
		t.Fatalf("result content = %+v", result.Content)
	}
}

func TestEnvironmentWorkerForwardsToolTimeoutToDefaultTools(t *testing.T) {
	maxIdle := 10 * time.Millisecond
	api := &fakeEnvironmentWorkerAPI{
		events: []selfhosted.Event{
			{
				ID:                  "toolu_default_timeout",
				Type:                selfhosted.EventTypeAgentToolUse,
				Name:                "bash",
				Input:               selfhosted.RawJSON(`{"command":"sleep 0.1; printf timeout-forwarded"}`),
				EvaluatedPermission: selfhosted.PermissionAllow,
			},
			{
				ID:         "evt_idle",
				Type:       selfhosted.EventTypeSessionStatusIdle,
				StopReason: &selfhosted.SessionStopReason{Type: selfhosted.SessionStopReasonEndTurn},
			},
		},
	}
	worker := NewEnvironmentWorker(api, EnvironmentWorkerOptions{
		EnvironmentID: "env_default_timeout",
		Workdir:       t.TempDir(),
		MaxIdle:       &maxIdle,
		ToolTimeout:   2 * time.Second,
		ToolContext: &agenttoolset.AgentToolContext{
			ToolTimeout: 20 * time.Millisecond,
		},
	})
	if err := worker.HandleItem(context.Background(), HandleItemOptions{
		WorkID:        "work_default_timeout",
		EnvironmentID: "env_default_timeout",
		SessionID:     "sess_default_timeout",
	}); err != nil {
		t.Fatal(err)
	}

	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.sent) != 1 {
		t.Fatalf("sent events = %+v", api.sent)
	}
	result := api.sent[0]
	if result.IsError == nil || *result.IsError {
		t.Fatalf("result is_error = %v", result.IsError)
	}
	if len(result.Content) != 1 || !strings.Contains(result.Content[0].Text, "timeout-forwarded") {
		t.Fatalf("result content = %+v", result.Content)
	}
}

func TestEnvironmentWorkerStopsWorkOnSessionIdleEvent(t *testing.T) {
	root := t.TempDir()
	maxIdle := 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api := &fakeEnvironmentWorkerAPI{
		pollItem: newTestWorkItem("work_idle", "env_local", "sess_idle"),
		events: []selfhosted.Event{{
			ID:         "evt_idle",
			Type:       selfhosted.EventTypeSessionStatusIdle,
			StopReason: &selfhosted.SessionStopReason{Type: selfhosted.SessionStopReasonEndTurn},
		}},
		onStop: cancel,
	}
	w := NewEnvironmentWorker(api, EnvironmentWorkerOptions{
		EnvironmentID: "env_local",
		WorkerID:      "worker_local",
		Workdir:       root,
		MaxIdle:       &maxIdle,
	})
	if err := w.Run(ctx); err != nil {
		t.Fatalf("Run err = %v", err)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if api.ackCount != 1 || api.stopCount == 0 {
		t.Fatalf("ack_count=%d stop_count=%d", api.ackCount, api.stopCount)
	}
	if got := api.stops[0]; got.WorkID != "work_idle" {
		t.Fatalf("worker stop = %+v", got)
	} else if force, ok := got.Force.Get(); !ok || !force {
		t.Fatalf("worker stop should be force=true: %+v", got)
	}
	if len(api.sent) != 0 {
		t.Fatalf("sent events = %+v", api.sent)
	}
}

func TestEnvironmentWorkerHandleItemTreatsAlreadyStoppedAsResolved(t *testing.T) {
	workdir := t.TempDir()
	maxIdle := 10 * time.Millisecond
	api := &fakeEnvironmentWorkerAPI{
		stopErr: &selfhosted.APIError{StatusCode: http.StatusConflict, Message: "already stopped"},
		events: []selfhosted.Event{{
			ID:         "evt_idle",
			Type:       selfhosted.EventTypeSessionStatusIdle,
			StopReason: &selfhosted.SessionStopReason{Type: selfhosted.SessionStopReasonEndTurn},
		}},
	}
	w := NewEnvironmentWorker(api, EnvironmentWorkerOptions{
		EnvironmentID: "env_local",
		WorkerID:      "worker_local",
		Workdir:       workdir,
		MaxIdle:       &maxIdle,
	})
	err := w.HandleItem(context.Background(), HandleItemOptions{
		WorkID:        "work_local",
		EnvironmentID: "env_local",
		SessionID:     "sess_local",
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestEnvironmentWorkerHeartbeatLeaseLostOnPreconditionFailed(t *testing.T) {
	var got selfhosted.HeartbeatWorkRequest
	api := &fakeEnvironmentWorkerAPI{
		heartbeat: func(_ context.Context, req selfhosted.HeartbeatWorkRequest) (*selfhosted.HeartbeatResponse, error) {
			got = req
			return nil, &selfhosted.APIError{StatusCode: http.StatusPreconditionFailed, Message: "lease lost"}
		},
	}
	w := NewEnvironmentWorker(api, EnvironmentWorkerOptions{
		EnvironmentID: "env_local",
		WorkerID:      "worker_local",
		Workdir:       t.TempDir(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var cause heartbeatStopCause
	w.heartbeatLoop(ctx, claimedWork{ID: "work_local", EnvironmentID: "env_local", SessionID: "sess_local"}, api, cancel, func(c heartbeatStopCause) {
		cause = c
	})
	if cause != heartbeatStopCauseLeaseLost {
		t.Fatalf("heartbeat cause = %q", cause)
	}
	if value, ok := got.ExpectedLastHeartbeat.Get(); !ok || value != selfhosted.ExpectedLastHeartbeatNoHeartbeat {
		t.Fatalf("expected_last_heartbeat=%+v", got.ExpectedLastHeartbeat)
	}
	if value, ok := got.DesiredTTLSeconds.Get(); !ok || value != int64(heartbeatDefault/time.Second) {
		t.Fatalf("desired_ttl_seconds=%+v", got.DesiredTTLSeconds)
	}
}

func TestEnvironmentWorkerSkipsStopAfterLeaseOwnershipLoss(t *testing.T) {
	api := &fakeEnvironmentWorkerAPI{
		heartbeat: func(context.Context, selfhosted.HeartbeatWorkRequest) (*selfhosted.HeartbeatResponse, error) {
			return nil, &selfhosted.APIError{StatusCode: http.StatusPreconditionFailed, Message: "lease lost"}
		},
		getSession: func(ctx context.Context, _ selfhosted.GetSessionRequest) (*selfhosted.Session, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	w := NewEnvironmentWorker(api, EnvironmentWorkerOptions{
		EnvironmentID: "env_local",
		WorkerID:      "worker_local",
		Workdir:       t.TempDir(),
	})
	if err := w.HandleItem(context.Background(), HandleItemOptions{
		WorkID:        "work_local",
		EnvironmentID: "env_local",
		SessionID:     "sess_local",
	}); err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if api.stopCount != 0 {
		t.Fatalf("stop_count=%d stops=%+v", api.stopCount, api.stops)
	}
}

func TestEnvironmentWorkerWaitsForHeartbeatCauseBeforeStopping(t *testing.T) {
	heartbeatStarted := make(chan struct{})
	api := &fakeEnvironmentWorkerAPI{
		heartbeat: func(ctx context.Context, _ selfhosted.HeartbeatWorkRequest) (*selfhosted.HeartbeatResponse, error) {
			close(heartbeatStarted)
			<-ctx.Done()
			return nil, &selfhosted.APIError{StatusCode: http.StatusPreconditionFailed, Message: "lease lost"}
		},
		getSession: func(context.Context, selfhosted.GetSessionRequest) (*selfhosted.Session, error) {
			<-heartbeatStarted
			return nil, &selfhosted.APIError{StatusCode: http.StatusInternalServerError, Message: "session lookup failed"}
		},
	}
	w := NewEnvironmentWorker(api, EnvironmentWorkerOptions{
		EnvironmentID: "env_local",
		WorkerID:      "worker_local",
		Workdir:       t.TempDir(),
	})
	if err := w.HandleItem(context.Background(), HandleItemOptions{
		WorkID:        "work_local",
		EnvironmentID: "env_local",
		SessionID:     "sess_local",
	}); err == nil {
		t.Fatal("expected session lookup failure")
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if api.stopCount != 0 {
		t.Fatalf("stop_count=%d stops=%+v", api.stopCount, api.stops)
	}
}

func TestShouldStopItemOnlyWhileOwnershipIsKnown(t *testing.T) {
	tests := []struct {
		cause heartbeatStopCause
		want  bool
	}{
		{cause: "", want: true},
		{cause: heartbeatStopCauseStopRequested, want: true},
		{cause: heartbeatStopCauseLeaseLost, want: false},
		{cause: heartbeatStopCauseLeaseNotExtended, want: false},
		{cause: heartbeatStopCauseHeartbeatLost, want: false},
		{cause: heartbeatStopCausePermanentFailure, want: false},
	}
	for _, test := range tests {
		if got := shouldStopItem(test.cause); got != test.want {
			t.Fatalf("shouldStopItem(%q)=%v want=%v", test.cause, got, test.want)
		}
	}
}

func TestEnvironmentWorkerHeartbeatStopsOnPermanent4xx(t *testing.T) {
	api := &fakeEnvironmentWorkerAPI{
		heartbeat: func(context.Context, selfhosted.HeartbeatWorkRequest) (*selfhosted.HeartbeatResponse, error) {
			return nil, &selfhosted.APIError{StatusCode: http.StatusUnauthorized, Message: "bad key"}
		},
	}
	w := NewEnvironmentWorker(api, EnvironmentWorkerOptions{
		EnvironmentID: "env_local",
		WorkerID:      "worker_local",
		Workdir:       t.TempDir(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var cause heartbeatStopCause
	w.heartbeatLoop(ctx, claimedWork{ID: "work_local", EnvironmentID: "env_local", SessionID: "sess_local"}, api, cancel, func(c heartbeatStopCause) {
		cause = c
	})
	if cause != heartbeatStopCausePermanentFailure {
		t.Fatalf("heartbeat cause = %q", cause)
	}
	if ctx.Err() == nil {
		t.Fatal("heartbeat should cancel session context")
	}
}

func TestEnvironmentWorkerHeartbeatPrefersStopRequestedStateOverLeaseNotExtended(t *testing.T) {
	api := &fakeEnvironmentWorkerAPI{
		heartbeat: func(context.Context, selfhosted.HeartbeatWorkRequest) (*selfhosted.HeartbeatResponse, error) {
			return &selfhosted.HeartbeatResponse{
				LastHeartbeat: time.Now().UTC().Format(time.RFC3339Nano),
				LeaseExtended: false,
				State:         selfhosted.WorkStateStopping,
			}, nil
		},
	}
	w := NewEnvironmentWorker(api, EnvironmentWorkerOptions{
		EnvironmentID: "env_local",
		WorkerID:      "worker_local",
		Workdir:       t.TempDir(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var cause heartbeatStopCause
	w.heartbeatLoop(ctx, claimedWork{ID: "work_local", EnvironmentID: "env_local", SessionID: "sess_local"}, api, cancel, func(c heartbeatStopCause) {
		cause = c
	})
	if cause != heartbeatStopCauseStopRequested {
		t.Fatalf("heartbeat cause = %q", cause)
	}
	if ctx.Err() == nil {
		t.Fatal("heartbeat should cancel session context")
	}
}

func TestEnvironmentWorkerHeartbeatUsesAnthropicDefaultTTL(t *testing.T) {
	var got selfhosted.HeartbeatWorkRequest
	var requestTimeout time.Duration
	api := &fakeEnvironmentWorkerAPI{
		heartbeat: func(ctx context.Context, req selfhosted.HeartbeatWorkRequest) (*selfhosted.HeartbeatResponse, error) {
			got = req
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("heartbeat request has no deadline")
			}
			requestTimeout = time.Until(deadline)
			return nil, &selfhosted.APIError{StatusCode: http.StatusPreconditionFailed, Message: "lease lost"}
		},
	}
	w := NewEnvironmentWorker(api, EnvironmentWorkerOptions{
		EnvironmentID: "env_local",
		WorkerID:      "worker_local",
		Workdir:       t.TempDir(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.heartbeatLoop(ctx, claimedWork{ID: "work_local", EnvironmentID: "env_local", SessionID: "sess_local"}, api, cancel, nil)
	if value, ok := got.DesiredTTLSeconds.Get(); !ok || value != int64(heartbeatDefault/time.Second) {
		t.Fatalf("desired_ttl_seconds=%+v", got.DesiredTTLSeconds)
	}
	if requestTimeout < heartbeatDefault/2-time.Second || requestTimeout > heartbeatDefault/2 {
		t.Fatalf("heartbeat request timeout=%s", requestTimeout)
	}
}
