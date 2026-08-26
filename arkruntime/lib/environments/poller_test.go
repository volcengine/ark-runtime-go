// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package environments

import (
	"context"
	"io"
	"net/http"
	"testing"

	selfhosted "github.com/volcengine/ark-runtime-go/arkruntime/selfhosted"
)

const (
	testWorkID    = "work_1"
	testSessionID = "sess_1"
)

type fakePollerAPI struct {
	pollItem  *selfhosted.WorkItem
	ackErr    error
	ackCount  int
	stopCount int
	stops     []selfhosted.StopWorkRequest
}

func (f *fakePollerAPI) PollWork(context.Context, selfhosted.PollWorkRequest) (*selfhosted.WorkItem, error) {
	item := f.pollItem
	f.pollItem = nil
	return item, nil
}

func (f *fakePollerAPI) AckWork(context.Context, selfhosted.AckWorkRequest) error {
	f.ackCount++
	return f.ackErr
}

func (f *fakePollerAPI) HeartbeatWork(context.Context, selfhosted.HeartbeatWorkRequest) (*selfhosted.HeartbeatResponse, error) {
	return nil, nil
}

func (f *fakePollerAPI) StopWork(_ context.Context, req selfhosted.StopWorkRequest) error {
	f.stopCount++
	f.stops = append(f.stops, req)
	return nil
}

func (f *fakePollerAPI) GetSession(context.Context, selfhosted.GetSessionRequest) (*selfhosted.Session, error) {
	return nil, nil
}

func (f *fakePollerAPI) ListEvents(context.Context, selfhosted.ListEventsRequest) (*selfhosted.ListEventsResponse, error) {
	return nil, nil
}

func (f *fakePollerAPI) SendEvent(context.Context, selfhosted.SendEventRequest) error {
	return nil
}

func (f *fakePollerAPI) OpenSkill(context.Context, selfhosted.OpenSkillRequest) (*selfhosted.SkillContent, error) {
	return &selfhosted.SkillContent{Body: io.NopCloser(nil)}, nil
}

func TestWorkPollerAcksAndStopsOnClose(t *testing.T) {
	api := &fakePollerAPI{
		pollItem: &selfhosted.WorkItem{
			ID:            testWorkID,
			EnvironmentID: "env_1",
			SessionID:     testSessionID,
		},
	}
	poller := NewWorkPoller(context.Background(), api, WorkPollerOptions{
		EnvironmentID: "env_1",
		WorkerID:      "worker_1",
		Drain:         true,
	})
	if !poller.Next() {
		t.Fatalf("Next returned false: %v", poller.Err())
	}
	if got := poller.Current(); got == nil || got.ID != testWorkID {
		t.Fatalf("current = %+v", got)
	}
	if api.ackCount != 1 {
		t.Fatalf("ack_count=%d", api.ackCount)
	}
	if err := poller.Close(); err != nil {
		t.Fatal(err)
	}
	if api.stopCount != 1 {
		t.Fatalf("stop_count=%d", api.stopCount)
	}
	if api.stops[0].WorkID != testWorkID {
		t.Fatalf("stop request = %+v", api.stops[0])
	}
	if api.stops[0].Force {
		t.Fatalf("poller release should not force stop: %+v", api.stops[0])
	}
}

func TestWorkPollerStopsPreviousBeforeNext(t *testing.T) {
	api := &fakePollerAPI{
		pollItem: &selfhosted.WorkItem{
			ID:            testWorkID,
			EnvironmentID: "env_1",
			SessionID:     testSessionID,
		},
	}
	poller := NewWorkPoller(context.Background(), api, WorkPollerOptions{
		EnvironmentID: "env_1",
		WorkerID:      "worker_1",
		Drain:         true,
	})
	if !poller.Next() {
		t.Fatalf("Next returned false: %v", poller.Err())
	}
	api.pollItem = &selfhosted.WorkItem{
		ID:            "work_2",
		EnvironmentID: "env_1",
		SessionID:     "sess_2",
	}
	if !poller.Next() {
		t.Fatalf("Next returned false: %v", poller.Err())
	}
	if api.stopCount != 1 {
		t.Fatalf("stop_count=%d", api.stopCount)
	}
	if poller.Err() != nil {
		t.Fatalf("poller err = %v", poller.Err())
	}
	if got := poller.Current(); got == nil || got.ID != "work_2" {
		t.Fatalf("current = %+v", got)
	}
}

func TestWorkPollerDoesNotStopWhenAckLosesClaimRace(t *testing.T) {
	api := &fakePollerAPI{
		pollItem: &selfhosted.WorkItem{
			ID:            testWorkID,
			EnvironmentID: "env_1",
			SessionID:     testSessionID,
		},
		ackErr: &selfhosted.APIError{StatusCode: 409, Message: "already claimed"},
	}
	poller := NewWorkPoller(context.Background(), api, WorkPollerOptions{
		EnvironmentID: "env_1",
		WorkerID:      "worker_1",
		Drain:         true,
	})
	if poller.Next() {
		t.Fatal("Next should not claim work after ack failure")
	}
	if api.ackCount != 1 {
		t.Fatalf("ack_count=%d", api.ackCount)
	}
	if api.stopCount != 0 {
		t.Fatalf("stop_count=%d", api.stopCount)
	}
}

func TestPollerConflictIsRecoverable(t *testing.T) {
	if isFatal4xx(&selfhosted.APIError{StatusCode: http.StatusConflict, Message: "temporary conflict"}) {
		t.Fatal("409 conflict must remain retryable")
	}
}

func TestWorkPollerStopsPollingOnPermanentAckFailure(t *testing.T) {
	api := &fakePollerAPI{
		pollItem: &selfhosted.WorkItem{
			ID:            testWorkID,
			EnvironmentID: "env_1",
			SessionID:     testSessionID,
		},
		ackErr: &selfhosted.APIError{StatusCode: 401, Message: "invalid credential"},
	}
	poller := NewWorkPoller(context.Background(), api, WorkPollerOptions{
		EnvironmentID: "env_1",
		WorkerID:      "worker_1",
		Drain:         true,
	})
	if poller.Next() {
		t.Fatal("Next should stop after a permanent ack failure")
	}
	if poller.Err() == nil {
		t.Fatal("expected permanent ack error")
	}
	if api.stopCount != 0 {
		t.Fatalf("stop_count=%d", api.stopCount)
	}
}

func TestWorkPollerTreatsEmptyWorkIDAsEmptyPoll(t *testing.T) {
	api := &fakePollerAPI{
		pollItem: &selfhosted.WorkItem{
			EnvironmentID: "env_1",
			SessionID:     testSessionID,
		},
	}
	poller := NewWorkPoller(context.Background(), api, WorkPollerOptions{
		EnvironmentID: "env_1",
		WorkerID:      "worker_1",
		Drain:         true,
	})
	if poller.Next() {
		t.Fatal("Next should not claim work without id")
	}
	if poller.Err() != nil {
		t.Fatalf("poller err = %v", poller.Err())
	}
	if api.ackCount != 0 || api.stopCount != 0 {
		t.Fatalf("ack_count=%d stop_count=%d", api.ackCount, api.stopCount)
	}
}

func TestWorkPollerNextStoresErr(t *testing.T) {
	api := &fakePollerAPI{}
	poller := NewWorkPoller(context.Background(), api, WorkPollerOptions{
		Drain: true,
	})
	if poller.Next() {
		t.Fatalf("work = %+v", poller.Current())
	}
	if poller.Err() == nil {
		t.Fatal("expected missing environment id error")
	}
}
