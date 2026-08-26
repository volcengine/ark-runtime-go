// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package environments

import "testing"

func TestHandleItemOptionsBuildsWorkItem(t *testing.T) {
	got, err := workItemFromOptions(HandleItemOptions{
		WorkID:            testWorkID,
		EnvironmentID:     "env_1",
		SessionID:         testSessionID,
		LatestHeartbeatAt: "2026-08-11T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != testWorkID || got.EnvironmentID != "env_1" || got.SessionIDValue() != testSessionID {
		t.Fatalf("work item = %+v", got)
	}
	if got.LatestHeartbeatValue() != "2026-08-11T00:00:00Z" {
		t.Fatalf("work heartbeat = %+v", got)
	}
	if got.Data.Type != "session" || got.Data.ID != testSessionID {
		t.Fatalf("work data = %+v", got.Data)
	}
}

func TestHandleItemOptionsUsesEnvironmentFallbacks(t *testing.T) {
	t.Setenv("MA_WORK_ID", "work_env")
	t.Setenv("MA_ENVIRONMENT_ID", "env_env")
	t.Setenv("MA_SESSION_ID", "sess_env")
	t.Setenv("MA_LATEST_HEARTBEAT_AT", "2026-08-11T00:00:00Z")

	got, err := workItemFromOptions(HandleItemOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "work_env" || got.EnvironmentID != "env_env" || got.SessionIDValue() != "sess_env" {
		t.Fatalf("work item = %+v", got)
	}
	if got.LatestHeartbeatValue() != "2026-08-11T00:00:00Z" {
		t.Fatalf("work heartbeat = %+v", got)
	}
}

func TestHandleItemOptionsRequiresWorkID(t *testing.T) {
	if _, err := workItemFromOptions(HandleItemOptions{EnvironmentID: "env_1", SessionID: "sess_1"}); err == nil {
		t.Fatal("expected work id error")
	}
}
