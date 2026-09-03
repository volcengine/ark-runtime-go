// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package environments

import "testing"

func TestEnvironmentWorkerUsesConfiguredWorkdir(t *testing.T) {
	root := t.TempDir()
	worker := NewEnvironmentWorker(nil, EnvironmentWorkerOptions{Workdir: root})

	got, err := worker.workdir()
	if err != nil {
		t.Fatal(err)
	}
	if got != root {
		t.Fatalf("workdir = %q, want %q", got, root)
	}
}

func TestHandleItemOptionsBuildsClaimedWork(t *testing.T) {
	got, err := claimedWorkFromOptions(HandleItemOptions{
		WorkID:            testWorkID,
		EnvironmentID:     "env_1",
		SessionID:         testSessionID,
		LatestHeartbeatAt: "2026-08-11T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != testWorkID || got.EnvironmentID != "env_1" || got.SessionID != testSessionID {
		t.Fatalf("claimed work = %+v", got)
	}
	if got.LatestHeartbeatAt != "2026-08-11T00:00:00Z" {
		t.Fatalf("work heartbeat = %+v", got)
	}
}

func TestHandleItemOptionsUsesEnvironmentFallbacks(t *testing.T) {
	t.Setenv("MA_WORK_ID", "work_env")
	t.Setenv("MA_ENVIRONMENT_ID", "env_env")
	t.Setenv("MA_SESSION_ID", "sess_env")
	t.Setenv("MA_LATEST_HEARTBEAT_AT", "2026-08-11T00:00:00Z")

	got, err := claimedWorkFromOptions(HandleItemOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "work_env" || got.EnvironmentID != "env_env" || got.SessionID != "sess_env" {
		t.Fatalf("claimed work = %+v", got)
	}
	if got.LatestHeartbeatAt != "2026-08-11T00:00:00Z" {
		t.Fatalf("work heartbeat = %+v", got)
	}
}

func TestHandleItemOptionsRequiresWorkID(t *testing.T) {
	if _, err := claimedWorkFromOptions(HandleItemOptions{EnvironmentID: "env_1", SessionID: "sess_1"}); err == nil {
		t.Fatal("expected work id error")
	}
}
