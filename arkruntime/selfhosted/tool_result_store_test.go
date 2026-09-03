// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package selfhosted

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileToolResultStoreRecoverIgnoresInterruptedTempFile(t *testing.T) {
	workdir := t.TempDir()
	store, err := NewFileToolResultStore(workdir)
	if err != nil {
		t.Fatal(err)
	}
	wantDir := filepath.Join(workdir, ".ma_self_hosted_worker", "tool_ledger")
	if store.dir != wantDir {
		t.Fatalf("store dir = %q, want %q", store.dir, wantDir)
	}
	tempPath := filepath.Join(store.dir, ".tool-result-interrupted")
	if err := os.WriteFile(tempPath, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}

	pending, processed, err := store.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 || len(processed) != 0 {
		t.Fatalf("pending=%v processed=%v", pending, processed)
	}
	if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
		t.Fatalf("temporary record was not removed: %v", err)
	}
}

func TestFileToolResultStoreRecoverUsesPersistedCallID(t *testing.T) {
	store, err := NewFileToolResultStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	event := Event{
		ID:              "event-id",
		Type:            EventTypeAgentToolUse,
		ToolUseID:       "call-id",
		SessionThreadID: "thread-id",
	}
	if _, err := store.Begin("call-id", event); err != nil {
		t.Fatal(err)
	}

	pending, _, err := store.Recover()
	if err != nil {
		t.Fatal(err)
	}
	result, ok := pending["call-id"]
	if !ok {
		t.Fatalf("pending=%v", pending)
	}
	if result.ToolUseID != "call-id" {
		t.Fatalf("tool_use_id=%q", result.ToolUseID)
	}
}

func TestSessionFileToolResultStoreIsolatesSessions(t *testing.T) {
	workdir := t.TempDir()
	first, err := NewSessionFileToolResultStore(workdir, "session-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewSessionFileToolResultStore(workdir, "session-b")
	if err != nil {
		t.Fatal(err)
	}
	wantDir := filepath.Join(workdir, ".ma_self_hosted_worker", "tool_ledger", "session-a")
	if first.dir != wantDir {
		t.Fatalf("store dir = %q, want %q", first.dir, wantDir)
	}
	if _, err := first.Begin("call-id", Event{ID: "event-id", Type: EventTypeAgentToolUse}); err != nil {
		t.Fatal(err)
	}
	pending, processed, err := second.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 || len(processed) != 0 {
		t.Fatalf("pending=%v processed=%v", pending, processed)
	}
}

func TestSessionFileToolResultStoreSanitizesSessionID(t *testing.T) {
	workdir := t.TempDir()
	store, err := NewSessionFileToolResultStore(workdir, "../../outside")
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(workdir, ".ma_self_hosted_worker", "tool_ledger")
	if filepath.Dir(store.dir) != base {
		t.Fatalf("store dir escaped base: %q", store.dir)
	}
}
