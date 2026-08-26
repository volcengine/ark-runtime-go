// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package selfhosted

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileToolResultStoreRecoverIgnoresInterruptedTempFile(t *testing.T) {
	store, err := NewFileToolResultStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
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
