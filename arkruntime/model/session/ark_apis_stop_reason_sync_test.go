// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Synced from ark-apis by vendor-session. Tests the actual stream decoder.
package session

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestArkAPISStreamStopReasonEventIDs(t *testing.T) {
	for _, kind := range []string{"session.status_idle", "session.thread_status_idle"} {
		for _, ids := range [][]string{{"sevt-a", "sevt-b"}, {"sevt-b"}, {}} {
			payload := map[string]interface{}{
				"type": kind, "id": "sevt-idle",
				"stop_reason": map[string]interface{}{"type": "requires_action", "event_ids": ids},
			}
			if kind == "session.thread_status_idle" {
				payload["session_thread_id"] = "sthr-1"
				payload["agent_name"] = "agent"
			}
			raw, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			event := decodeSessionEvent(raw)
			var reason *ManagedAgentsStopReason
			switch event := event.(type) {
			case *ManagedAgentsSessionStatusIdleEvent:
				reason = event.StopReason
			case *ManagedAgentsSessionThreadStatusIdleEvent:
				reason = event.StopReason
			default:
				t.Fatalf("unexpected event %T", event)
			}
			if reason == nil || reason.Type != "requires_action" || reason.EventIDs == nil || !reflect.DeepEqual(*reason.EventIDs, ids) {
				t.Fatalf("lost pending event IDs: %#v", reason)
			}
			encoded, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			var before, after interface{}
			if err := json.Unmarshal(raw, &before); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(encoded, &after); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("wire shape changed: %s -> %s", raw, encoded)
			}
		}
	}
}

func TestArkAPISLegacyStopReason(t *testing.T) {
	for _, kind := range []string{"end_turn", "retries_exhausted", "future_reason"} {
		reason := ManagedAgentsStopReason{Type: kind}
		encoded, err := json.Marshal(reason)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != `{"type":"`+kind+`"}` {
			t.Fatalf("legacy stop reason changed: %s", encoded)
		}
	}
}
