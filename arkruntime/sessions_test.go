// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/session"
)

func TestUpgradeSession(t *testing.T) {
	const upgradeTestID = "upgrade-test-id"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/sessions/"+upgradeTestID+"/upgrades" {
			http.NotFound(w, r)
			return
		}

		var body map[string]json.RawMessage
		decodeJSONBody(t, r, &body)
		if got, ok := body["vault_ids"]; !ok || string(got) != "[]" {
			t.Fatalf("vault_ids = %s, present=%v; want explicit []", got, ok)
		}
		if _, ok := body["initial_events"]; ok {
			t.Fatalf("unexpected initial_events in body = %s", body["initial_events"])
		}

		w.Header().Set("Content-Type", "application/json")
		response := map[string]any{
			"id":             upgradeTestID,
			"type":           session.SessionTypeSession,
			"status":         session.SessionStatusUpgrading,
			"environment_id": "environment-test-id",
			"agent":          map[string]any{},
			"created_at":     "2026-10-09T00:00:00Z",
			"updated_at":     "2026-10-09T00:00:01Z",
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	defer server.Close()

	client := NewClientWithApiKey("test-api-key", WithBaseUrl(server.URL))
	got, err := client.UpgradeSession(
		context.Background(),
		upgradeTestID,
		&session.CreateSessionUpgradeRequest{VaultIds: []string{}},
	)
	if err != nil {
		t.Fatalf("UpgradeSession() error = %v", err)
	}
	if got == nil || got.ID != upgradeTestID || got.Status != session.SessionStatusUpgrading {
		t.Fatalf("UpgradeSession() = %+v", got)
	}
}

func TestUpgradeSessionValidation(t *testing.T) {
	client := NewClientWithApiKey("test-api-key", WithBaseUrl("https://example.com"))

	if _, err := client.UpgradeSession(context.Background(), "", &session.CreateSessionUpgradeRequest{}); err == nil || err.Error() != "missing required session_id" {
		t.Fatalf("UpgradeSession() error = %v, want missing required session_id", err)
	}
	if _, err := client.UpgradeSession(context.Background(), "sess-1", nil); err == nil || err.Error() != "missing required request body" {
		t.Fatalf("UpgradeSession() error = %v, want missing required request body", err)
	}
}
