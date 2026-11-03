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
	if _, err := client.UpgradeSession(context.Background(), testSessionID, nil); err == nil || err.Error() != "missing required request body" {
		t.Fatalf("UpgradeSession() error = %v, want missing required request body", err)
	}
}

func TestListSessionsAcceptsNullArchivedAt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/sessions" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"data":[{
				"id":"sess-1",
				"type":"session",
				"status":"running",
				"environment_id":"env-1",
				"agent":{"id":"agent-1","type":"agent","tools":[]},
				"created_at":"2026-10-23T10:00:00Z",
				"updated_at":"2026-10-23T10:00:00Z",
				"archived_at":null,
				"scene":"default",
				"stats":{"active_seconds":0,"duration_seconds":0}
			},{
				"id":"sess-2",
				"type":"session",
				"status":"running",
				"environment_id":"env-2",
				"agent":{"id":"agent-2","type":"agent","tools":[]},
				"created_at":"2026-10-23T10:00:00Z",
				"updated_at":"2026-10-23T10:00:00Z",
				"scene":"default",
				"stats":{"active_seconds":0,"duration_seconds":0}
			}],
			"next_page":"next"
		}`))
	}))
	defer server.Close()

	client := NewClientWithApiKey("test-api-key", WithBaseUrl(server.URL))
	got, err := client.ListSessions(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListSessions() error = %v", err)
	}
	if len(got.Data) != 2 || got.Data[0].ID != testSessionID || got.Data[1].ID != "sess-2" {
		t.Fatalf("sessions = %+v", got.Data)
	}
	if next, ok := got.NextPage.Get(); !ok || next != "next" {
		t.Fatalf("next_page = %q, set=%v", next, ok)
	}
}
