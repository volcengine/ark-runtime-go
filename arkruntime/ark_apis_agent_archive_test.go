// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/agent"
)

func TestArkApisAgentArchive(t *testing.T) {
	const resourceID = "resource/1"
	const archivedAt = "2026-10-29T00:00:00Z"
	var query string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		body, _ := io.ReadAll(r.Body)
		if len(body) != 0 {
			t.Errorf("expected no request body, got %s", body)
		}
		if r.Method == http.MethodPost {
			if r.URL.EscapedPath() != "/agents/resource%2F1/archive" {
				t.Errorf("path: %s", r.URL.EscapedPath())
			}
			// Build synthetic response data explicitly, independently of SDK codecs.
			response := map[string]interface{}{
				"id":          resourceID,
				"type":        "agent",
				"archived_at": archivedAt,
				"created_at":  archivedAt,
				"updated_at":  archivedAt,
				"name":        "archive test",
				"version":     1,
			}
			if err := json.NewEncoder(w).Encode(response); err != nil {
				t.Error(err)
			}
		} else {
			if r.Method != http.MethodGet || r.URL.Path != "/agents" {
				t.Errorf("unexpected list request: %s %s", r.Method, r.URL)
			}
			query = r.URL.Query().Get("include_archived")
			io.WriteString(w, `{"data": [], "total": 0}`)
		}
	}))
	defer server.Close()
	client := NewClientWithApiKey("test", WithBaseUrl(server.URL))
	result, err := client.ArchiveAgent(context.Background(), resourceID)
	if err != nil {
		t.Fatal(err)
	}
	if result.ID != resourceID || result.ArchivedAt.Value != archivedAt {
		t.Fatalf("result: %+v", result)
	}
	if _, err := client.ArchiveAgent(context.Background(), ""); err == nil {
		t.Fatal("missing ID accepted")
	}
	for _, tc := range []struct {
		params *agent.AgentsListParams
		want   string
	}{
		{nil, ""},
		{&agent.AgentsListParams{}, ""},
		{&agent.AgentsListParams{IncludeArchived: agent.NewOptBool(false)}, "false"},
		{&agent.AgentsListParams{IncludeArchived: agent.NewOptBool(true)}, "true"},
	} {
		if _, err := client.ListAgents(context.Background(), tc.params); err != nil {
			t.Fatal(err)
		}
		if query != tc.want {
			t.Fatalf("query: %q; want %q", query, tc.want)
		}
	}
}
