// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package selfhosted_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/environment"
	"github.com/volcengine/ark-runtime-go/arkruntime/selfhosted"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

const skillArchiveContentType = "application/zip"

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestClientAPIConvertsSessionAndEvents(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "GET /sessions/sess-1":
			_, _ = w.Write([]byte(`{
				"id":"sess-1",
				"type":"session",
				"status":"idle",
				"environment_id":"env-1",
				"agent":{"skills":[{"type":"skill_hub","display_name":"demo","skill_id":"skill-1","version":"v1"}]},
				"created_at":"2026-08-10T00:00:00Z",
				"updated_at":"2026-08-10T00:00:00Z"
			}`))
		case "GET /sessions/sess-1/events":
			_, _ = w.Write([]byte(`{
				"data":[{
					"id":"evt-1",
					"type":"agent.tool_use",
					"session_thread_id":"thread-1",
					"tool_use_id":"toolu-1",
					"name":"bash",
					"input":{"command":"echo hi"},
					"evaluated_permission":"ask"
				}],
				"next_page":"page-2"
			}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	api := selfhosted.NewClientAPI(arkruntime.NewClientWithApiKey("test-api-key", arkruntime.WithBaseUrl(server.URL)))
	session, err := api.GetSession(context.Background(), selfhosted.GetSessionRequest{SessionID: "sess-1"})
	if err != nil {
		t.Fatalf("GetSession() error = %v", err)
	}
	skills := session.SkillRefs()
	if len(skills) != 1 || skills[0].IDValue() != "skill-1" || skills[0].Type != "skill_hub" || skills[0].NameValue() != "demo" || skills[0].Version != "v1" {
		t.Fatalf("skills = %+v", skills)
	}

	events, err := api.ListEvents(context.Background(), selfhosted.ListEventsRequest{
		SessionID: "sess-1",
		Limit:     100,
		Order:     selfhosted.EventListOrderAsc,
	})
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if events.NextPage != "page-2" || len(events.Events) != 1 {
		t.Fatalf("events = %+v", events)
	}
	event := events.Events[0]
	if event.ToolUseID != "toolu-1" || event.EvaluatedPermission != selfhosted.PermissionAsk {
		t.Fatalf("event = %+v", event)
	}
}

func TestClientAPIResolveSkillUsesControlPlaneMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/skills/skill-1" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"id":"skill-1",
			"object":"skill",
			"created_at":1786506774,
			"updated_at":1786506774,
			"name":"canonical-skill-name",
			"display_title":"Canonical skill",
			"source":"skill_hub",
			"latest_version":"1.0.0"
		}`))
	}))
	defer server.Close()

	api := selfhosted.NewClientAPI(arkruntime.NewClientWithApiKey("test-api-key", arkruntime.WithBaseUrl(server.URL)))
	resolved, err := api.ResolveSkill(context.Background(), selfhosted.SkillRef{SkillID: "skill-1"})
	if err != nil {
		t.Fatalf("ResolveSkill() error = %v", err)
	}
	if resolved.Name != "canonical-skill-name" || resolved.DisplayName != "Canonical skill" || resolved.Type != "skill_hub" || resolved.Version != "1.0.0" {
		t.Fatalf("resolved skill = %+v", resolved)
	}
}

func TestClientAPIOpenSkillHubUsesMetadataAndVersionedDownload(t *testing.T) {
	const archive = "skill-hub-zip"
	var requests []string
	httpClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if got := req.Header.Get("Authorization"); got != "" {
				t.Fatalf("Authorization = %q", got)
			}
			requests = append(requests, req.URL.String())
			body := ""
			contentType := "application/json"
			switch {
			case req.URL.Path == "/v1/skills" && req.URL.Query().Get("skillIds") == "skill-1":
				body = `{"Skills":[{"Id":"other-skill","Slug":"wrong/slug"},{"Id":"skill-1","Slug":"volcengine/ark/demo"}],"Total":2}`
			case req.URL.Path == "/v1/skills/download/volcengine/ark/demo" && req.URL.Query().Get("version") == "1.0.0":
				body = archive
				contentType = skillArchiveContentType
			default:
				t.Fatalf("unexpected request: %s", req.URL)
			}
			return &http.Response{
				StatusCode:    http.StatusOK,
				Header:        http.Header{"Content-Type": []string{contentType}},
				Body:          io.NopCloser(strings.NewReader(body)),
				ContentLength: int64(len(body)),
				Request:       req,
			}, nil
		}),
	}
	api := selfhosted.NewClientAPI(arkruntime.NewClientWithApiKey("test-api-key", arkruntime.WithHTTPClient(httpClient)))

	content, err := api.OpenSkill(context.Background(), selfhosted.OpenSkillRequest{
		Skill: selfhosted.SkillRef{Type: "skill_hub", SkillID: "skill-1", Version: "1.0.0"},
	})
	if err != nil {
		t.Fatalf("OpenSkill() error = %v", err)
	}
	defer content.Body.Close()
	data, err := io.ReadAll(content.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if len(requests) != 2 {
		t.Fatalf("requests = %v", requests)
	}
	if string(data) != archive || content.ContentType != skillArchiveContentType || content.FileName != "demo" {
		t.Fatalf("content = %q type=%q file=%q", data, content.ContentType, content.FileName)
	}
}

func TestClientAPIHeartbeatDefaultsExpectedLastHeartbeat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/environments/env-1/work/work-1/heartbeat" {
			http.NotFound(w, r)
			return
		}
		if got := r.URL.Query().Get("expected_last_heartbeat"); got != selfhosted.ExpectedLastHeartbeatNoHeartbeat {
			t.Fatalf("expected_last_heartbeat = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"last_heartbeat":"2026-08-11T00:00:00Z","lease_extended":true,"state":"active","ttl_seconds":30,"type":"work_heartbeat"}`))
	}))
	defer server.Close()

	api := selfhosted.NewClientAPI(arkruntime.NewClientWithApiKey("test-api-key", arkruntime.WithBaseUrl(server.URL)))
	resp, err := api.HeartbeatWork(context.Background(), selfhosted.HeartbeatWorkRequest{
		EnvironmentID:     "env-1",
		WorkID:            "work-1",
		DesiredTTLSeconds: environment.NewOptInt64(30),
	})
	if err != nil {
		t.Fatalf("HeartbeatWork() error = %v", err)
	}
	if resp.LastHeartbeat != "2026-08-11T00:00:00Z" || resp.State != selfhosted.WorkStateActive {
		t.Fatalf("HeartbeatWork() = %+v", resp)
	}
}

func TestClientAPIOpenSkillDownloadURLUsesConfiguredHTTPClient(t *testing.T) {
	const archive = "zip-bytes"
	called := false
	httpClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			called = true
			if got := req.URL.String(); got != "https://signed.example.com/skill.zip" {
				t.Fatalf("url = %q", got)
			}
			if got := req.Header.Get("Authorization"); got != "" {
				t.Fatalf("Authorization = %q", got)
			}
			return &http.Response{
				StatusCode:    http.StatusOK,
				Header:        http.Header{"Content-Type": []string{skillArchiveContentType}},
				Body:          io.NopCloser(strings.NewReader(archive)),
				ContentLength: int64(len(archive)),
				Request:       req,
			}, nil
		}),
	}
	api := selfhosted.NewClientAPI(arkruntime.NewClientWithApiKey("test-api-key", arkruntime.WithHTTPClient(httpClient)))

	content, err := api.OpenSkill(context.Background(), selfhosted.OpenSkillRequest{
		Skill: selfhosted.SkillRef{DownloadURL: "https://signed.example.com/skill.zip"},
	})
	if err != nil {
		t.Fatalf("OpenSkill() error = %v", err)
	}
	defer content.Body.Close()
	data, err := io.ReadAll(content.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if !called {
		t.Fatal("configured HTTP client was not used")
	}
	if string(data) != archive || content.ContentType != skillArchiveContentType || content.FileName != "skill.zip" {
		t.Fatalf("content = %q type=%q file=%q", data, content.ContentType, content.FileName)
	}
}
