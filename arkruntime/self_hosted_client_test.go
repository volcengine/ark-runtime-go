// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/environment"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/session"
)

const testBearerToken = "Bearer test-api-key"

func TestEnvironmentWorkRequests(t *testing.T) {
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Method+" "+r.URL.Path)
		if got := r.Header.Get("Authorization"); got != testBearerToken {
			t.Fatalf("Authorization = %q", got)
		}
		if got := r.Header.Get("X-Ark-Environment-Key"); got != "" {
			t.Fatalf("unexpected environment key header = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")

		switch r.Method + " " + r.URL.Path {
		case "GET /environments/env-1/work/poll":
			if got := r.Header.Get(environmentWorkWorkerIDHeader); got != "worker-1" {
				t.Fatalf("Ark-Worker-ID = %q", got)
			}
			if got := r.URL.Query().Get("block_ms"); got != "999" {
				t.Fatalf("block_ms = %q", got)
			}
			if got := r.URL.Query().Get("worker_id"); got != "" {
				t.Fatalf("unexpected worker_id query = %q", got)
			}
			if got := r.URL.Query().Get("max_items"); got != "" {
				t.Fatalf("unexpected max_items query = %q", got)
			}
			_, _ = w.Write([]byte(`{"id":"work-1","created_at":"2026-08-10T00:00:00Z","data":{"id":"sess-1","type":"session"},"environment_id":"env-1","latest_heartbeat_at":"2026-08-10T00:00:00Z","state":"queued","type":"work"}`))
		case "POST /environments/env-1/work/work-1/ack":
			assertNoBody(t, r)
			if got := r.Header.Get(environmentWorkWorkerIDHeader); got != "worker-1" {
				t.Fatalf("Ark-Worker-ID = %q", got)
			}
			_, _ = w.Write([]byte(`{"id":"work-1","created_at":"2026-08-10T00:00:00Z","data":{"id":"sess-1","type":"session"},"environment_id":"env-1","state":"starting","type":"work"}`))
		case "POST /environments/env-1/work/work-1/heartbeat":
			assertNoBody(t, r)
			if got := r.URL.Query().Get("desired_ttl_seconds"); got != "30" {
				t.Fatalf("desired_ttl_seconds = %q", got)
			}
			if got := r.URL.Query().Get("expected_last_heartbeat"); got != "2026-08-10T00:00:00Z" {
				t.Fatalf("expected_last_heartbeat = %q", got)
			}
			_, _ = w.Write([]byte(`{"last_heartbeat":"2026-08-10T00:00:00Z","lease_extended":true,"state":"active","ttl_seconds":30,"type":"work_heartbeat"}`))
		case "POST /environments/env-1/work/work-1/stop":
			var body map[string]bool
			decodeJSONBody(t, r, &body)
			if len(body) != 1 || !body["force"] {
				t.Fatalf("stop body = %+v", body)
			}
			_, _ = w.Write([]byte(`{"id":"work-1","created_at":"2026-08-10T00:00:00Z","data":{"id":"sess-1","type":"session"},"environment_id":"env-1","state":"stopping","type":"work"}`))
		case "POST /environments/env-1/work/work-2/stop":
			assertNoBody(t, r)
			_, _ = w.Write([]byte(`{"id":"work-2","created_at":"2026-08-10T00:00:00Z","data":{"id":"sess-2","type":"session"},"environment_id":"env-1","state":"stopping","type":"work"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewClientWithApiKey("test-api-key", WithBaseUrl(server.URL))
	ctx := context.Background()

	work, err := client.PollWork(ctx, &environment.PollWorkRequest{
		EnvironmentID: "env-1",
		WorkerID:      "worker-1",
		BlockMS:       999,
	})
	if err != nil {
		t.Fatalf("PollWork() error = %v", err)
	}
	if work == nil || work.SessionIDValue() != "sess-1" {
		t.Fatalf("PollWork() = %+v", work)
	}
	if got := work.LatestHeartbeatValue(); got != "2026-08-10T00:00:00Z" {
		t.Fatalf("PollWork().LatestHeartbeatValue() = %q", got)
	}
	if err := client.AckWork(ctx, &environment.AckWorkRequest{
		EnvironmentID: "env-1",
		WorkID:        "work-1",
		WorkerID:      environment.NewOptString("worker-1"),
	}); err != nil {
		t.Fatalf("AckWork() error = %v", err)
	}
	heartbeat, err := client.HeartbeatWork(ctx, &environment.HeartbeatWorkRequest{
		EnvironmentID:         "env-1",
		WorkID:                "work-1",
		DesiredTTLSeconds:     environment.NewOptInt64(30),
		ExpectedLastHeartbeat: environment.NewOptString("2026-08-10T00:00:00Z"),
	})
	if err != nil {
		t.Fatalf("HeartbeatWork() error = %v", err)
	}
	if heartbeat == nil {
		t.Fatalf("HeartbeatWork() = %+v", heartbeat)
	}
	if heartbeat.TTLSeconds != 30 {
		t.Fatalf("HeartbeatWork() = %+v", heartbeat)
	}
	if err := client.StopWork(ctx, &environment.StopWorkRequest{
		EnvironmentID: "env-1",
		WorkID:        "work-1",
		Force:         environment.NewOptBool(true),
	}); err != nil {
		t.Fatalf("StopWork() error = %v", err)
	}
	if err := client.StopWork(ctx, &environment.StopWorkRequest{
		EnvironmentID: "env-1",
		WorkID:        "work-2",
	}); err != nil {
		t.Fatalf("StopWork() error = %v", err)
	}

	want := []string{
		"GET /environments/env-1/work/poll",
		"POST /environments/env-1/work/work-1/ack",
		"POST /environments/env-1/work/work-1/heartbeat",
		"POST /environments/env-1/work/work-1/stop",
		"POST /environments/env-1/work/work-2/stop",
	}
	if strings.Join(seen, "\n") != strings.Join(want, "\n") {
		t.Fatalf("requests = %v, want %v", seen, want)
	}
}

func TestSessionStreamsIgnoreHTTPClientTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(80 * time.Millisecond)
		_, _ = io.WriteString(w, "data: {\"type\":\"session.status_idle\"}\n\n")
	}))
	defer server.Close()

	client := NewClientWithApiKey(
		"test-api-key",
		WithBaseUrl(server.URL),
		WithHTTPClient(&http.Client{Timeout: 20 * time.Millisecond}),
	)
	tests := []struct {
		name string
		open func(context.Context) (*session.StreamDecoder, error)
	}{
		{
			name: "session",
			open: func(ctx context.Context) (*session.StreamDecoder, error) {
				return client.StreamSessionEvents(ctx, "sess-1")
			},
		},
		{
			name: "thread",
			open: func(ctx context.Context) (*session.StreamDecoder, error) {
				return client.StreamSessionThreadEvents(ctx, "sess-1", "thread-1")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			decoder, err := test.open(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer decoder.Close() //nolint:errcheck // test cleanup
			if !decoder.Next() {
				t.Fatalf("stream ended before delayed event: %v", decoder.Err())
			}
			if decoder.Event().Type != "session.status_idle" {
				t.Fatalf("event type = %q", decoder.Event().Type)
			}
		})
	}
}

func TestEnvironmentPollEmptyQueueReturnsNil(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/environments/env-empty/work/poll" {
			http.NotFound(w, r)
			return
		}
		assertNoBody(t, r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	client := NewClientWithApiKey("test-api-key", WithBaseUrl(server.URL))
	work, err := client.PollWork(context.Background(), &environment.PollWorkRequest{
		EnvironmentID: "env-empty",
	})
	if err != nil {
		t.Fatalf("PollWork() error = %v", err)
	}
	if work != nil {
		t.Fatalf("PollWork() = %+v, want nil", work)
	}
}

func TestSkillContentDownload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/zip/skill-1-v1.zip" {
			w.Header().Set("Content-Type", "application/zip")
			_, _ = w.Write([]byte("zip-bytes"))
			return
		}
		if r.Method != http.MethodGet || r.URL.Path != "/skills/skill-1/versions/v1/content" {
			http.NotFound(w, r)
			return
		}
		if got := r.Header.Get("Authorization"); got != testBearerToken {
			t.Fatalf("Authorization = %q", got)
		}
		http.Redirect(w, r, "/zip/skill-1-v1.zip", http.StatusFound)
	}))
	defer server.Close()

	client := NewClientWithApiKey("test-api-key", WithBaseUrl(server.URL))
	content, err := client.OpenSkillContent(context.Background(), "skill-1", "v1")
	if err != nil {
		t.Fatalf("OpenSkillContent() error = %v", err)
	}
	defer content.Body.Close()
	data, err := io.ReadAll(content.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(data) != "zip-bytes" || content.ContentType != "application/zip" {
		t.Fatalf("content = %q %q", data, content.ContentType)
	}
}

func TestCreateSkillWithOptionsMultipartContract(t *testing.T) {
	protectionEnabled := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/skills" {
			http.NotFound(w, r)
			return
		}
		if err := r.ParseMultipartForm(1024); err != nil {
			t.Fatalf("ParseMultipartForm() error = %v", err)
		}
		if got := r.FormValue("display_title"); got != "Readiness Skill" {
			t.Fatalf("display_title = %q", got)
		}
		if got := r.FormValue("protection_enabled"); got != "true" {
			t.Fatalf("protection_enabled = %q", got)
		}
		if files := r.MultipartForm.File["files"]; len(files) != 1 || files[0].Filename != "skill.zip" {
			t.Fatalf("files = %+v", files)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"skill-1","object":"skill","created_at":1786430000,"description":"ok","latest_version":"1","display_title":"Readiness Skill","source":"custom","updated_at":1786430001,"name":"readiness","protection_enabled":true}`))
	}))
	defer server.Close()

	client := NewClientWithApiKey("test-api-key", WithBaseUrl(server.URL))
	out, err := client.CreateSkillWithOptions(
		context.Background(),
		strings.NewReader("zip-bytes"),
		"skill.zip",
		CreateSkillOptions{
			DisplayTitle:      "Readiness Skill",
			ProtectionEnabled: &protectionEnabled,
		},
	)
	if err != nil {
		t.Fatalf("CreateSkillWithOptions() error = %v", err)
	}
	if out == nil || out.DisplayTitle != "Readiness Skill" || out.Source != "custom" {
		t.Fatalf("CreateSkillWithOptions() = %+v", out)
	}
}

func TestSendSessionEventRaw(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/sessions/sess-1/events" {
			http.NotFound(w, r)
			return
		}
		if got := r.Header.Get("Idempotency-Key"); got != "" {
			t.Fatalf("Idempotency-Key = %q", got)
		}
		var body struct {
			Events []map[string]any `json:"events"`
		}
		decodeJSONBody(t, r, &body)
		if len(body.Events) != 1 || body.Events[0]["type"] != "user.tool_result" {
			t.Fatalf("body = %+v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"type":"user.tool_result","tool_use_id":"toolu-1"}]}`))
	}))
	defer server.Close()

	client := NewClientWithApiKey("test-api-key", WithBaseUrl(server.URL))
	err := client.SendSessionEventRaw(
		context.Background(),
		"sess-1",
		map[string]any{"type": "user.tool_result", "tool_use_id": "toolu-1"},
	)
	if err != nil {
		t.Fatalf("SendSessionEventRaw() error = %v", err)
	}
}

func TestSessionMAContractRequests(t *testing.T) {
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Method+" "+r.URL.Path)
		if got := r.Header.Get("Authorization"); got != testBearerToken {
			t.Fatalf("Authorization = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")

		switch r.Method + " " + r.URL.Path {
		case "GET /sessions/sess-1":
			assertNoBody(t, r)
			assertQueryValue(t, r, "work_id", "")
			_, _ = w.Write([]byte(`{"id":"sess-1","type":"session","status":"idle","environment_id":"env-1","agent":{},"created_at":"2026-08-10T00:00:00Z","updated_at":"2026-08-10T00:00:00Z"}`))
		case "GET /sessions/sess-1/events":
			assertNoBody(t, r)
			assertQueryValue(t, r, "created_at[gt]", "2026-08-10T00:00:00Z")
			assertQueryValue(t, r, "limit", "2")
			assertQueryValue(t, r, "order", "asc")
			assertQueryValue(t, r, "page", "page-1")
			assertQueryValues(t, r, "types", []string{"agent.message", "agent.tool_use"})
			assertQueryValue(t, r, "work_id", "")
			assertQueryValue(t, r, "lease_id", "")
			_, _ = w.Write([]byte(`{"data":[{"id":"sevt-1","type":"agent.message","processed_at":"2026-08-10T00:00:01Z","content":[]}],"next_page":"page-2"}`))
		case "GET /sessions/sess-1/events/stream":
			assertNoBody(t, r)
			if got := r.Header.Get("Accept"); got != "text/event-stream" {
				t.Fatalf("Accept = %q", got)
			}
			assertQueryValues(t, r, "event_deltas", []string{"agent.message", "agent.thinking"})
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: [DONE]\n\n"))
		case "POST /sessions/sess-1/events":
			if got := r.Header.Get("Idempotency-Key"); got != "" {
				t.Fatalf("Idempotency-Key = %q", got)
			}
			var body struct {
				Events []map[string]any `json:"events"`
			}
			decodeJSONBody(t, r, &body)
			if len(body.Events) != 1 || body.Events[0]["type"] != "user.tool_result" {
				t.Fatalf("body = %+v", body)
			}
			if _, ok := body.Events[0]["work_id"]; ok {
				t.Fatalf("unexpected work_id in body = %+v", body)
			}
			if _, ok := body.Events[0]["lease_id"]; ok {
				t.Fatalf("unexpected lease_id in body = %+v", body)
			}
			_, _ = w.Write([]byte(`{"data":[{"type":"user.tool_result","tool_use_id":"toolu-1"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewClientWithApiKey("test-api-key", WithBaseUrl(server.URL))
	ctx := context.Background()
	if got, err := client.GetSession(ctx, "sess-1"); err != nil || got == nil || got.ID != "sess-1" {
		t.Fatalf("GetSession() = %+v, %v", got, err)
	}
	resp, err := client.ListSessionEvents(ctx, "sess-1", &session.SessionEventsListEventsParams{
		CreatedAtGt: session.NewOptString("2026-08-10T00:00:00Z"),
		Limit:       session.NewOptInt32(2),
		Order:       session.NewOptListSessionsOrder(session.ListSessionsOrderAsc),
		Page:        session.NewOptString("page-1"),
		Types:       []string{"agent.message", "agent.tool_use"},
	})
	if err != nil {
		t.Fatalf("ListSessionEvents() error = %v", err)
	}
	if resp == nil || len(resp.Events) != 1 {
		t.Fatalf("ListSessionEvents() = %+v", resp)
	}
	decoder, err := client.StreamSessionEventsWithParams(ctx, "sess-1", &session.SessionEventsStreamEventsParams{
		EventDeltas: []string{"agent.message", "agent.thinking"},
	})
	if err != nil {
		t.Fatalf("StreamSessionEventsWithParams() error = %v", err)
	}
	_ = decoder.Close()
	err = client.SendSessionEventRaw(
		ctx,
		"sess-1",
		map[string]any{"type": "user.tool_result", "tool_use_id": "toolu-1"},
	)
	if err != nil {
		t.Fatalf("SendSessionEventRaw() error = %v", err)
	}

	want := []string{
		"GET /sessions/sess-1",
		"GET /sessions/sess-1/events",
		"GET /sessions/sess-1/events/stream",
		"POST /sessions/sess-1/events",
	}
	if strings.Join(seen, "\n") != strings.Join(want, "\n") {
		t.Fatalf("requests = %v, want %v", seen, want)
	}
}

func decodeJSONBody(t *testing.T, r *http.Request, out any) {
	t.Helper()
	if err := json.NewDecoder(r.Body).Decode(out); err != nil {
		t.Fatalf("decode body: %v", err)
	}
}

func assertNoBody(t *testing.T, r *http.Request) {
	t.Helper()
	data, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("unexpected body = %s", data)
	}
}

func assertQueryValue(t *testing.T, r *http.Request, key, want string) {
	t.Helper()
	if got := r.URL.Query().Get(key); got != want {
		t.Fatalf("query %s = %q, want %q", key, got, want)
	}
}

func assertQueryValues(t *testing.T, r *http.Request, key string, want []string) {
	t.Helper()
	got := r.URL.Query()[key]
	if len(got) != len(want) {
		t.Fatalf("query %s = %v, want %v", key, got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("query %s = %v, want %v", key, got, want)
		}
	}
}
