// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func TestPublicMAWire(t *testing.T) {
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.Path)
		require.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
		require.Equal(t, "application/json", r.Header.Get("Accept"))
		switch r.Method + " " + r.URL.Path {
		case "POST /api/v3/sessions":
			require.Equal(t, "application/json", r.Header.Get("Content-Type"))
			require.Equal(t, "create-1", r.Header.Get("Idempotency-Key"))
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, map[string]any{"type": "agent", "id": "agent-1", "version": float64(2)}, body["agent"])
			require.Equal(t, "env-1", body["environment_id"])
			require.Equal(t, []any{"vault-1"}, body["vault_ids"])
			require.Equal(t, []any{map[string]any{"type": "memory_store", "memory_store_id": "mem-1", "access": "read_write"}}, body["resources"])
			require.Equal(t, []any{map[string]any{"key": "channel_session_key", "value": "route-1"}}, body["tags"])
			_, _ = io.WriteString(w, `{"id":"session-1","status":"running","agent":{"type":"agent","id":"agent-1","version":2},"tags":[{"key":"channel_session_key","value":"route-1"}],"type":"session","environment_id":"env","created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}`)
		case "GET /api/v3/sessions/session-1":
			_, _ = io.WriteString(w, `{"id":"session-1","status":"completed","agent":{"id":"agent-1","version":2},"type":"session","environment_id":"env","created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}`)
		case "GET /api/v3/sessions":
			require.Equal(t, "100", r.URL.Query().Get("limit"))
			require.Equal(t, "asc", r.URL.Query().Get("order"))
			require.Equal(t, "next-1", r.URL.Query().Get("page"))
			var tags []map[string]string
			require.NoError(t, json.Unmarshal([]byte(r.URL.Query().Get("tags")), &tags))
			require.Equal(t, []map[string]string{{"key": "channel_session_key", "value": "route-1"}}, tags)
			_, _ = io.WriteString(w, `{"data":[{"id":"session-1","agent":{"id":"agent-1","version":2},"tags":[{"key":"channel_session_key","value":"route-1"}],"type":"session","status":"idle","environment_id":"env","created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}],"next_page":"next-2"}`)
		case "POST /api/v3/sessions/session-1/events":
			require.Empty(t, r.Header.Get("Idempotency-Key"))
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, []any{map[string]any{"id": "input-1", "type": "user.message", "content": []any{map[string]any{"type": "text", "text": "hello"}}}}, body["events"])
			_, _ = io.WriteString(w, `{"data":[{"id":"input-1","type":"user.message"}]}`)
		case "GET /api/v3/sessions/session-1/events":
			require.Equal(t, "200", r.URL.Query().Get("limit"))
			require.Equal(t, "asc", r.URL.Query().Get("order"))
			require.Equal(t, "event-page", r.URL.Query().Get("page"))
			require.Equal(t, []string{"user.message", "session.status_idle"}, r.URL.Query()["types"])
			require.Equal(t, "2026-10-13T01:02:03Z", r.URL.Query().Get("created_at[gte]"))
			_, _ = io.WriteString(w, `{"data":[{"id":"out-1","type":"agent.message","session_thread_id":"turn-1","content":[{"type":"text","text":"answer"}]}],"next_page":"event-next"}`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "test-key", ma.ClientOptions{})
	require.NoError(t, err)
	var port core.MAClient = client
	created, err := port.CreateSession(context.Background(), core.CreateSessionSpec{Binding: core.Binding{AgentID: "agent-1", AgentVersion: "v2", EnvironmentID: "env-1", VaultIDs: []string{"vault-1"}, MemoryIDs: []string{"mem-1"}}, CreationKey: "create-1", LookupKey: "route-1"})
	require.NoError(t, err)
	require.Equal(t, "agent-1", created.AgentID)
	require.Equal(t, "2", created.AgentVersion)
	require.Equal(t, "route-1", created.Tags["channel_session_key"])
	got, err := port.GetSession(context.Background(), "session-1")
	require.NoError(t, err)
	require.Equal(t, "session-1", got.ID)
	page, err := port.ListSessions(context.Background(), "route-1", "next-1")
	require.NoError(t, err)
	require.Equal(t, "next-2", page.NextPage)
	require.Equal(t, "session-1", page.Sessions[0].ID)
	sent, err := port.SendMessage(context.Background(), "session-1", "input-1", core.UserInput{Text: "hello"})
	require.NoError(t, err)
	require.Equal(t, "input-1", sent.Events[0].ID)
	since := time.Date(2026, 10, 13, 9, 2, 3, 0, time.FixedZone("CST", 8*3600))
	events, err := port.ListEvents(context.Background(), "session-1", core.EventQuery{Page: "event-page", Limit: 200, Types: []string{"user.message", "session.status_idle"}, CreatedAtGte: &since})
	require.NoError(t, err)
	require.Equal(t, "event-next", events.NextPage)
	event := events.Events[0]
	require.Equal(t, "out-1", event.ID)
	require.Equal(t, "turn-1", event.SessionThreadID)
	require.Equal(t, "answer", event.Content[0].Text)
	require.Len(t, calls, 5)
}

func TestBaseURLAndVersionValidation(t *testing.T) {
	for _, raw := range []string{"http://user:pass@example.com", "http://example.com?x=y", "http://example.com#fragment", "file:///tmp/sock"} {
		_, err := ma.NewClient(raw, "key", ma.ClientOptions{})
		require.Error(t, err, raw)
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer server.Close()
	client, err := ma.NewClient(server.URL+"/api/v3", "key", ma.ClientOptions{})
	require.NoError(t, err)
	_, err = client.CreateSession(context.Background(), core.CreateSessionSpec{Binding: core.Binding{AgentID: "a", AgentVersion: "latest"}, CreationKey: "c", LookupKey: "l"})
	require.Error(t, err)
	require.Zero(t, calls.Load())
	_, err = client.CreateSession(context.Background(), core.CreateSessionSpec{Binding: core.Binding{AgentID: "a", AgentVersion: "1"}, LookupKey: "l"})
	require.Error(t, err)
	require.Zero(t, calls.Load())
	_, err = client.GetSession(context.Background(), "../inner")
	require.Error(t, err)
	require.Zero(t, calls.Load())
}

func TestAPIErrorWithoutOwnedSDKRetries(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("X-Request-ID", "req-1")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"code":"RateLimit","message":"secret body should stay private"}}`)
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "test-secret", ma.ClientOptions{})
	require.NoError(t, err)
	_, err = client.SendMessage(context.Background(), "session-1", "input-1", core.UserInput{Text: "hello"})
	var apiErr *ma.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 429, apiErr.HTTPStatus())
	require.Equal(t, "RateLimit", apiErr.Code)
	require.Equal(t, "req-1", apiErr.RequestID)
	require.NotContains(t, err.Error(), "test-secret")
	require.NotContains(t, err.Error(), "secret body")
	require.EqualValues(t, 1, calls.Load(), "the rejected write is returned without a transport replay")
}

func TestHTTPResponseErrorsAndCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/wait") {
			<-r.Context().Done()
			return
		}
		_, _ = io.WriteString(w, strings.Repeat("x", 4096))
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "key", ma.ClientOptions{})
	require.NoError(t, err)
	_, err = client.GetSession(context.Background(), "s")
	require.Error(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.GetSession(ctx, "s")
	require.ErrorIs(t, err, context.Canceled)
	timed, err := ma.NewClient(server.URL, "key", ma.ClientOptions{RequestTimeout: 20 * time.Millisecond})
	require.NoError(t, err)
	_, err = timed.GetSession(context.Background(), "wait")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestSSEFramesEOFAndCancellation(t *testing.T) {
	streamReady := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v3/sessions/s/events/stream", r.URL.Path)
		require.Equal(t, "text/event-stream", r.Header.Get("Accept"))
		_, _ = io.WriteString(w, ": comment\r\ndata: {\"id\":\"one\",\r\ndata: \"type\":\"future.event\"}\r\n\r\ndata: {\"id\":\"two\",\"type\":\"agent.message\"}\r\n\r\n")
		w.(http.Flusher).Flush()
		close(streamReady)
		<-r.Context().Done()
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "key", ma.ClientOptions{StreamTimeout: time.Minute})
	require.NoError(t, err)
	stream, err := client.StreamEvents(context.Background(), "s")
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()
	<-streamReady
	event, err := stream.Recv(context.Background())
	require.NoError(t, err)
	require.Equal(t, "future.event", event.Type)
	event, err = stream.Recv(context.Background())
	require.NoError(t, err)
	require.Equal(t, "two", event.ID)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	_, err = stream.Recv(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestSSEEOF(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "data: {\"id\":\"last\",\"type\":\"agent.message\"}\n\n")
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "key", ma.ClientOptions{})
	require.NoError(t, err)
	stream, err := client.StreamEvents(context.Background(), "s")
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()
	event, err := stream.Recv(context.Background())
	require.NoError(t, err)
	require.Equal(t, "last", event.ID)
	_, err = stream.Recv(context.Background())
	require.ErrorIs(t, err, io.EOF)
}

func TestBaseURLQueryEscaping(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := url.QueryUnescape(r.URL.RawQuery)
		require.NoError(t, err)
		_, _ = io.WriteString(w, `{"data":[]}`)
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL+"/api/v3", "key", ma.ClientOptions{})
	require.NoError(t, err)
	_, err = client.ListSessions(context.Background(), "tenant:chat/1", "page+1")
	require.NoError(t, err)
}

func TestSSEHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":{"code":"Unauthorized"}}`)
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "key", ma.ClientOptions{})
	require.NoError(t, err)
	_, err = client.StreamEvents(context.Background(), "s")
	var apiErr *ma.APIError
	require.True(t, errors.As(err, &apiErr))
	require.Equal(t, 401, apiErr.HTTPStatus())
}

func TestSendMessageWithContextAndFileBlocks(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		_, _ = io.WriteString(w, `{"data":[{"id":"input-1","type":"user.message","content":[{"type":"text","text":"ctx"},{"type":"text","text":"hi"},{"type":"image","source":{"type":"url","url":"https://x"}}]}]}`)
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "k", ma.ClientOptions{})
	require.NoError(t, err)
	sent, err := client.SendMessage(context.Background(), "sesn-1", "input-1", core.UserInput{Context: "ctx", Text: "hi", Blocks: []core.InputBlock{{Type: "image", FileID: "file-1"}, {Type: "document", FileID: "file-2", Title: "a.pdf"}, {Type: "video", FileID: "file-3"}}})
	require.NoError(t, err)
	events := body["events"].([]any)
	content := events[0].(map[string]any)["content"].([]any)
	require.Len(t, content, 5)
	require.Equal(t, map[string]any{"type": "text", "text": "ctx"}, content[0])
	require.Equal(t, map[string]any{"type": "text", "text": "hi"}, content[1])
	require.Equal(t, map[string]any{"type": "image", "source": map[string]any{"type": "file", "file_id": "file-1"}}, content[2])
	require.Equal(t, map[string]any{"type": "document", "title": "a.pdf", "source": map[string]any{"type": "file", "file_id": "file-2"}}, content[3])
	require.Equal(t, "video", content[4].(map[string]any)["type"])
	require.Equal(t, []core.TextBlock{{Type: "text", Text: "ctx"}, {Type: "text", Text: "hi"}, {Type: "image"}}, sent.Events[0].Content, "mixed echoes keep block types")
	_, err = client.SendMessage(context.Background(), "sesn-1", "input-1", core.UserInput{Text: "hi", Blocks: []core.InputBlock{{Type: "audio", FileID: "f"}}})
	require.Error(t, err)
	_, err = client.SendMessage(context.Background(), "sesn-1", "input-1", core.UserInput{Text: "hi", Blocks: []core.InputBlock{{Type: "image"}}})
	require.Error(t, err)
}

func TestSendMessageWithImageOnlyInput(t *testing.T) {
	for _, tc := range []struct {
		name    string
		input   core.UserInput
		content string
	}{
		{
			name:    "single image",
			input:   core.UserInput{Blocks: []core.InputBlock{{Type: "image", FileID: "file-1"}}},
			content: `[{"type":"image","source":{"type":"file","file_id":"file-1"}}]`,
		},
		{
			name:    "image with context",
			input:   core.UserInput{Context: "ctx", Blocks: []core.InputBlock{{Type: "image", FileID: "file-1"}}},
			content: `[{"type":"text","text":"ctx"},{"type":"image","source":{"type":"file","file_id":"file-1"}}]`,
		},
		{
			name:    "multiple images",
			input:   core.UserInput{Blocks: []core.InputBlock{{Type: "image", FileID: "file-1"}, {Type: "image", FileID: "file-2"}}},
			content: `[{"type":"image","source":{"type":"file","file_id":"file-1"}},{"type":"image","source":{"type":"file","file_id":"file-2"}}]`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var body []byte
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, http.MethodPost, r.Method)
				require.Equal(t, "/api/v3/sessions/sesn-1/events", r.URL.Path)
				var err error
				body, err = io.ReadAll(r.Body)
				require.NoError(t, err)
				_, _ = io.WriteString(w, `{"data":[{"id":"input-1","type":"user.message"}]}`)
			}))
			defer server.Close()
			client, err := ma.NewClient(server.URL, "k", ma.ClientOptions{})
			require.NoError(t, err)

			_, err = client.SendMessage(context.Background(), "sesn-1", "input-1", tc.input)
			require.NoError(t, err)
			require.JSONEq(t, `{"events":[{"id":"input-1","type":"user.message","content":`+tc.content+`}]}`, string(body))
		})
	}
}

func TestSendMessageNoticeBlockRendersAsText(t *testing.T) {
	var body struct {
		Events []struct {
			ID      string `json:"id"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"events"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{}})
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "k", ma.ClientOptions{})
	require.NoError(t, err)
	notice := core.InputBlock{Type: core.InputBlockNotice, Title: "表.xlsx"}
	_, err = client.SendMessage(context.Background(), "sesn-1", "input-1", core.UserInput{Text: "看看", Blocks: []core.InputBlock{notice}})
	require.NoError(t, err)
	require.Len(t, body.Events, 1)
	require.Len(t, body.Events[0].Content, 2)
	require.Equal(t, "看看", body.Events[0].Content[0].Text)
	require.Equal(t, notice.NoticeText(""), body.Events[0].Content[1].Text, "notice blocks submit as text and carry no file id")
}

func TestUploadFileStreamsMultipartAndWaitsActive(t *testing.T) {
	polls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v3/files":
			require.True(t, strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data"))
			require.NoError(t, r.ParseMultipartForm(1<<20))
			require.Equal(t, "user_data", r.FormValue("purpose"))
			require.NotEmpty(t, r.FormValue("expire_at"))
			file, header, err := r.FormFile("file")
			require.NoError(t, err)
			data, _ := io.ReadAll(file)
			require.Equal(t, "png-bytes", string(data))
			require.Equal(t, "a_b.png", header.Filename, "path separators are stripped from file names")
			require.Equal(t, "image/png", header.Header.Get("Content-Type"))
			_, _ = io.WriteString(w, `{"id":"file-9","status":"processing","object":"file","purpose":"user_data","filename":"fixture","created_at":0,"expire_at":-1}`)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/files/file-9":
			polls++
			status := "processing"
			if polls >= 2 {
				status = "active"
			}
			_, _ = io.WriteString(w, `{"object":"file","purpose":"user_data","filename":"fixture","created_at":0,"expire_at":-1,"id":"file-9","status":"`+status+`"}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "k", ma.ClientOptions{FilePollInterval: time.Millisecond, FilePollTimeout: time.Second})
	require.NoError(t, err)
	id, err := client.UploadFile(context.Background(), core.FileUpload{Reader: strings.NewReader("png-bytes"), FileName: "a/b.png", MimeType: "image/png"})
	require.NoError(t, err)
	require.Equal(t, "file-9", id)
	require.Equal(t, 2, polls)
	_, err = client.UploadFile(context.Background(), core.FileUpload{Reader: strings.NewReader("x"), FileName: "x", Purpose: "weird"})
	require.Error(t, err)
}

func TestUploadFileFailedStatusIsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"id":"file-1","status":"failed","object":"file","purpose":"user_data","filename":"fixture","created_at":0,"expire_at":-1}`)
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "k", ma.ClientOptions{})
	require.NoError(t, err)
	_, err = client.UploadFile(context.Background(), core.FileUpload{Reader: strings.NewReader("x"), FileName: "x.txt"})
	require.ErrorContains(t, err, "processing failed")
}

func TestUpsertEnvCredentialCreatesThenUpdatesManagedOnly(t *testing.T) {
	var created, updated map[string]any
	existing := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/vaults/vlt_1/credentials":
			_, _ = io.WriteString(w, `{"credentials":[`+existing+`]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v3/vaults/vlt_1/credentials":
			require.NoError(t, json.NewDecoder(r.Body).Decode(&created))
			_, _ = io.WriteString(w, `{"id":"cred_1","type":"vault_credential","vault_id":"vault","auth":{"type":"environment_variable"},"created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v3/vaults/vlt_1/credentials/cred_1":
			require.NoError(t, json.NewDecoder(r.Body).Decode(&updated))
			_, _ = io.WriteString(w, `{"id":"cred_1","type":"vault_credential","vault_id":"vault","auth":{"type":"environment_variable"},"created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "k", ma.ClientOptions{})
	require.NoError(t, err)
	spec := core.BotCredentialSpec{VaultID: "vlt_1", SecretName: "FEISHU_TENANT_ACCESS_TOKEN", AllowedHosts: []string{"open.feishu.cn"}}
	id, err := client.UpsertEnvCredential(context.Background(), spec, "t-1")
	require.NoError(t, err)
	require.Equal(t, "cred_1", id)
	auth := created["auth"].(map[string]any)
	require.Equal(t, "environment_variable", auth["type"])
	require.Equal(t, "FEISHU_TENANT_ACCESS_TOKEN", auth["secret_name"])
	require.Equal(t, "t-1", auth["secret_value"])
	require.Equal(t, map[string]any{"type": "limited", "limited": map[string]any{"allowed_hosts": []any{"open.feishu.cn"}}}, auth["networking"])
	require.Equal(t, map[string]any{"managed_by": "ark-agent-channel"}, created["metadata"])

	existing = `{"id":"cred_1","auth":{"type":"environment_variable","secret_name":"FEISHU_TENANT_ACCESS_TOKEN"},"metadata":{"managed_by":"ark-agent-channel"},"type":"vault_credential","vault_id":"vault","created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}`
	id, err = client.UpsertEnvCredential(context.Background(), spec, "t-2")
	require.NoError(t, err)
	require.Equal(t, "cred_1", id)
	auth = updated["auth"].(map[string]any)
	require.Equal(t, "t-2", auth["secret_value"])
	require.NotContains(t, auth, "secret_name", "secret_name is immutable after create")

	existing = `{"id":"cred_1","auth":{"type":"environment_variable","secret_name":"FEISHU_TENANT_ACCESS_TOKEN"},"metadata":{},"type":"vault_credential","vault_id":"vault","created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}`
	_, err = client.UpsertEnvCredential(context.Background(), spec, "t-3")
	require.ErrorContains(t, err, "not channel managed")
	require.NotContains(t, err.Error(), "t-3")
}

func TestDownloadFileUsesPresignedURLWithoutAPIKey(t *testing.T) {
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Empty(t, r.Header.Get("Authorization"), "the api key never leaves the ma origin")
		_, _ = io.WriteString(w, "file-bytes")
	}))
	defer storage.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v3/files/file-1", r.URL.Path)
		require.Equal(t, "86400", r.Header.Get("X-Ark-PreSignedURL-ExpiresAfter"))
		_, _ = io.WriteString(w, `{"id":"file-1","filename":"out.pdf","bytes":10,"mime_type":"application/pdf","status":"active","download_url":"`+storage.URL+`/obj"}`)
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "k", ma.ClientOptions{})
	require.NoError(t, err)
	content, err := client.DownloadFile(context.Background(), "file-1", 1024)
	require.NoError(t, err)
	data, err := io.ReadAll(content.Body)
	require.NoError(t, err)
	require.NoError(t, content.Body.Close())
	require.Equal(t, "file-bytes", string(data))
	require.Equal(t, "out.pdf", content.FileName)
	require.Equal(t, "application/pdf", content.MimeType)

	_, err = client.DownloadFile(context.Background(), "file-1", 5)
	require.ErrorContains(t, err, "too large")
	_, err = client.DownloadFile(context.Background(), "../x", 5)
	require.Error(t, err)
}
