// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/secret"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	config, err := Load(writeConfig(t, validJSON()))
	require.NoError(t, err)
	return config
}

func TestStatusReadOnlyProjectionAndWriterLock(t *testing.T) {
	config := testConfig(t)
	_, err := Status(context.Background(), config)
	require.Error(t, err)
	require.NoFileExists(t, config.DatabasePath)
	require.NoError(t, Install(t.Context(), config))
	store, err := openTestEmbedded(config.DatabasePath, embedded.Options{})
	require.NoError(t, err)
	_, err = store.Ingest(context.Background(), core.NewBindingVersion(core.Binding{ID: "primary", AppID: "cli_app", AgentID: "agent_1", AgentVersion: "v1", EnvironmentID: "env_1"}), core.Message{BindingID: "primary", ProviderEventID: "evt_1", MessageID: "msg_1", TenantKey: "tenant", ChatID: "chat", ChatType: "p2p", SenderID: "sender", Text: "private text SECRET_SENTINEL", ReceivedAt: time.Now()})
	require.NoError(t, err)
	_, err = Status(context.Background(), config)
	require.ErrorContains(t, err, "stop")
	require.NoError(t, store.Close())
	before, err := os.ReadFile(config.DatabasePath)
	require.NoError(t, err)
	status, err := Status(context.Background(), config)
	require.NoError(t, err)
	encoded, err := json.Marshal(status)
	require.NoError(t, err)
	require.Len(t, status.Turns, 1)
	require.NotEmpty(t, status.Turns[0].ID)
	require.Equal(t, 1, status.Stats.QueuedTurns)
	require.Len(t, status.Conversations, 1)
	require.NotContains(t, string(encoded), "SECRET_SENTINEL")
	require.NotContains(t, string(encoded), "APP_SECRET")
	after, err := os.ReadFile(config.DatabasePath)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestRunMissingSecretDoesNotLeaveDatabaseLocked(t *testing.T) {
	config := testConfig(t)
	require.NoError(t, Install(t.Context(), config))
	t.Setenv("APP_SECRET", "")
	t.Setenv("MA_KEY", "")
	err := Run(context.Background(), config)
	require.Error(t, err)
	require.ErrorContains(t, err, "ma api key unavailable")
	require.NotContains(t, err.Error(), "APP_SECRET")
	store, err := openTestEmbedded(config.DatabasePath, embedded.Options{})
	require.NoError(t, err)
	require.NoError(t, store.Close())
}

type blockingProvider struct{ started chan struct{} }

type fakeMA struct{}

func (fakeMA) CreateSession(context.Context, core.CreateSessionSpec) (core.Session, error) {
	return core.Session{}, nil
}
func (fakeMA) GetSession(context.Context, string) (core.Session, error) { return core.Session{}, nil }
func (fakeMA) ListSessions(context.Context, string, string) (core.SessionPage, error) {
	return core.SessionPage{}, nil
}
func (fakeMA) SendMessage(context.Context, string, string, core.UserInput) (core.SentResponse, error) {
	return core.SentResponse{}, nil
}
func (fakeMA) SendInterrupt(context.Context, string, string) (core.SentResponse, error) {
	return core.SentResponse{}, nil
}
func (fakeMA) SendToolConfirmation(context.Context, string, core.ToolConfirmation) (core.SentResponse, error) {
	return core.SentResponse{}, nil
}
func (fakeMA) ListEvents(context.Context, string, core.EventQuery) (core.EventPage, error) {
	return core.EventPage{}, nil
}
func (fakeMA) StreamEvents(context.Context, string) (core.EventStream, error) { return nil, nil }

func (p blockingProvider) Start(ctx context.Context, _ func(context.Context, core.Message) error) error {
	close(p.started)
	<-ctx.Done()
	return nil
}
func (blockingProvider) Deliver(context.Context, core.Delivery) (string, error) { return "", nil }

func TestRunCancellationClosesStore(t *testing.T) {
	config := testConfig(t)
	require.NoError(t, Install(t.Context(), config))
	t.Setenv("MA_KEY", "fixture-key")
	started := make(chan struct{})
	deps := dependencies{
		Resolver: secret.EnvFileResolver{},
		NewMA:    func(_ MAConfig, _ string) (core.MAClient, error) { return fakeMA{}, nil },
		NewProvider: func(_ core.Binding, _ FeishuConfig, _ secret.Resolver) (core.Provider, error) {
			return blockingProvider{started}, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runWith(ctx, config, deps) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("provider did not start")
	}
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("runtime did not stop")
	}
	store, err := openTestEmbedded(config.DatabasePath, embedded.Options{})
	require.NoError(t, err)
	require.NoError(t, store.Close())
}

func TestStatusRejectsBrokenDatabaseWithoutMutation(t *testing.T) {
	config := testConfig(t)
	require.NoError(t, os.WriteFile(config.DatabasePath, []byte("not-a-db"), 0o600))
	before, err := os.ReadFile(config.DatabasePath)
	require.NoError(t, err)
	_, err = Status(context.Background(), config)
	require.Error(t, err)
	statusError := err
	after, err := os.ReadFile(config.DatabasePath)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, filepath.Base(config.DatabasePath), "state.db")
	require.False(t, strings.Contains(statusError.Error(), "not-a-db"))
}

// acceptingProvider 启动后注入一条消息，再阻塞到取消。
type acceptingProvider struct {
	message core.Message
	started chan struct{}
}

func (p acceptingProvider) Start(ctx context.Context, accept func(context.Context, core.Message) error) error {
	_ = accept(ctx, p.message)
	close(p.started)
	<-ctx.Done()
	return nil
}
func (acceptingProvider) Deliver(context.Context, core.Delivery) (string, error) { return "om", nil }

func TestRunServesAdminMetricsAndLogs(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	data := strings.Replace(validJSON(), `"database_path"`, `"observability":{"admin_addr":"`+addr+`","log_level":"debug"},"database_path"`, 1)
	config, err := Load(writeConfig(t, data))
	require.NoError(t, err)
	require.NoError(t, Install(t.Context(), config))
	t.Setenv("MA_KEY", "fixture-key")
	started := make(chan struct{})
	var logs syncBuffer
	message := core.Message{BindingID: "primary", ProviderEventID: "evt", MessageID: "msg", TenantKey: "t", ChatID: "c", ChatType: "p2p", Text: "SECRET_TEXT_SENTINEL", ReceivedAt: time.Now()}
	deps := dependencies{
		Resolver:  secret.EnvFileResolver{},
		NewMA:     func(_ MAConfig, _ string) (core.MAClient, error) { return fakeMA{}, nil },
		LogOutput: &logs,
		NewProvider: func(_ core.Binding, _ FeishuConfig, _ secret.Resolver) (core.Provider, error) {
			return acceptingProvider{message: message, started: started}, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runWith(ctx, config, deps) }()
	<-started
	var body string
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://" + addr + "/metrics")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		raw, _ := io.ReadAll(resp.Body)
		body = string(raw)
		return strings.Contains(body, `event="ingress",status="accepted"`) && strings.Contains(body, "channel_queue_queued_turns")
	}, 3*time.Second, 20*time.Millisecond)
	resp, err := http.Get("http://" + addr + "/readyz")
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	cancel()
	require.NoError(t, <-done)
	text := logs.String()
	require.Contains(t, text, `"msg":"channel runner started"`)
	require.Contains(t, text, `"event":"ingress"`)
	require.NotContains(t, text, "SECRET_TEXT_SENTINEL", "message text never reaches logs")
	require.NotContains(t, body, "SECRET_TEXT_SENTINEL")
}

func TestLoadRejectsNonLoopbackAdmin(t *testing.T) {
	for _, value := range []string{`{"admin_addr":"0.0.0.0:9100"}`, `{"admin_addr":":9100"}`, `{"log_level":"trace"}`, `{"log_format":"xml"}`} {
		_, err := Load(writeConfig(t, strings.Replace(validJSON(), `"database_path"`, `"observability":`+value+`,"database_path"`, 1)))
		require.Error(t, err, value)
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
