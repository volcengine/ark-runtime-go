// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package observability_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	observability "github.com/volcengine/ark-runtime-go/channel/internal/telemetry"
)

func TestMetricsUseBoundedLabelsAndExportText(t *testing.T) {
	m := observability.NewMetrics()
	m.Observe(context.Background(), core.Observation{Name: core.EventTurnSettle, Status: "done", Duration: 2 * time.Second, TurnID: "turn:secret-id", SessionID: "sesn-1"})
	m.Observe(context.Background(), core.Observation{Name: core.EventTurnSettle, Status: "failed", ErrorKind: "Agent Failed\"}", Duration: 40 * time.Second})
	m.SetGauge("queue_depth", 3)
	var out bytes.Buffer
	require.NoError(t, m.WriteText(&out))
	text := out.String()
	require.Contains(t, text, `channel_events_total{event="turn_settle",status="done",error_kind=""} 1`)
	require.Contains(t, text, `channel_events_total{event="turn_settle",status="failed",error_kind="agent_failed__"} 1`, "labels are normalized and cannot break the format")
	require.Contains(t, text, `channel_event_duration_seconds_bucket{event="turn_settle",status="done",error_kind="",le="2.5"} 1`)
	require.Contains(t, text, `channel_event_duration_seconds_bucket{event="turn_settle",status="done",error_kind="",le="1"} 0`)
	require.Contains(t, text, "channel_queue_depth 3")
	require.NotContains(t, text, "secret-id", "IDs never become metric labels")
	require.NotContains(t, text, "sesn-1")
	require.Equal(t, uint64(1), m.Count(core.EventTurnSettle, "done", ""))
}

func TestMetricsSeriesAreCapped(t *testing.T) {
	m := observability.NewMetrics()
	for i := 0; i < 2000; i++ {
		m.Observe(context.Background(), core.Observation{Name: "ingress", Status: "s", ErrorKind: fmt.Sprintf("kind_%d", i)})
	}
	var out bytes.Buffer
	require.NoError(t, m.WriteText(&out))
	require.LessOrEqual(t, strings.Count(out.String(), "channel_events_total{"), 513)
	require.Contains(t, out.String(), `event="overflow"`)
	m.Observe(context.Background(), core.Observation{Name: "ingress", Status: "s", ErrorKind: strings.Repeat("y", 500)})
	out.Reset()
	require.NoError(t, m.WriteText(&out))
	require.NotContains(t, out.String(), strings.Repeat("y", 65), "label values are truncated")
}

func TestLoggerWritesStructuredFieldsWithoutText(t *testing.T) {
	var out bytes.Buffer
	logger := observability.NewLogger(slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))
	logger.Observe(context.Background(), core.Observation{Name: core.EventTurnSettle, Status: "failed", ErrorKind: "agent_failed", TurnID: "turn:1", SessionID: "sesn-1", ConversationID: "route:v1:x", Duration: time.Second})
	line := out.String()
	require.Contains(t, line, `"level":"WARN"`)
	require.Contains(t, line, `"event":"turn_settle"`)
	require.Contains(t, line, `"turn_id":"turn:1"`)
	require.Contains(t, line, `"session_id":"sesn-1"`)
	out.Reset()
	quiet := observability.NewLogger(slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelInfo})))
	quiet.Observe(context.Background(), core.Observation{Name: core.EventIngress, Status: "accepted"})
	require.Empty(t, out.String(), "routine ingress is debug level")
}

type panicObserver struct{}

func (panicObserver) Observe(context.Context, core.Observation) { panic("boom") }

func TestFanoutIsolatesPanics(t *testing.T) {
	m := observability.NewMetrics()
	observability.Fanout{panicObserver{}, m}.Observe(context.Background(), core.Observation{Name: "ingress", Status: "accepted"})
	require.Equal(t, uint64(1), m.Count("ingress", "accepted", ""))
}

func TestAdminServerEndpointsAndLoopbackOnly(t *testing.T) {
	require.NoError(t, observability.ValidateLoopback("127.0.0.1:9100"))
	require.NoError(t, observability.ValidateLoopback("localhost:9100"))
	require.NoError(t, observability.ValidateLoopback("[::1]:9100"))
	require.Error(t, observability.ValidateLoopback("0.0.0.0:9100"))
	require.Error(t, observability.ValidateLoopback(":9100"))
	require.Error(t, observability.ValidateLoopback("10.0.0.1:9100"))
	m := observability.NewMetrics()
	m.Observe(context.Background(), core.Observation{Name: "ingress", Status: "accepted"})
	admin := &observability.AdminServer{Addr: "127.0.0.1:0", Metrics: m, Status: func(context.Context) (any, error) { return map[string]int{"tasks": 1}, nil }}
	server := httptest.NewServer(admin.Handler())
	defer server.Close()
	get := func(path string) (int, string) {
		resp, err := http.Get(server.URL + path)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	code, _ := get("/healthz")
	require.Equal(t, 200, code)
	code, _ = get("/readyz")
	require.Equal(t, 503, code)
	admin.SetReady(true)
	code, _ = get("/readyz")
	require.Equal(t, 200, code)
	code, body := get("/metrics")
	require.Equal(t, 200, code)
	require.Contains(t, body, "channel_events_total")
	code, body = get("/status")
	require.Equal(t, 200, code)
	require.JSONEq(t, `{"tasks":1}`, body)
	resp, err := http.Post(server.URL+"/healthz", "text/plain", nil)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

func TestAdminServeStopsOnCancel(t *testing.T) {
	admin := &observability.AdminServer{Addr: "127.0.0.1:0"}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- admin.Serve(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("admin server did not stop")
	}
	require.Error(t, (&observability.AdminServer{Addr: "0.0.0.0:0"}).Serve(context.Background()))
}
