// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
)

// newQueueTTLHarness 构造带 QueueTTL 的 Runtime 与共享同一个 Store 的测试夹具。
func newQueueTTLHarness(t *testing.T, queueTTL time.Duration, clock func() time.Time) (*core.Runtime, testStore, *runtimeProvider) {
	t.Helper()
	store, err := openEmbeddedFixture(filepath.Join(t.TempDir(), "qttl.db"), embedded.Options{Clock: clock, NoSync: true, QueueTTL: queueTTL})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	fixture := &conformance.MAServer{}
	server := httptest.NewServer(http.HandlerFunc(fixture.ServeHTTP))
	t.Cleanup(server.Close)
	client, err := newTestMA(server.URL, "fixture-key", ma.ClientOptions{RequestTimeout: time.Second, StreamTimeout: time.Second})
	require.NoError(t, err)
	options := testRuntimeOptions()
	options.Clock = clock
	provider := &runtimeProvider{}
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: runtimeBinding(), MAClient: client, Provider: provider}}, options)
	require.NoError(t, err)
	return runtime, store, provider
}

// TestQueueDeadlineSettlesExpiredTailTurn 验证排队超时（S5b）：Turn 在队尾等待超过
// received_at+QueueTTL 后才被领取时，直接以 queue_deadline 失败结算并产生诊断回复；
// StartedAt 保持零值，不进入执行。
func TestQueueDeadlineSettlesExpiredTailTurn(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return base }
	runtime, store, _ := newQueueTTLHarness(t, 5*time.Minute, clock)

	msg := runtimeMessage()
	msg.ReceivedAt = base.Add(-6 * time.Minute)
	ingested, err := runtime.Enqueue(context.Background(), "binding", msg)
	require.NoError(t, err)
	require.False(t, ingested.Turn.QueueDeadlineAt.IsZero(), "queue deadline is frozen at ingest")
	require.True(t, ingested.Turn.Schedule.StartedAt.IsZero())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()

	state := waitLatest(t, store, core.TurnFailed, core.DeliveryDone, 3*time.Second)
	cancel()
	require.NoError(t, <-done)

	turn := state.turn
	require.NotNil(t, turn)
	require.Equal(t, ingested.Turn.ID, turn.ID)
	require.NotNil(t, turn.Outcome)
	require.Equal(t, "queue_deadline", turn.Outcome.ErrorKind)
	require.True(t, turn.Phase == core.PhaseNone, "an expired queue turn never starts session creation")
}

// TestQueueDeadlineAllowsFreshTurn 验证排队时限内的 Turn 不被判排队超时。
func TestQueueDeadlineAllowsFreshTurn(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return base }
	_, store, _ := newQueueTTLHarness(t, 5*time.Minute, clock)

	msg := runtimeMessage()
	msg.ReceivedAt = base.Add(-1 * time.Minute)
	ingested, err := store.Ingest(context.Background(), core.NewBindingVersion(runtimeBinding()), msg)
	require.NoError(t, err)
	require.False(t, ingested.Turn.QueueDeadlineAt.IsZero())
	require.True(t, ingested.Turn.QueueDeadlineAt.After(base), "a turn received one minute ago is within the 5 minute queue ttl")
}
