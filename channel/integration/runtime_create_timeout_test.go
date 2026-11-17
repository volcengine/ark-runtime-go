// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func TestRuntimeInFlightCreateTimeoutRetainsUnknownEvenIfServerIntends429(t *testing.T) {
	fixture := &conformance.MAServer{CreateCode: http.StatusTooManyRequests}
	var creates atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/v3/sessions" {
			_, _ = io.Copy(io.Discard, r.Body)
			creates.Add(1)
			// 请求已到服务端，但429响应晚于客户端超时，不能作为客户端已知的拒绝证明。
			select {
			case <-r.Context().Done():
			case <-time.After(time.Second):
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"code":"SessionQuotaExceeded"}}`))
			return
		}
		fixture.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	client, err := newTestMA(server.URL, "fixture", ma.ClientOptions{RequestTimeout: 60 * time.Millisecond})
	require.NoError(t, err)
	options := deadlineRuntimeOptions()
	options.MessageDeadline = 3 * time.Second
	runtime := mustRuntime(t, store, runtimeBinding(), client, &runtimeProvider{}, options)
	first := mustEnqueue(t, runtime, "binding", runtimeMessage())
	runUntilCleanup(t, runtime)
	require.Eventually(t, func() bool { fixture.Lock(); defer fixture.Unlock(); return fixture.ListSessionCount > 0 }, 2*time.Second, 5*time.Millisecond)
	turn, err := store.GetTurn(context.Background(), first.Turn.ID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), turn.Submit.WriteAttempts)
	ops, err := store.ListRemoteOperations(context.Background(), core.RemoteOperationQuery{TurnID: first.Turn.ID, OpenOnly: true})
	require.NoError(t, err)
	require.Len(t, ops, 1)
	require.True(t, ops[0].Counted)
	require.Nil(t, ops[0].Proof)
	require.Equal(t, int32(1), creates.Load(), "unknown create must be looked up rather than posted again")
}
