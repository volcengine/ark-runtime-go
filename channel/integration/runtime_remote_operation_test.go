// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/volcengine/ark-runtime-go/channel/ma"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestRemoteUnknownRetainsBudgetAndSessionBarrier(t *testing.T) {
	f := &conformance.MAServer{SendCode: 503}
	opts := deadlineRuntimeOptions()
	opts.AccountLimit = 1
	h := newHarnessWith(t, f, &runtimeProvider{}, runtimeBinding(), opts)
	first := h.enqueue(runtimeMessage())
	h.run()
	h.waitSettled(core.TurnFailed, core.DeliveryDone, 3*time.Second)
	ops, e := h.store.ListRemoteOperations(context.Background(), core.RemoteOperationQuery{TurnID: first.Turn.ID, OpenOnly: true})
	require.NoError(t, e)
	require.Len(t, ops, 1)
	require.True(t, ops[0].Counted)
	second := runtimeMessage()
	second.MessageID = "second"
	second.ProviderEventID = "second-event"
	h.enqueue(second)
	h.waitSettled(core.TurnFailed, core.DeliveryDone, 3*time.Second)
	f.Lock()
	sent := f.SendCount
	f.Unlock()
	require.Equal(t, 1, sent, "unknown input plus interrupt 200 must block later input")
}

func TestTwoRuntimesShareRemoteExecutionBudget(t *testing.T) {
	f := &conformance.MAServer{}
	opts := testRuntimeOptions()
	opts.Workers = 1
	opts.AccountLimit = 1
	opts.MessageDeadline = 5 * time.Second
	binding := runtimeBinding()
	binding.Owner = "shared-account"
	h := newHarnessWith(t, f, &runtimeProvider{}, binding, opts)
	ctx := context.Background()
	var inputs []core.Message
	for i := 0; i < 2; i++ {
		m := runtimeMessage()
		m.ChatID = fmt.Sprint(i)
		m.MessageID = "seed" + m.ChatID
		m.ProviderEventID = m.MessageID
		seed, e := h.store.Ingest(ctx, core.NewBindingVersion(binding), m)
		require.NoError(t, e)
		l, e := h.store.LeaseTurn(ctx, conformance.Claim("seed"))
		require.NoError(t, e)
		_, e = h.store.BindSession(ctx, l.Turn.Lease, core.SessionBinding{State: core.SessionActive, Generation: 1, SessionID: "sesn-" + m.ChatID, CreationKey: "seed", LookupKey: "seed" + m.ChatID, BindingVersion: seed.Turn.BindingVersion})
		require.NoError(t, e)
		require.NoError(t, h.store.SettleTurn(ctx, l.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, nil))
		m.MessageID = "input" + m.ChatID
		m.ProviderEventID = m.MessageID
		inputs = append(inputs, m)
	}
	for _, m := range inputs {
		h.enqueue(m)
	}
	client, e := newTestMA(h.server.URL, "fixture", ma.ClientOptions{})
	require.NoError(t, e)
	second, e := core.NewRuntime(h.store, []core.Connection{{Binding: binding, MAClient: client, Provider: &runtimeProvider{}}}, opts)
	require.NoError(t, e)
	h.run()
	runUntilCleanup(t, second)
	require.Eventually(t, func() bool { f.Lock(); defer f.Unlock(); return f.SendCount == 1 }, time.Second, 5*time.Millisecond)
	require.Never(t, func() bool { f.Lock(); defer f.Unlock(); return f.SendCount > 1 }, 200*time.Millisecond, 5*time.Millisecond)
	ops, e := h.store.ListRemoteOperations(ctx, core.RemoteOperationQuery{OpenOnly: true})
	require.NoError(t, e)
	require.Len(t, ops, 1)
	require.Equal(t, core.RemoteInput, ops[0].Kind)
}

type cancelRemoteIntentStore struct {
	core.Store
	cancel context.CancelFunc
}

func (s *cancelRemoteIntentStore) UpdateTurn(ctx context.Context, l core.Lease, u core.TurnUpdate) (core.Turn, error) {
	t, e := s.Store.UpdateTurn(ctx, l, u)
	if e == nil && u.RemoteIntent != nil {
		s.cancel()
	}
	return t, e
}
func TestRemoteIntentCancelledBeforeHTTPIsNotUnknown(t *testing.T) {
	f := &conformance.MAServer{}
	opts := testRuntimeOptions()
	opts.Workers = 1
	h := newHarnessWith(t, f, &runtimeProvider{}, runtimeBinding(), opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wrapped := &cancelRemoteIntentStore{Store: h.store, cancel: cancel}
	client, e := newTestMA(h.server.URL, "fixture", ma.ClientOptions{})
	require.NoError(t, e)
	r, e := core.NewRuntime(wrapped, []core.Connection{{Binding: runtimeBinding(), MAClient: client, Provider: &runtimeProvider{}}}, opts)
	require.NoError(t, e)
	_, e = r.Enqueue(ctx, "binding", runtimeMessage())
	require.NoError(t, e)
	require.NoError(t, r.Run(ctx))
	f.Lock()
	creates := f.CreateCount
	f.Unlock()
	require.Zero(t, creates)
	ops, e := h.store.ListRemoteOperations(context.Background(), core.RemoteOperationQuery{OpenOnly: true})
	require.NoError(t, e)
	require.Empty(t, ops, "cancel before the HTTP call is proven not sent")
}

type failingRemoteContextStore struct {
	core.Store
	failure error
}

func (s failingRemoteContextStore) GetRemoteOperationContext(context.Context, string) (*core.TurnLease, error) {
	return nil, s.failure
}
func TestRemoteReconcilerStoreFailureStopsRuntime(t *testing.T) {
	f := &conformance.MAServer{}
	h := newHarness(t, f, &runtimeProvider{})
	ctx := context.Background()
	first := h.enqueue(runtimeMessage())
	l, e := h.store.LeaseTurn(ctx, conformance.Claim("setup"))
	require.NoError(t, e)
	_, e = h.store.UpdateTurn(ctx, l.Turn.Lease, core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{Generation: 1, SessionID: "sesn-original"}, Submit: core.Submission{WriteAttempts: 1, SubmittedAt: time.Now()}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteInput, Limit: 1}})
	require.NoError(t, e)
	require.NoError(t, h.store.SettleTurn(ctx, l.Turn.Lease, core.TurnOutcome{Status: core.TurnFailed}, nil))
	require.NotEmpty(t, first.Turn.ID)
	failure := errors.New("remote source read failure")
	wrapped := failingRemoteContextStore{Store: h.store, failure: failure}
	client, e := newTestMA(h.server.URL, "fixture", ma.ClientOptions{})
	require.NoError(t, e)
	r, e := core.NewRuntime(wrapped, []core.Connection{{Binding: runtimeBinding(), MAClient: client, Provider: &runtimeProvider{}}}, testRuntimeOptions())
	require.NoError(t, e)
	runCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, r.Run(runCtx), failure)
}
