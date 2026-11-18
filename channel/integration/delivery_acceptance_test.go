// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestDeliveryRetryDoesNotReexecuteAgent(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{errs: []error{errUnknownDelivery, nil}}
	b := runtimeBinding()
	b.Owner = "owner"
	b.Project = "project"
	b.BindingEpoch = 7
	options := testRuntimeOptions()
	h := newHarnessWith(t, f, p, b, options)
	m := runtimeMessage()
	m.ChatType = "group"
	m.Mentioned = true
	m.ThreadID = "original-thread"
	h.enqueue(m)
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("frozen answer")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 3*time.Second)
	sent := p.delivered()
	require.Len(t, sent, 2)
	for _, d := range sent {
		require.Equal(t, sent[0].UUID, d.UUID)
		require.Equal(t, core.ReplyTargetFor(m, b), d.ReplyTarget)
		require.Equal(t, sent[0].Body, d.Body)
		require.NotNil(t, d.Snapshot)
		require.Equal(t, b.AppID, d.Snapshot.Binding.AppID)
		require.Equal(t, b.BindingEpoch, d.Snapshot.Binding.BindingEpoch)
	}
	require.Equal(t, sent[0].UUID, state.export.Deliveries[0].UUID)
	f.Lock()
	defer f.Unlock()
	require.Equal(t, 1, f.CreateCount)
	require.Equal(t, 1, f.SendCount)
}

func TestDeliveryUnknownOutsideWindowDoesNotResend(t *testing.T) {
	for _, elapsed := range []time.Duration{time.Hour - time.Nanosecond, time.Hour, time.Hour + time.Nanosecond} {
		t.Run(elapsed.String(), func(t *testing.T) {
			now := time.Now()
			var mu sync.Mutex
			clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
			store, err := newEmbeddedStore(t, clock)
			require.NoError(t, err)
			ctx := context.Background()
			b, m := runtimeBinding(), runtimeMessage()
			ingested, err := store.Ingest(ctx, core.NewBindingVersion(b), m)
			require.NoError(t, err)
			lease, err := store.LeaseTurn(ctx, conformance.Claim("setup"))
			require.NoError(t, err)
			d := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: ingested.Turn.ID}, 0, "answer", b.ID, core.ReplyTargetFor(m, b), core.TextBody("answer"))
			require.NoError(t, store.SettleTurn(ctx, lease.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{d}))
			first, err := store.LeaseDelivery(ctx, "first", time.Minute)
			require.NoError(t, err)
			require.NotNil(t, first)
			require.NoError(t, store.CompleteDelivery(ctx, first.Lease, core.DeliveryResult{Status: core.DeliveryUncertain, ErrorKind: "provider_result_unknown", NextAttemptAt: now.Add(time.Second)}))
			mu.Lock()
			now = now.Add(elapsed)
			mu.Unlock()
			f, p := &conformance.MAServer{}, &runtimeProvider{}
			options := testRuntimeOptions()
			options.Clock = clock
			runtime, err := core.NewRuntime(store, []core.Connection{{Binding: b, Provider: p, MAClient: newMAForHarness(t, f)}}, options)
			require.NoError(t, err)
			runUntilCleanup(t, runtime)
			want := core.DeliveryDone
			if elapsed >= time.Hour {
				want = core.DeliveryFailed
			}
			state := waitLatest(t, store, core.TurnDone, want, time.Second)
			final := state.export.Deliveries[0]
			require.Equal(t, first.UUID, final.UUID)
			if elapsed >= time.Hour {
				require.Zero(t, p.count())
				require.Equal(t, "provider_result_window_expired", final.ErrorKind)
			} else {
				require.Equal(t, 1, p.count())
				require.Equal(t, first.UUID, p.delivered()[0].UUID)
			}
			f.Lock()
			defer f.Unlock()
			require.Zero(t, f.CreateCount)
			require.Zero(t, f.SendCount)
		})
	}
}

func TestDeliveryRebindingCannotRerouteFrozenApplication(t *testing.T) {
	f, original, newApp := &conformance.MAServer{}, &runtimeProvider{}, &runtimeProvider{}
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	b := runtimeBinding()
	b.Owner = "owner"
	b.BindingEpoch = 1
	ctx := context.Background()
	m := runtimeMessage()
	accepted, err := store.Ingest(ctx, core.NewBindingVersion(b), m)
	require.NoError(t, err)
	lease, err := store.LeaseTurn(ctx, conformance.Claim("setup"))
	require.NoError(t, err)
	d := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: accepted.Turn.ID}, 0, "answer", b.ID, core.ReplyTargetFor(m, b), core.TextBody("original answer"))
	d.Snapshot = core.NewTurnSnapshot(b, time.Time{})
	require.NoError(t, store.SettleTurn(ctx, lease.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{d}))
	b.AppID = "rebound-app"
	b.BindingEpoch = 2
	options := testRuntimeOptions()
	options.Resolver = fixtureResolver{connections: map[string]core.Connection{b.ID: {Binding: b, MAClient: newMAForHarness(t, f), Provider: newApp}}}
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: runtimeBinding(), MAClient: newMAForHarness(t, f), Provider: original}}, options)
	require.NoError(t, err)
	runUntilCleanup(t, runtime)
	state := waitLatest(t, store, core.TurnDone, core.DeliveryFailed, time.Second)
	require.Equal(t, "connection_invalid", state.export.Deliveries[0].ErrorKind)
	require.Zero(t, newApp.count())
	require.Zero(t, original.count())
	require.Equal(t, d.UUID, state.export.Deliveries[0].UUID)
	require.Equal(t, d.ReplyTarget, state.export.Deliveries[0].ReplyTarget)
}
