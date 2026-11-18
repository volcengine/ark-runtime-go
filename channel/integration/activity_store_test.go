// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	memory "github.com/volcengine/ark-runtime-go/channel/internal/testutil/memorystore"
)

func TestIdleCheckpointPersistsAcrossClaimAndRejectsStaleFence(t *testing.T) {
	for _, kind := range []string{"embedded", "sql"} {
		t.Run(kind, func(t *testing.T) {
			clock := &idleClock{now: time.Now().UTC()}
			var store core.Store
			if kind == "embedded" {
				backing, err := newEmbeddedStore(t, clock.Now)
				require.NoError(t, err)
				store = backing
			} else {
				backing, err := memory.New(memory.Options{Clock: clock.Now})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, backing.Close()) })
				store = backing
			}
			opts := testRuntimeOptions()
			opts.Clock = clock.Now
			r := mustRuntime(t, store, runtimeBinding(), newMAForHarness(t, &conformance.MAServer{}), &runtimeProvider{}, opts)
			ingested := mustEnqueue(t, r, "binding", runtimeMessage())
			ctx := context.Background()
			first, err := store.LeaseTurn(ctx, core.ClaimOptions{Owner: "first", TTL: time.Hour, MessageDeadline: time.Hour})
			require.NoError(t, err)
			require.NotNil(t, first)
			progress := &core.TurnActivity{Initialized: true, LastProgressAt: clock.Now().Add(20 * time.Minute), SeenEvents: map[string]bool{"a": true, "b": true}, Streams: map[string][]uint64{"message": {12, 34}}}
			clock.advance(20 * time.Minute)
			_, err = store.UpdateTurn(ctx, first.Turn.Lease, core.TurnUpdate{Phase: core.PhaseObserve, Session: core.TurnSession{SessionID: "sesn-main"}, Activity: progress, IdleTimeout: time.Hour})
			require.NoError(t, err)
			require.NoError(t, store.ReleaseTurn(ctx, first.Turn.Lease))
			clock.advance(10 * time.Minute)
			second, err := store.LeaseTurn(ctx, core.ClaimOptions{Owner: "second", TTL: time.Hour, MessageDeadline: time.Hour})
			require.NoError(t, err)
			require.NotNil(t, second)
			require.Equal(t, progress, second.Turn.Activity)
			require.Equal(t, progress.LastProgressAt.Add(time.Hour), second.Turn.Schedule.DeadlineAt)
			_, err = store.UpdateTurn(ctx, first.Turn.Lease, core.TurnUpdate{Phase: core.PhaseObserve, Activity: progress, IdleTimeout: time.Hour})
			require.ErrorIs(t, err, core.ErrLeaseLost)
			saved, err := store.GetTurn(ctx, ingested.Turn.ID)
			require.NoError(t, err)
			require.Equal(t, progress, saved.Activity)
		})
	}
}
