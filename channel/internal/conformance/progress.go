// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func progressContract(t *testing.T, open opener) {
	s, clock := open(t, StoreConfig{})
	ctx := t.Context()
	_, err := s.Ingest(ctx, SampleVersion(), SampleMessage("native-progress"))
	require.NoError(t, err)
	claimed, err := s.LeaseTurn(ctx, Claim("worker"))
	require.NoError(t, err)
	cp := &core.ProgressCheckpoint{Handle: "opaque-cot", Sequence: 1}
	_, err = s.UpdateTurn(ctx, claimed.Turn.Lease, core.TurnUpdate{Progress: cp})
	require.NoError(t, err)
	persisted, err := s.GetTurn(ctx, claimed.Turn.ID)
	require.NoError(t, err)
	require.Equal(t, cp, persisted.Progress)
	stale := claimed.Turn.Lease
	stale.Fence++
	_, err = s.UpdateTurn(ctx, stale, core.TurnUpdate{Progress: cp})
	require.ErrorIs(t, err, core.ErrLeaseLost)
	reply := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: claimed.Turn.ID}, 0, "answer", claimed.Turn.BindingID, core.ReplyTarget{ChatID: "chat"}, core.TextBody("answer"))
	progress := core.NewDelivery(core.DeliverySource{Kind: core.SourceProgress, ID: claimed.Turn.ID}, 0, "finish", claimed.Turn.BindingID, core.ReplyTarget{ChatID: "chat"}, core.DeliveryBody{Type: core.BodyProgress, Progress: &core.ProgressDelivery{Checkpoint: *cp, Update: core.ProgressUpdate{TurnID: claimed.Turn.ID, State: core.ProgressDone}}})
	progress.NextAttemptAt = clock.Now().Add(time.Hour)
	require.NoError(t, s.SettleTurn(ctx, claimed.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{progress, reply}))
	first, err := s.LeaseDelivery(ctx, "sender", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, core.BodyText, first.Body.Type, "pending COT cannot block the final answer")
	require.NoError(t, s.CompleteDelivery(ctx, first.Lease, core.DeliveryResult{Status: core.DeliveryDone}))
	removed, err := s.Prune(ctx, clock.Now().Add(time.Minute))
	require.NoError(t, err)
	require.Zero(t, removed.Turns, "unclosed COT keeps its source reference")
	clock.Advance(time.Hour)
	last, err := s.LeaseDelivery(ctx, "closer", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, last)
	require.Equal(t, core.BodyProgress, last.Body.Type)
	attempts, ok := s.(core.ProgressDeliveryAttemptStore)
	require.True(t, ok)
	stale = last.Lease
	stale.Fence++
	require.ErrorIs(t, attempts.MarkProgressDeliveryAttempt(ctx, stale), core.ErrLeaseLost)
	require.NoError(t, attempts.MarkProgressDeliveryAttempt(ctx, last.Lease))
	require.ErrorIs(t, attempts.MarkProgressDeliveryAttempt(ctx, last.Lease), core.ErrConflict)
	clock.Advance(2 * time.Minute)
	last, err = s.LeaseDelivery(ctx, "recovered-closer", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, last)
	require.True(t, last.ProgressAttempted, "recovery retains remote intent independently of the lease counter")
	require.ErrorIs(t, attempts.MarkProgressDeliveryAttempt(ctx, last.Lease), core.ErrConflict)
	require.NoError(t, s.CompleteDelivery(ctx, last.Lease, core.DeliveryResult{Status: core.DeliveryFailed, ErrorKind: "progress_unconfirmed"}))
	_, err = s.RetryDelivery(ctx, last.ID)
	require.ErrorIs(t, err, core.ErrConflict, "unknown COT completion cannot use ordinary message retry")
	removed, err = s.Prune(ctx, clock.Now().Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, 1, removed.Turns)
	require.Equal(t, 2, removed.Deliveries)
}
