// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func snapshotContract(t *testing.T, open opener) {
	s, _ := open(t, StoreConfig{})
	ctx := context.Background()
	ingest(t, s, SampleMessage("snapshot"))
	claimed := lease(t, s, "worker")
	session := core.TurnSession{Generation: 1, CreationKey: "create"}
	_, err := s.UpdateTurn(ctx, claimed.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: session})
	require.NoError(t, err)
	spec := core.CreateSessionSpec{Binding: SampleBinding(), CreationKey: "create", LookupKey: "lookup", EnvironmentOverride: &core.EnvironmentOverride{Type: "sandbox", Env: map[string]string{"CUSTOM": "original"}}}
	snapshot := core.NewSessionSnapshot(claimed.Turn.ConversationID, 1, spec)
	frozen, err := s.FreezeSession(ctx, claimed.Turn.Lease, snapshot)
	require.NoError(t, err)
	spec.EnvironmentOverride.Env["CUSTOM"] = "changed"
	_, err = s.FreezeSession(ctx, claimed.Turn.Lease, core.NewSessionSnapshot(claimed.Turn.ConversationID, 1, spec))
	require.ErrorIs(t, err, core.ErrConflict)
	got, err := s.GetSessionSnapshot(ctx, frozen.ID)
	require.NoError(t, err)
	require.Equal(t, "original", got.Spec.EnvironmentOverride.Env["CUSTOM"])
	turn, err := s.GetTurn(ctx, claimed.Turn.ID)
	require.NoError(t, err)
	require.Equal(t, frozen.ID, turn.Session.SnapshotID)
	got.Spec.EnvironmentOverride.Env["CUSTOM"] = "mutated"
	got, err = s.GetSessionSnapshot(ctx, frozen.ID)
	require.NoError(t, err)
	require.Equal(t, "original", got.Spec.EnvironmentOverride.Env["CUSTOM"])
}

func sessionHistoryContract(t *testing.T, open opener) {
	s, clock := open(t, StoreConfig{})
	ctx := context.Background()
	history, ok := s.(core.SessionHistoryStore)
	require.True(t, ok, "store must retain per-generation session history")
	var route core.RouteKey
	for generation := uint64(1); generation <= 3; generation++ {
		msg := SampleMessage(fmt.Sprintf("history-%d", generation))
		ingest(t, s, msg)
		claimed := lease(t, s, "worker")
		route = claimed.Turn.ConversationID
		binding := core.SessionBinding{State: core.SessionActive, Generation: generation, SessionID: fmt.Sprintf("session-%d", generation), CreationKey: fmt.Sprintf("creation-%d", generation), LookupKey: "lookup", BindingVersion: claimed.Turn.BindingVersion}
		_, err := s.BindSession(ctx, claimed.Turn.Lease, binding)
		require.NoError(t, err)
		_, err = s.BindSession(ctx, claimed.Turn.Lease, binding)
		require.NoError(t, err)
		require.NoError(t, s.SettleTurn(ctx, claimed.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, nil))
		_, err = s.ResetConversation(ctx, route)
		require.NoError(t, err)
	}
	clock.Advance(8 * 24 * time.Hour)
	_, err := s.Prune(ctx, clock.Now())
	require.NoError(t, err)
	rows, err := history.ListSessionHistory(ctx, route, 0, 2)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, "session-1", rows[0].Binding.SessionID)
	require.Equal(t, uint64(2), rows[1].Binding.Generation)
	rows, err = history.ListSessionHistory(ctx, route, 2, 2)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "session-3", rows[0].Binding.SessionID)
}

func queueTailDeadlineContract(t *testing.T, open opener) {
	s, clock := open(t, StoreConfig{QueueTTL: time.Minute})
	ctx := context.Background()
	ingest(t, s, SampleMessage("head"))
	head := lease(t, s, "head-worker")
	_, err := s.RenewTurn(ctx, head.Turn.Lease, time.Hour)
	require.NoError(t, err)
	tail := ingest(t, s, SampleMessage("tail"))
	clock.Advance(2 * time.Minute)
	claimed, err := s.LeaseTurn(ctx, Claim("expiry-worker"))
	require.NoError(t, err)
	require.NotNil(t, claimed, "expired queue tail must be claimable while head remains leased")
	require.Equal(t, tail.Turn.ID, claimed.Turn.ID)
	require.NoError(t, s.SettleTurn(ctx, claimed.Turn.Lease, core.TurnOutcome{Status: core.TurnFailed, ErrorKind: "queue_deadline"}, nil))
	running, err := s.GetTurn(ctx, head.Turn.ID)
	require.NoError(t, err)
	require.Equal(t, core.TurnRunning, running.Status)
	require.Equal(t, "head-worker", running.Lease.Owner)
}

func accountQuotaContract(t *testing.T, open opener) {
	s, _ := open(t, StoreConfig{})
	ctx := context.Background()
	for _, item := range []struct{ id, owner string }{{"one", "a"}, {"two", "a"}, {"three", "b"}} {
		b := SampleBinding()
		b.Owner = item.owner
		m := SampleMessage(item.id)
		m.ChatID = item.id
		_, e := s.Ingest(ctx, core.NewBindingVersion(b), m)
		require.NoError(t, e)
	}
	claim := Claim("worker")
	claim.AccountLimit = 1
	a, e := s.LeaseTurn(ctx, claim)
	require.NoError(t, e)
	require.NotNil(t, a)
	intent := func(t core.Turn) core.TurnUpdate {
		return core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{Generation: 1, SessionID: "sesn-" + t.ID}, Submit: core.Submission{WriteAttempts: 1, SubmittedAt: time.Now()}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteInput, Limit: 1}}
	}
	current, e := s.UpdateTurn(ctx, a.Turn.Lease, intent(a.Turn))
	require.NoError(t, e)
	b, e := s.LeaseTurn(ctx, claim)
	require.NoError(t, e)
	require.NotNil(t, b)
	require.Equal(t, "two", b.Message.Message.ChatID)
	_, e = s.UpdateTurn(ctx, b.Turn.Lease, intent(b.Turn))
	require.ErrorIs(t, e, core.ErrRemoteLimit)
	other, e := s.LeaseTurn(ctx, claim)
	require.NoError(t, e)
	require.NotNil(t, other)
	require.Equal(t, "three", other.Message.Message.ChatID)
	_, e = s.UpdateTurn(ctx, other.Turn.Lease, intent(other.Turn))
	require.NoError(t, e)
	require.NoError(t, s.WaitTurn(ctx, a.Turn.Lease, core.TurnWait{Reason: "observe", Submit: current.Submit, Until: time.Now().Add(time.Hour)}))
	_, e = s.UpdateTurn(ctx, b.Turn.Lease, intent(b.Turn))
	require.ErrorIs(t, e, core.ErrRemoteLimit, "waiting observation retains execution reservation")
}

func admissionContract(t *testing.T, open opener) {
	s, clock := open(t, StoreConfig{})
	msg := SampleMessage("expired")
	msg.AdmitUntil = clock.Now()
	_, err := s.Ingest(context.Background(), SampleVersion(), msg)
	require.ErrorIs(t, err, core.ErrAdmissionExpired)
	turns, err := s.ListTurns(context.Background(), core.TurnQuery{})
	require.NoError(t, err)
	require.Empty(t, turns)
}

func queueTailRecoveryContract(t *testing.T, open opener) {
	s, clock := open(t, StoreConfig{QueueTTL: time.Minute})
	ctx := context.Background()
	ingest(t, s, SampleMessage("head"))
	head := lease(t, s, "head")
	_, err := s.RenewTurn(ctx, head.Turn.Lease, time.Hour)
	require.NoError(t, err)
	tail := ingest(t, s, SampleMessage("tail"))
	clock.Advance(2 * time.Minute)
	first, err := s.LeaseTurn(ctx, Claim("crashed"))
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, tail.Turn.ID, first.Turn.ID)
	clock.Advance(time.Minute)
	second, err := s.LeaseTurn(ctx, Claim("recovery"))
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, tail.Turn.ID, second.Turn.ID)
	require.Greater(t, second.Turn.Lease.Fence, first.Turn.Lease.Fence)
	require.NoError(t, s.SettleTurn(ctx, second.Turn.Lease, core.TurnOutcome{Status: core.TurnFailed, ErrorKind: "queue_deadline"}, nil))
	require.ErrorIs(t, s.SettleTurn(ctx, first.Turn.Lease, core.TurnOutcome{Status: core.TurnFailed}, nil), core.ErrLeaseLost)
}
