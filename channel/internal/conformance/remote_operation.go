// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// RunRemoteOperationStore 验证远端额度独立于本地租约和终态。
func RunRemoteOperationStore(t *testing.T, factory StoreFactory) {
	t.Run("reservation_survives_wait_settle_and_prune", func(t *testing.T) {
		ctx := context.Background()
		clock := NewClock(time.Now())
		s := factory(t, StoreConfig{Clock: clock.Now})
		defer func() { require.NoError(t, s.Close()) }()
		binding := SampleBinding()
		binding.Owner = "owner"
		version := core.NewBindingVersion(binding)
		first, err := s.Ingest(ctx, version, SampleMessage("remote1"))
		require.NoError(t, err)
		claim := Claim("worker")
		claim.AccountLimit = 1
		leased, err := s.LeaseTurn(ctx, claim)
		require.NoError(t, err)
		require.NotNil(t, leased)
		update := core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{Generation: 1, SessionID: "sesn-remote"}, Submit: core.Submission{WriteAttempts: 1, SubmittedAt: clock.Now()}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteInput, Limit: 1}}
		turn, err := s.UpdateTurn(ctx, leased.Turn.Lease, update)
		require.NoError(t, err)
		require.NoError(t, s.WaitTurn(ctx, turn.Lease, core.TurnWait{Reason: "unknown", Submit: turn.Submit}))
		leased, err = s.LeaseTurn(ctx, claim)
		require.NoError(t, err)
		require.NotNil(t, leased)
		_, err = s.UpdateTurn(ctx, leased.Turn.Lease, update)
		require.NoError(t, err, "takeover must not double charge")
		require.NoError(t, s.SettleTurn(ctx, leased.Turn.Lease, core.TurnOutcome{Status: core.TurnFailed}, nil))
		clock.Advance(time.Hour)
		_, err = s.Prune(ctx, clock.Now())
		require.NoError(t, err)
		retained, err := s.GetTurn(ctx, first.Turn.ID)
		require.NoError(t, err)
		require.NotNil(t, retained, "unknown pins local terminal")
		m := SampleMessage("remote2")
		m.ChatID = "other"
		_, err = s.Ingest(ctx, version, m)
		require.NoError(t, err)
		next, err := s.LeaseTurn(ctx, claim)
		require.NoError(t, err)
		require.NotNil(t, next, "full account must still lease observers")
		update.Session.SessionID = "sesn-other"
		_, err = s.UpdateTurn(ctx, next.Turn.Lease, update)
		require.ErrorIs(t, err, core.ErrRemoteLimit)
		unchanged, err := s.GetTurn(ctx, next.Turn.ID)
		require.NoError(t, err)
		require.Zero(t, unchanged.Submit.WriteAttempts, "intent and budget rollback together")
		remote := s.(core.RemoteOperationStore)
		ops, err := remote.ListRemoteOperations(ctx, core.RemoteOperationQuery{OpenOnly: true})
		require.NoError(t, err)
		require.Len(t, ops, 1)
		op, err := remote.LeaseRemoteOperation(ctx, core.RemoteClaim{Owner: "reconciler", TTL: time.Minute})
		require.NoError(t, err)
		require.NotNil(t, op)
		_, err = remote.UpdateRemoteOperation(ctx, op.Lease, core.RemoteUpdate{Proof: &core.RemoteProof{Kind: core.RemoteGone, Reference: "gone"}})
		require.NoError(t, err)
		_, err = remote.UpdateRemoteOperation(ctx, op.Lease, core.RemoteUpdate{Proof: &core.RemoteProof{Kind: core.RemoteGone, Reference: "gone"}})
		require.ErrorIs(t, err, core.ErrLeaseLost)
		_, err = s.UpdateTurn(ctx, next.Turn.Lease, update)
		require.NoError(t, err)
	})
}

// RunRemoteOperationFences 验证独立 fence、晚到中断屏障和按账号隔离。
func RunRemoteOperationFences(t *testing.T, factory StoreFactory) {
	runCombinedRemoteUpdates(t, factory)
	runInterruptDispatchOnce(t, factory)
	runStopAfterRemoteResolution(t, factory)
	t.Run("cleanup_fence_and_generation_barrier", func(t *testing.T) {
		ctx := context.Background()
		clock := NewClock(time.Now())
		s := factory(t, StoreConfig{Clock: clock.Now})
		defer func() { require.NoError(t, s.Close()) }()
		remote := s.(core.RemoteOperationStore)
		ingest := func(id, owner string) *core.TurnLease {
			b := SampleBinding()
			b.Owner = owner
			m := SampleMessage(id)
			m.ChatID = id
			_, e := s.Ingest(ctx, core.NewBindingVersion(b), m)
			require.NoError(t, e)
			l, e := s.LeaseTurn(ctx, Claim(id))
			require.NoError(t, e)
			require.NotNil(t, l)
			return l
		}
		intent := func(l *core.TurnLease, session string, kind core.RemoteKind) error {
			phase := core.PhaseSend
			if kind == core.RemoteCreate {
				phase = core.PhaseCreate
			}
			_, e := s.UpdateTurn(ctx, l.Turn.Lease, core.TurnUpdate{Phase: phase, Session: core.TurnSession{Generation: 1, SessionID: session}, Submit: core.Submission{SubmittedAt: clock.Now(), WriteAttempts: 1}, RemoteIntent: &core.RemoteIntent{Kind: kind, Limit: 1}})
			return e
		}
		first := ingest("one", "account")
		require.NoError(t, intent(first, "sesn-one", core.RemoteInput))
		require.NoError(t, s.SettleTurn(ctx, first.Turn.Lease, core.TurnOutcome{Status: core.TurnFailed}, nil))
		op, e := remote.LeaseRemoteOperation(ctx, core.RemoteClaim{Owner: "old", TTL: time.Second})
		require.NoError(t, e)
		require.NotNil(t, op)
		old := op.Lease
		clock.Advance(2 * time.Second)
		op, e = remote.LeaseRemoteOperation(ctx, core.RemoteClaim{Owner: "new", TTL: time.Minute})
		require.NoError(t, e)
		require.NotNil(t, op)
		_, e = remote.UpdateRemoteOperation(ctx, old, core.RemoteUpdate{CleanupStarted: true})
		require.ErrorIs(t, e, core.ErrLeaseLost)
		_, e = remote.UpdateRemoteOperation(ctx, op.Lease, core.RemoteUpdate{CleanupStarted: true})
		require.NoError(t, e)
		_, e = remote.UpdateRemoteOperation(ctx, op.Lease, core.RemoteUpdate{Proof: &core.RemoteProof{Kind: core.RemoteTerminal, Reference: "terminal"}})
		require.ErrorIs(t, e, core.ErrRemoteBarrier, "terminal input cannot retract in-flight interrupt")
		_, e = s.UpdateTurn(ctx, first.Turn.Lease, core.TurnUpdate{Phase: core.PhaseObserve})
		require.ErrorIs(t, e, core.ErrLeaseLost)
		second := ingest("two", "account")
		require.ErrorIs(t, intent(second, "sesn-one", core.RemoteInput), core.ErrRemoteBarrier)
		require.ErrorIs(t, intent(second, "sesn-new", core.RemoteInput), core.ErrRemoteLimit, "new generation retains old input budget")
		require.NoError(t, intent(second, "sesn-new", core.RemoteCreate), "new generation uses independent creation budget")
		other := ingest("three", "other")
		require.NoError(t, intent(other, "sesn-other", core.RemoteInput), "another account progresses")
	})
	t.Run("explicit_not_accepted_releases_once", func(t *testing.T) {
		ctx := context.Background()
		s := factory(t, StoreConfig{})
		defer func() { require.NoError(t, s.Close()) }()
		_, e := s.Ingest(ctx, SampleVersion(), SampleMessage("not_sent"))
		require.NoError(t, e)
		l, e := s.LeaseTurn(ctx, Claim("worker"))
		require.NoError(t, e)
		u := core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{Generation: 1, SessionID: "sesn-one"}, Submit: core.Submission{SubmittedAt: time.Now(), WriteAttempts: 1}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteInput, Limit: 1}}
		_, e = s.UpdateTurn(ctx, l.Turn.Lease, u)
		require.NoError(t, e)
		u.RemoteIntent = nil
		u.RemoteKind = core.RemoteInput
		u.RemoteProof = &core.RemoteProof{Kind: core.RemoteNotAccepted, Reference: "transport_not_sent"}
		u.Submit = core.Submission{}
		_, e = s.UpdateTurn(ctx, l.Turn.Lease, u)
		require.NoError(t, e)
		_, e = s.UpdateTurn(ctx, l.Turn.Lease, u)
		require.NoError(t, e)
		ops, e := s.(core.RemoteOperationStore).ListRemoteOperations(ctx, core.RemoteOperationQuery{OpenOnly: true})
		require.NoError(t, e)
		require.Empty(t, ops)
		u.RemoteProof = nil
		u.RemoteIntent = &core.RemoteIntent{Kind: core.RemoteInput, Limit: 1}
		u.Submit = core.Submission{WriteAttempts: 1, SubmittedAt: time.Now()}
		_, e = s.UpdateTurn(ctx, l.Turn.Lease, u)
		require.NoError(t, e)
	})
}

func runInterruptDispatchOnce(t *testing.T, factory StoreFactory) {
	t.Run("interrupt_dispatch_survives_takeover", func(t *testing.T) {
		ctx := context.Background()
		s := factory(t, StoreConfig{})
		defer func() { require.NoError(t, s.Close()) }()
		_, err := s.Ingest(ctx, SampleVersion(), SampleMessage("interrupt-once"))
		require.NoError(t, err)
		l, err := s.LeaseTurn(ctx, Claim("worker"))
		require.NoError(t, err)
		intent := core.TurnUpdate{Phase: core.PhaseObserve, Session: core.TurnSession{Generation: 1, SessionID: "sesn-one"}, Submit: core.Submission{SubmittedAt: time.Now(), WriteAttempts: 1}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteInput, Limit: 1}}
		_, err = s.UpdateTurn(ctx, l.Turn.Lease, intent)
		require.NoError(t, err)
		cleanup := core.TurnUpdate{Phase: intent.Phase, Session: intent.Session, Submit: intent.Submit, RemoteKind: core.RemoteInput, RemoteCleanup: true}
		combined := cleanup
		combined.RemoteProof = &core.RemoteProof{Kind: core.RemoteInterrupted, Reference: (core.RemoteOperation{TurnID: l.Turn.ID, Generation: 1}).InterruptEventID()}
		_, err = s.UpdateTurn(ctx, l.Turn.Lease, combined)
		require.ErrorIs(t, err, core.ErrRemoteBarrier, "first interrupt intent cannot release its own barrier in the same update")
		_, err = s.UpdateTurn(ctx, l.Turn.Lease, cleanup)
		require.NoError(t, err)
		_, err = s.UpdateTurn(ctx, l.Turn.Lease, cleanup)
		require.ErrorIs(t, err, core.ErrInterruptStarted, "one persisted stop must not grant a second HTTP dispatch")
		require.NoError(t, s.SettleTurn(ctx, l.Turn.Lease, core.TurnOutcome{Status: core.TurnStopped}, nil))
		remote := s.(core.RemoteOperationStore)
		op, err := remote.LeaseRemoteOperation(ctx, core.RemoteClaim{Owner: "takeover", TTL: time.Minute})
		require.NoError(t, err)
		require.NotNil(t, op)
		_, err = remote.UpdateRemoteOperation(ctx, op.Lease, core.RemoteUpdate{CleanupStarted: true})
		require.ErrorIs(t, err, core.ErrInterruptStarted, "background takeover must not dispatch the already claimed interrupt")
		_, err = remote.UpdateRemoteOperation(ctx, op.Lease, core.RemoteUpdate{Proof: &core.RemoteProof{Kind: core.RemoteInterrupted, Reference: "wrong-stop"}})
		require.ErrorIs(t, err, core.ErrRemoteBarrier)
		resolved, err := remote.UpdateRemoteOperation(ctx, op.Lease, core.RemoteUpdate{Proof: &core.RemoteProof{Kind: core.RemoteInterrupted, Reference: op.InterruptEventID()}})
		require.NoError(t, err)
		require.True(t, resolved.Resolved)
		require.False(t, resolved.Counted)
		_, err = remote.UpdateRemoteOperation(ctx, op.Lease, core.RemoteUpdate{CleanupStarted: true})
		require.ErrorIs(t, err, core.ErrLeaseLost, "late worker cannot reacquire a resolved stop")
		_, err = s.Ingest(ctx, SampleVersion(), SampleMessage("after-stop"))
		require.NoError(t, err)
		next, err := s.LeaseTurn(ctx, Claim("next"))
		require.NoError(t, err)
		_, err = s.UpdateTurn(ctx, next.Turn.Lease, intent)
		require.NoError(t, err, "matching consumption proof releases same-session barrier and budget")
	})
}

func runStopAfterRemoteResolution(t *testing.T, factory StoreFactory) {
	t.Run("stop_does_not_reopen_resolved_remote_operation", func(t *testing.T) {
		ctx := context.Background()
		s := factory(t, StoreConfig{})
		defer func() { require.NoError(t, s.Close()) }()
		_, err := s.Ingest(ctx, SampleVersion(), SampleMessage("stop-after-proof"))
		require.NoError(t, err)
		l, err := s.LeaseTurn(ctx, Claim("worker"))
		require.NoError(t, err)
		u := core.TurnUpdate{Phase: core.PhaseObserve, Session: core.TurnSession{Generation: 1, SessionID: "sesn-one"}, Submit: core.Submission{SubmittedAt: time.Now(), WriteAttempts: 1}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteInput, Limit: 1}}
		_, err = s.UpdateTurn(ctx, l.Turn.Lease, u)
		require.NoError(t, err)
		u.RemoteIntent, u.RemoteKind = nil, core.RemoteInput
		u.RemoteProof = &core.RemoteProof{Kind: core.RemoteTerminal, Reference: l.Turn.InputEventID}
		_, err = s.UpdateTurn(ctx, l.Turn.Lease, u)
		require.NoError(t, err)
		// 模拟远端证明已提交、本地 SettleTurn 前崩溃，然后由停止请求接管。
		u.RemoteProof, u.RemoteCleanup = nil, true
		_, err = s.UpdateTurn(ctx, l.Turn.Lease, u)
		require.ErrorIs(t, err, core.ErrInterruptStarted, "released budget cannot grant a new untracked interrupt")
		ops, err := s.ListRemoteOperations(ctx, core.RemoteOperationQuery{TurnID: l.Turn.ID})
		require.NoError(t, err)
		require.Len(t, ops, 1)
		require.True(t, ops[0].Resolved)
		require.False(t, ops[0].CleanupStarted)
	})
}

// RunRemoteCreationBinding 验证绑定 Session 和释放创建预算处于同一事务。
func RunRemoteCreationBinding(t *testing.T, factory StoreFactory) {
	t.Run("bind_resolves_creation_atomically", func(t *testing.T) {
		ctx := context.Background()
		s := factory(t, StoreConfig{})
		defer func() { require.NoError(t, s.Close()) }()
		_, e := s.Ingest(ctx, SampleVersion(), SampleMessage("create"))
		require.NoError(t, e)
		l, e := s.LeaseTurn(ctx, Claim("w"))
		require.NoError(t, e)
		_, e = s.UpdateTurn(ctx, l.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "key"}, Submit: core.Submission{SubmittedAt: time.Now(), WriteAttempts: 1}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteCreate, Limit: 1}})
		require.NoError(t, e)
		_, e = s.BindSession(ctx, l.Turn.Lease, core.SessionBinding{State: core.SessionActive, Generation: 1, CreationKey: "key", LookupKey: "lookup", SessionID: "sesn-created", BindingVersion: l.Turn.BindingVersion})
		require.NoError(t, e)
		ops, e := s.ListRemoteOperations(ctx, core.RemoteOperationQuery{OpenOnly: true})
		require.NoError(t, e)
		require.Empty(t, ops, "committed binding must release create reservation without a later worker transaction")
	})
}

func runCombinedRemoteUpdates(t *testing.T, factory StoreFactory) {
	for _, tc := range []struct {
		name    string
		kind    core.RemoteKind
		proof   core.RemoteProofKind
		blocked bool
	}{
		{"terminal_and_new_cleanup", core.RemoteInput, core.RemoteTerminal, true},
		{"not_accepted_and_new_cleanup", core.RemoteInput, core.RemoteNotAccepted, true},
		{"created_and_new_cleanup", core.RemoteCreate, core.RemoteCreated, true},
		{"gone_and_new_cleanup", core.RemoteInput, core.RemoteGone, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := factory(t, StoreConfig{})
			defer func() { require.NoError(t, s.Close()) }()
			b := SampleBinding()
			b.Owner = "combined-owner"
			version := core.NewBindingVersion(b)
			_, err := s.Ingest(ctx, version, SampleMessage("first"))
			require.NoError(t, err)
			first, err := s.LeaseTurn(ctx, Claim("first"))
			require.NoError(t, err)
			phase := core.PhaseSend
			if tc.kind == core.RemoteCreate {
				phase = core.PhaseCreate
			}
			intent := core.TurnUpdate{Phase: phase, Session: core.TurnSession{Generation: 1, SessionID: "sesn-first", CreationKey: "create-first"}, Submit: core.Submission{SubmittedAt: time.Now(), WriteAttempts: 1}, RemoteIntent: &core.RemoteIntent{Kind: tc.kind, Limit: 1}}
			_, err = s.UpdateTurn(ctx, first.Turn.Lease, intent)
			require.NoError(t, err)
			require.NoError(t, s.SettleTurn(ctx, first.Turn.Lease, core.TurnOutcome{Status: core.TurnFailed}, nil))
			op, err := s.LeaseRemoteOperation(ctx, core.RemoteClaim{Owner: "reconciler", TTL: time.Minute})
			require.NoError(t, err)
			require.NotNil(t, op)
			result, err := s.UpdateRemoteOperation(ctx, op.Lease, core.RemoteUpdate{Proof: &core.RemoteProof{Kind: tc.proof, Reference: "proof"}, CleanupStarted: true, AttentionRequired: true, NextReconcileAt: time.Now().Add(time.Hour)})
			if tc.blocked {
				require.ErrorIs(t, err, core.ErrRemoteBarrier)
			} else {
				require.NoError(t, err)
				require.True(t, result.Resolved)
				require.Equal(t, "resolved", result.State)
				require.False(t, result.Counted)
				require.True(t, result.CleanupStarted)
				require.False(t, result.AttentionRequired)
				require.True(t, result.NextReconcileAt.IsZero())
			}
			saved, err := s.ListRemoteOperations(ctx, core.RemoteOperationQuery{TurnID: first.Turn.ID})
			require.NoError(t, err)
			require.Len(t, saved, 1)
			if tc.blocked {
				require.Equal(t, *op, saved[0], "invalid combined update must roll back all operation fields")
			}
			message := SampleMessage("next")
			message.ChatID = "next"
			_, err = s.Ingest(ctx, version, message)
			require.NoError(t, err)
			next, err := s.LeaseTurn(ctx, Claim("next"))
			require.NoError(t, err)
			require.NotNil(t, next)
			intent.Session.SessionID = "sesn-next"
			intent.Session.CreationKey = "create-next"
			_, err = s.UpdateTurn(ctx, next.Turn.Lease, intent)
			if tc.blocked {
				require.ErrorIs(t, err, core.ErrRemoteLimit, "combined terminal/cleanup must not release the reservation")
			} else {
				require.NoError(t, err, "authoritative gone releases exactly one reservation")
			}
		})
	}
}
