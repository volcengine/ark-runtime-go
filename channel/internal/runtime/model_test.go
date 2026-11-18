// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestBindingVersionIsContentAddressed(t *testing.T) {
	binding := core.Binding{ID: "b", AppID: "app", AgentID: "a", AgentVersion: "1", VaultIDs: []string{"v"}}
	first := core.NewBindingVersion(binding)
	require.True(t, first.Valid())
	require.Equal(t, first.Version, core.NewBindingVersion(binding).Version)
	empty := binding
	empty.MemoryIDs = []string{}
	require.Equal(t, first.Version, core.NewBindingVersion(empty).Version, "nil and empty slices hash the same")
	changed := binding
	changed.ShowProgress = true
	require.NotEqual(t, first.Version, core.NewBindingVersion(changed).Version)
	tampered := first
	tampered.Content.AgentID = "other"
	require.False(t, tampered.Valid())
	binding.VaultIDs[0] = "mutated"
	require.Equal(t, "v", first.Content.VaultIDs[0], "a version never aliases the caller's slices")
}

func TestClassifyAndContentHash(t *testing.T) {
	message := core.Message{BindingID: "b", ProviderEventID: "e1", MessageID: "m", TenantKey: "t", ChatID: "c", ChatType: "p2p", Text: " /stop "}
	require.Equal(t, core.MessageControl, core.ClassifyMessage(message))
	group := message
	group.ChatType = "group"
	require.Equal(t, core.MessageUser, core.ClassifyMessage(group), "group /stop is plain text")
	redelivered := message
	redelivered.ProviderEventID, redelivered.ReceivedAt = "e2", t0
	require.Equal(t, core.ContentHash(message), core.ContentHash(redelivered))
	edited := message
	edited.Text = "other"
	require.NotEqual(t, core.ContentHash(message), core.ContentHash(edited))
}

func TestClassifyMessageStopScope(t *testing.T) {
	for _, tc := range []struct {
		name, chatType, text string
		mentioned            bool
		want                 core.MessageKind
	}{
		{"p2p", "p2p", "/stop", false, core.MessageControl},
		{"mentioned group", "group", " \n/stop\t", true, core.MessageControl},
		{"unmentioned group", "group", "/stop", false, core.MessageUser},
		{"group extra text", "group", "/stop please", true, core.MessageUser},
		{"p2p extra text", "p2p", "/stop please", false, core.MessageUser},
		{"ordinary group message", "group", "hello", true, core.MessageUser},
		{"unknown chat", "other", "/stop", true, core.MessageUser},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message := core.Message{ChatType: tc.chatType, Mentioned: tc.mentioned, Text: tc.text}
			require.Equal(t, tc.want, core.ClassifyMessage(message))
		})
	}
}

func TestSessionBindingTransitions(t *testing.T) {
	active := core.SessionBinding{State: core.SessionActive, Generation: 1, SessionID: "s1", CreationKey: "k1", LookupKey: "l1", BindingVersion: "bv"}
	none := core.SessionBinding{State: core.SessionNone}
	require.NoError(t, none.CheckBind(active))
	require.NoError(t, active.CheckBind(active), "rebinding the same session is idempotent")
	other := active
	other.SessionID = "s2"
	require.ErrorIs(t, active.CheckBind(other), core.ErrConflict)
	older := active
	older.Generation = 0
	require.ErrorIs(t, active.CheckBind(older), core.ErrInvalid)
	reserved := active.Reset()
	require.Equal(t, core.SessionBinding{State: core.SessionReserved, Generation: 2}, reserved)
	require.Equal(t, reserved, reserved.Reset(), "reset is idempotent until the reserved generation is used")
	require.ErrorIs(t, reserved.CheckBind(active), core.ErrConflict, "generations never go back")
	next := active
	next.Generation, next.SessionID = 2, "s2"
	require.NoError(t, reserved.CheckBind(next))
}

func TestTurnPhaseOnlyMovesForward(t *testing.T) {
	turn := core.Turn{ID: "turn:1", Status: core.TurnQueued}.Claim("w", t0, time.Minute, time.Hour)
	require.Equal(t, t0.Add(time.Hour), turn.Schedule.DeadlineAt)
	turn, err := turn.Apply(core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "k1"}})
	require.NoError(t, err)
	turn, err = turn.Apply(core.TurnUpdate{Phase: core.PhaseObserve, Session: core.TurnSession{SessionID: "s1"}})
	require.NoError(t, err)
	_, err = turn.Apply(core.TurnUpdate{Phase: core.PhaseSend})
	require.ErrorIs(t, err, core.ErrInvalid, "phases never go back")
	_, err = turn.Apply(core.TurnUpdate{Phase: core.PhaseObserve, Session: core.TurnSession{SessionID: "s2"}})
	require.ErrorIs(t, err, core.ErrConflict, "session id is written once")
	_, err = turn.Apply(core.TurnUpdate{Phase: core.PhaseCreate, Rebind: true, Session: core.TurnSession{Generation: 1, CreationKey: "k2"}})
	require.ErrorIs(t, err, core.ErrInvalid, "rebind must advance the generation")
	rebound, err := turn.Apply(core.TurnUpdate{Phase: core.PhaseCreate, Rebind: true, Session: core.TurnSession{Generation: 2, CreationKey: "k2", Renewed: true}})
	require.NoError(t, err)
	require.Empty(t, rebound.Session.SessionID)
	require.True(t, rebound.Session.Renewed)
	frozen, err := rebound.Apply(core.TurnUpdate{Phase: core.PhaseCreate, Input: core.FrozenInput{ContextFrozen: true, Context: "ctx"}})
	require.NoError(t, err)
	_, err = frozen.Apply(core.TurnUpdate{Phase: core.PhaseCreate, Input: core.FrozenInput{ContextFrozen: true, Context: "other"}})
	require.ErrorIs(t, err, core.ErrConflict, "frozen input is written once")
	empty, err := frozen.Apply(core.TurnUpdate{Phase: core.PhaseCreate, Input: core.FrozenInput{BlocksFrozen: true}})
	require.NoError(t, err)
	require.True(t, empty.Input.BlocksFrozen, "an empty block list can be frozen")
}

func TestTurnEligibilityHonoursLeaseWaitAndStop(t *testing.T) {
	turn := core.Turn{ID: "turn:1", Status: core.TurnQueued}
	require.True(t, turn.EligibleAt(t0).IsZero())
	claimed := turn.Claim("w", t0, time.Minute, time.Hour)
	require.Equal(t, t0.Add(time.Minute), claimed.EligibleAt(t0), "a live lease blocks until it expires")
	waiting, err := claimed.Wait(core.TurnWait{Reason: "send_pending", Until: t0.Add(time.Hour)})
	require.NoError(t, err)
	require.Equal(t, t0.Add(time.Hour), waiting.EligibleAt(t0))
	stopped := waiting.RequestStop(t0)
	require.True(t, stopped.EligibleAt(t0).IsZero(), "a stopped waiting turn is claimable at once")
	settled, err := claimed.Settle(core.TurnOutcome{Status: core.TurnStopped}, t0)
	require.NoError(t, err)
	require.Equal(t, t0, settled.Outcome.SettledAt)
	_, err = claimed.Settle(core.TurnOutcome{Status: core.TurnRunning}, t0)
	require.ErrorIs(t, err, core.ErrInvalid)
}

func TestInteractionDecidesOnce(t *testing.T) {
	ix := core.Interaction{ID: "ix", Status: core.InteractionPending, ApproverID: "u", BindingID: "binding", AppID: "app", TenantKey: "tenant", ExpiresAt: t0.Add(time.Minute)}
	_, _, err := ix.Decide(core.InteractionDecision{Allow: true, DeciderID: "intruder"}, t0)
	require.ErrorIs(t, err, core.ErrApprovalForbidden)
	decided, changed, err := ix.Decide(core.InteractionDecision{Allow: true, DeciderID: "u"}, t0)
	require.NoError(t, err)
	require.True(t, changed)
	again, changed, err := decided.Decide(core.InteractionDecision{Allow: true, DeciderID: "u"}, t0)
	require.NoError(t, err)
	require.False(t, changed, "a repeated click returns the same decision")
	require.Equal(t, core.InteractionAllowed, again.Status)
	_, _, err = decided.Decide(core.InteractionDecision{Allow: false, DeciderID: "u"}, t0)
	require.ErrorIs(t, err, core.ErrConflict)
	_, _, err = ix.Decide(core.InteractionDecision{Allow: true, DeciderID: "u"}, t0.Add(time.Minute))
	require.ErrorIs(t, err, core.ErrApprovalExpired)
}

func TestInteractionRejectsStaleBinding(t *testing.T) {
	ix := core.Interaction{ID: "ix2", Status: core.InteractionPending, ApproverID: "u", BindingID: "binding", BindingEpoch: 2, AppID: "app", TenantKey: "tenant", ExpiresAt: t0.Add(time.Minute)}
	// 坐标完全一致：允许决定。
	_, _, err := ix.Decide(core.InteractionDecision{Allow: true, DeciderID: "u", BindingID: "binding", BindingEpoch: 2, AppID: "app", TenantKey: "tenant"}, t0)
	require.NoError(t, err)
	stale := core.Interaction{ID: "ix3", Status: core.InteractionPending, ApproverID: "u", BindingID: "binding", BindingEpoch: 2, AppID: "app", TenantKey: "tenant", ExpiresAt: t0.Add(time.Minute)}
	for _, decision := range []core.InteractionDecision{
		{Allow: true, DeciderID: "u", BindingID: "other", BindingEpoch: 2, AppID: "app", TenantKey: "tenant"},
		{Allow: true, DeciderID: "u", BindingID: "binding", BindingEpoch: 1, AppID: "app", TenantKey: "tenant"},
		{Allow: true, DeciderID: "u", BindingID: "binding", BindingEpoch: 2, AppID: "app", TenantKey: "other-tenant"},
	} {
		_, _, err := stale.Decide(decision, t0)
		require.ErrorIs(t, err, core.ErrApprovalForbidden)
	}
}

func TestDeliveryBodyIsTaggedUnion(t *testing.T) {
	for _, body := range []core.DeliveryBody{core.TextBody("hi"), core.ArtifactBody(core.Artifact{Kind: "image", FileID: "f"})} {
		data, err := json.Marshal(body)
		require.NoError(t, err)
		var decoded core.DeliveryBody
		require.NoError(t, json.Unmarshal(data, &decoded))
		require.Equal(t, body, decoded)
	}
	var decoded core.DeliveryBody
	require.ErrorIs(t, json.Unmarshal([]byte(`{"type":"sticker"}`), &decoded), core.ErrUnknownBody)
	_, err := json.Marshal(core.DeliveryBody{})
	require.ErrorIs(t, err, core.ErrUnknownBody)
}

func TestDeliveryIdentityAndRetry(t *testing.T) {
	source := core.DeliverySource{Kind: core.SourceTurn, ID: "turn:1"}
	a := core.NewDelivery(source, 0, "answer", "b", core.ReplyTarget{Mode: "reply"}, core.TextBody("x"))
	b := core.NewDelivery(source, 0, "answer", "b", core.ReplyTarget{Mode: "reply"}, core.TextBody("x"))
	require.Equal(t, a.ID, b.ID)
	require.Equal(t, a.UUID, b.UUID)
	require.True(t, a.ValidNew(source))
	require.False(t, a.ValidNew(core.DeliverySource{Kind: core.SourceTurn, ID: "turn:2"}))
	claimed := a.Claim("w", t0, time.Minute)
	require.True(t, claimed.Holds(claimed.Lease, t0))
	failed, err := claimed.Complete(core.DeliveryResult{Status: core.DeliveryFailed, ErrorKind: "x"})
	require.NoError(t, err)
	retried, err := failed.Retry()
	require.NoError(t, err)
	require.Equal(t, core.DeliveryPending, retried.Status)
	require.Zero(t, retried.Attempt)
	require.Equal(t, a.UUID, retried.UUID)
	_, err = retried.Retry()
	require.ErrorIs(t, err, core.ErrConflict)
}
