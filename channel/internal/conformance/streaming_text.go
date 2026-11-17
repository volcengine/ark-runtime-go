// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func textStreamContract(t *testing.T, open opener) {
	for _, kind := range []core.BodyType{core.BodyStreamText, core.BodyImageStream} {
		t.Run(string(kind), func(t *testing.T) { textStreamBodyContract(t, open, kind) })
	}
}

func textStreamBodyContract(t *testing.T, open opener, kind core.BodyType) {
	s, clock := open(t, StoreConfig{})
	ctx := t.Context()
	_, err := s.Ingest(ctx, SampleVersion(), SampleMessage("stream-final"))
	require.NoError(t, err)
	turn, err := s.LeaseTurn(ctx, Claim("worker"))
	require.NoError(t, err)
	d := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: turn.Turn.ID}, 0, "answer", turn.Turn.BindingID, core.ReplyTarget{ChatID: "chat"}, core.StreamTextBody("confirmed final"))
	d.Body.Type = kind
	if kind == core.BodyImageStream {
		d.Body.ImageFallbackText = "`![original](/mnt/session/outputs/original.png)`"
	}
	require.NoError(t, s.SettleTurn(ctx, turn.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{d}))
	claimed, err := s.LeaseDelivery(ctx, "sender", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	port, ok := s.(core.TextStreamStore)
	require.True(t, ok, "provided Stores must persist text-stream checkpoints")
	state := core.TextStreamState{Handle: "card", MessageID: "message", Sequence: 2, Position: 2, PendingPosition: 3, FirstSendAt: clock.Now(), LastContentAt: clock.Now(), Failures: 2, ImagesAsCode: kind == core.BodyImageStream}
	stale := claimed.Lease
	stale.Fence++
	require.ErrorIs(t, port.UpdateTextStream(ctx, stale, state), core.ErrLeaseLost)
	require.NoError(t, port.UpdateTextStream(ctx, claimed.Lease, state))
	clock.Advance(2 * time.Minute)
	recovered, err := s.LeaseDelivery(ctx, "recovery", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, recovered)
	require.Equal(t, state, *recovered.TextStream)
	require.Equal(t, "confirmed final", recovered.Body.Text)
	require.Equal(t, d.Body.ImageFallbackText, recovered.Body.ImageFallbackText)
	require.ErrorIs(t, port.UpdateTextStream(ctx, claimed.Lease, state), core.ErrLeaseLost)
	bad := state
	bad.Handle = "other"
	require.ErrorIs(t, port.UpdateTextStream(ctx, recovered.Lease, bad), core.ErrConflict)
	require.NoError(t, s.CompleteDelivery(ctx, recovered.Lease, core.DeliveryResult{Status: core.DeliveryFailed}))
	retried, err := s.RetryDelivery(ctx, recovered.ID)
	require.NoError(t, err)
	state.Failures = 0
	require.Equal(t, state, *retried.TextStream, "manual retry resets failures and keeps original identity/window")
}
