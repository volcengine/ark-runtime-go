// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func reactionContract(t *testing.T, open func(*testing.T, StoreConfig) (TestStore, *Clock)) {
	ctx := context.Background()
	s, clock := open(t, StoreConfig{})
	message := P2PMessage("reaction")
	raw, err := json.Marshal(message)
	require.NoError(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(raw, &fields))
	fields["track_reactions"] = true
	raw, err = json.Marshal(fields)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &message))
	result, err := s.Ingest(ctx, SampleVersion(), message)
	require.NoError(t, err)
	deliveries, err := s.ListDeliveries(ctx, core.DeliveryQuery{})
	require.NoError(t, err)
	require.Len(t, deliveries, 1)
	require.Equal(t, "reaction", string(deliveries[0].Source.Kind))
	require.Equal(t, "reaction", string(deliveries[0].Body.Type))
	reaction, err := s.LeaseDelivery(ctx, "reaction-worker", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, reaction)
	require.NoError(t, s.CompleteDelivery(ctx, reaction.Lease, core.DeliveryResult{Status: core.DeliveryPending, NextAttemptAt: clock.Now(), ReactionState: &core.MessageReactionState{Applied: core.ReactionQueued, CheckedAt: clock.Now()}}))
	turn, err := s.LeaseTurn(ctx, Claim("worker"))
	require.NoError(t, err)
	reply := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: result.Turn.ID}, 0, "answer", "binding", core.ReplyTargetFor(message, SampleBinding()), core.TextBody("done"))
	require.NoError(t, s.SettleTurn(ctx, turn.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{reply}))
	answer, err := s.LeaseDelivery(ctx, "answer-worker", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, answer, "reaction waiting must not block the answer")
	require.Equal(t, core.SourceTurn, answer.Source.Kind)
	require.NoError(t, s.CompleteDelivery(ctx, answer.Lease, core.DeliveryResult{Status: core.DeliveryDone}))
	clock.Advance(48 * time.Hour)
	_, err = s.Prune(ctx, clock.Now().Add(-time.Hour))
	require.NoError(t, err)
	retained, err := s.GetTurn(ctx, turn.Turn.ID)
	require.NoError(t, err)
	require.NotNil(t, retained, "pending reaction must retain its Turn and original message")
	reaction, err = s.LeaseDelivery(ctx, "reaction-restarted", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, reaction)
	require.NoError(t, s.CompleteDelivery(ctx, reaction.Lease, core.DeliveryResult{Status: core.DeliveryDone}))
	_, err = s.Prune(ctx, clock.Now().Add(-time.Hour))
	require.NoError(t, err)
	retained, err = s.GetTurn(ctx, turn.Turn.ID)
	require.NoError(t, err)
	require.Nil(t, retained)
}
