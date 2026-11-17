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

func recallMessage(t *testing.T, original core.Message, eventID string) core.Message {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"binding_id": original.BindingID, "provider_event_id": eventID,
		"message_id": "recall-event", "recalled_message_id": original.MessageID,
		"tenant_key": original.TenantKey, "chat_id": original.ChatID, "chat_type": "recall",
	})
	require.NoError(t, err)
	var message core.Message
	require.NoError(t, json.Unmarshal(raw, &message))
	return message
}

func recallContract(t *testing.T, open func(*testing.T, StoreConfig) (TestStore, *Clock)) {
	ctx := context.Background()
	t.Run("DedupSurvivesPinnedOriginal", func(t *testing.T) {
		s, clock := open(t, StoreConfig{})
		original := P2PMessage("retained-recall")
		original.TrackReactions = true
		_, err := s.Ingest(ctx, SampleVersion(), original)
		require.NoError(t, err)
		recall := recallMessage(t, original, "retained-recall-1")
		_, err = s.Ingest(ctx, SampleVersion(), recall)
		require.NoError(t, err)
		claimed, err := s.LeaseTurn(ctx, Claim("stop-worker"))
		require.NoError(t, err)
		require.NoError(t, s.SettleTurn(ctx, claimed.Turn.Lease, core.TurnOutcome{Status: core.TurnStopped}, nil))
		clock.Advance(48 * time.Hour)
		_, err = s.Prune(ctx, clock.Now().Add(-time.Hour))
		require.NoError(t, err)
		next, err := s.Ingest(ctx, SampleVersion(), P2PMessage("after-retained-recall"))
		require.NoError(t, err)
		recall.ProviderEventID = "retained-recall-2"
		result, err := s.Ingest(ctx, SampleVersion(), recall)
		require.NoError(t, err)
		require.True(t, result.Duplicate, "the original retained message must also retain its recall dedup fact")
		turn, err := s.GetTurn(ctx, next.Turn.ID)
		require.NoError(t, err)
		require.True(t, turn.StopRequestedAt.IsZero())
	})
	t.Run("OriginalThreadAndDedup", func(t *testing.T) {
		s, clock := open(t, StoreConfig{})
		original := SampleMessage("recall-first")
		original.ThreadID = "original-thread"
		first, err := s.Ingest(ctx, SampleVersion(), original)
		require.NoError(t, err)
		next := SampleMessage("recall-next")
		next.ThreadID = original.ThreadID
		second, err := s.Ingest(ctx, SampleVersion(), next)
		require.NoError(t, err)
		recall := recallMessage(t, original, "recall-event-1")
		result, err := s.Ingest(ctx, SampleVersion(), recall)
		require.NoError(t, err)
		require.NotNil(t, result.Control)
		require.Nil(t, result.Turn)
		require.NotNil(t, result.Stopped)
		require.Equal(t, first.Turn.ID, result.Stopped.ID)
		require.Equal(t, first.Message.ConversationID, result.Message.ConversationID)
		claimed, err := s.LeaseTurn(ctx, Claim("recall-worker"))
		require.NoError(t, err)
		require.NotNil(t, claimed)
		require.NoError(t, s.SettleTurn(ctx, claimed.Turn.Lease, core.TurnOutcome{Status: core.TurnStopped}, nil))
		clock.Advance(time.Second)
		recall.ProviderEventID = "recall-event-2"
		result, err = s.Ingest(ctx, SampleVersion(), recall)
		require.NoError(t, err)
		require.True(t, result.Duplicate)
		require.Equal(t, first.Turn.ID, result.Control.TargetTurnID)
		remaining, err := s.GetTurn(ctx, second.Turn.ID)
		require.NoError(t, err)
		require.True(t, remaining.StopRequestedAt.IsZero())
	})
	t.Run("UnknownAndForeignMessagesDoNotStop", func(t *testing.T) {
		s, _ := open(t, StoreConfig{})
		original := SampleMessage("known")
		_, err := s.Ingest(ctx, SampleVersion(), original)
		require.NoError(t, err)
		unknown := original
		unknown.MessageID = "unknown-message"
		result, err := s.Ingest(ctx, SampleVersion(), recallMessage(t, unknown, "unknown-recall"))
		require.NoError(t, err)
		require.Nil(t, result.Turn)
		require.Nil(t, result.Control)
		foreign := recallMessage(t, original, "foreign-recall")
		foreign.TenantKey = "another-tenant"
		_, err = s.Ingest(ctx, SampleVersion(), foreign)
		require.ErrorIs(t, err, core.ErrInvalid)
		turns, err := s.ListTurns(ctx, core.TurnQuery{OpenOnly: true})
		require.NoError(t, err)
		require.Len(t, turns, 1)
		require.True(t, turns[0].StopRequestedAt.IsZero())
	})
}
