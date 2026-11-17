// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type reactionResultStore struct {
	Store
	result DeliveryResult
	turn   Turn
}

func (s *reactionResultStore) GetTurn(context.Context, string) (*Turn, error) {
	next := s.turn
	return &next, nil
}

type reactionRecorder struct{ states []MessageReaction }

func (p *reactionRecorder) SyncMessageReaction(_ context.Context, _ ReplyTarget, state MessageReaction) error {
	p.states = append(p.states, state)
	return nil
}

func (s *reactionResultStore) CompleteDelivery(_ context.Context, _ Lease, result DeliveryResult) error {
	s.result = result
	return nil
}

type reactionUnavailableResolver struct{ ConnectionResolver }

func (reactionUnavailableResolver) ResolveForDelivery(context.Context, Delivery) (Connection, error) {
	return Connection{}, errors.New("temporary connection failure")
}

func TestReactionResolverFailureUsesFailureBudget(t *testing.T) {
	now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	store := &reactionResultStore{}
	r := Runtime{store: store, options: RuntimeOptions{Clock: func() time.Time { return now }, Observer: noopObserver{}, Resolver: reactionUnavailableResolver{}, DeliveryMaxAttempts: 3, PollInterval: time.Second}}
	delivery := NewMessageReactionDelivery(Turn{ID: "turn", BindingID: "binding"}, Message{ChatID: "chat", MessageID: "message"})
	delivery.Attempt = 100
	require.NoError(t, r.processDelivery(context.Background(), delivery))
	require.Equal(t, DeliveryPending, store.result.Status)
	require.NotNil(t, store.result.ReactionState)
	require.EqualValues(t, 1, store.result.ReactionState.Failures)
}

func TestReactionManualRetryResetsFailureBudget(t *testing.T) {
	delivery := NewMessageReactionDelivery(Turn{ID: "turn", BindingID: "binding"}, Message{ChatID: "chat", MessageID: "message"})
	delivery.Status = DeliveryFailed
	delivery.ReactionState = &MessageReactionState{Applied: ReactionRunning, CheckedAt: time.Now(), Failures: 5}
	next, err := delivery.Retry()
	require.NoError(t, err)
	require.Zero(t, next.ReactionState.Failures)
	require.True(t, next.ReactionState.CheckedAt.IsZero())
	require.Equal(t, ReactionRunning, next.ReactionState.Applied)
	require.EqualValues(t, 5, delivery.ReactionState.Failures, "retry must not mutate the previous snapshot")
}

func TestReactionSuccessfulOutcomeWinsLateStop(t *testing.T) {
	turn := Turn{Status: TurnDone, StopRequestedAt: time.Now()}
	require.Equal(t, ReactionDone, ReactionForTurn(turn))
}

func TestReactionTerminalStateIsReconciledAgain(t *testing.T) {
	now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	store := &reactionResultStore{turn: Turn{ID: "turn", BindingID: "binding", Status: TurnQueued}}
	provider := &reactionRecorder{}
	r := Runtime{store: store, options: RuntimeOptions{Clock: func() time.Time { return now }, Observer: noopObserver{}, DeliveryMaxAttempts: 3, PollInterval: time.Second, DeliveryTimeout: time.Second}}
	delivery := NewMessageReactionDelivery(store.turn, Message{ChatID: "chat", MessageID: "message"})
	connection := Connection{Provider: &reactionTestProvider{reactionRecorder: provider}}
	require.NoError(t, r.processMessageReaction(context.Background(), connection, delivery))
	delivery.ReactionState = store.result.ReactionState
	store.turn.Status, store.turn.Schedule.StartedAt = TurnRunning, now
	require.NoError(t, r.processMessageReaction(context.Background(), connection, delivery))
	delivery.ReactionState = store.result.ReactionState
	store.turn.Status = TurnDone
	require.NoError(t, r.processMessageReaction(context.Background(), connection, delivery))
	require.Equal(t, DeliveryPending, store.result.Status)
	delivery.ReactionState = store.result.ReactionState
	now = now.Add(30 * time.Second)
	require.NoError(t, r.processMessageReaction(context.Background(), connection, delivery))
	require.Equal(t, DeliveryDone, store.result.Status)
	require.Equal(t, []MessageReaction{ReactionQueued, ReactionRunning, ReactionDone, ReactionDone}, provider.states)
}

type reactionTestProvider struct {
	Provider
	*reactionRecorder
}
