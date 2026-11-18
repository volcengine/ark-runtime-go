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

type shutdownStreamProvider struct {
	Provider
	cancel context.CancelFunc
}

func (p shutdownStreamProvider) DeliverTextStream(ctx context.Context, delivery Delivery, save SaveTextStream) (TextStreamResult, error) {
	next := *delivery.TextStream
	next.Done = true
	if err := save(ctx, next); err != nil {
		return TextStreamResult{}, err
	}
	p.cancel()
	return TextStreamResult{MessageID: next.MessageID}, nil
}

func TestTextStreamCompletesFinalCheckpointDuringShutdown(t *testing.T) {
	now := time.Now()
	state := TextStreamState{Handle: "card", MessageID: "sent", Sequence: 7, Position: 5, PendingPosition: 5, FirstSendAt: now, LastContentAt: now}
	store := &streamBoundaryStore{state: state}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	provider := shutdownStreamProvider{cancel: cancel}
	options, err := (RuntimeOptions{}).Normalize()
	require.NoError(t, err)
	runtime := &Runtime{store: store, options: options}
	d := NewDelivery(DeliverySource{Kind: SourceTurn, ID: "turn"}, 0, "answer", "binding", ReplyTarget{ChatID: "chat"}, StreamTextBody("final"))
	d.TextStream = &state
	err = runtime.processTextStream(ctx, Connection{Provider: provider}, d)
	require.True(t, store.state.Done)
	require.NoError(t, err)
	require.Equal(t, DeliveryDone, store.result.Status)
	require.Equal(t, "sent", store.result.RemoteMessageID)
}

type shutdownReactionProvider struct {
	Provider
	cancel context.CancelFunc
}

func (p shutdownReactionProvider) SyncMessageReaction(context.Context, ReplyTarget, MessageReaction) error {
	p.cancel()
	return nil
}

type contextAwareReactionStore struct {
	*reactionResultStore
	readErr error
}

func (s contextAwareReactionStore) GetTurn(ctx context.Context, id string) (*Turn, error) {
	if err := ctx.Err(); err != nil {
		if s.readErr != nil {
			return nil, s.readErr
		}
		return nil, err
	}
	return s.reactionResultStore.GetTurn(ctx, id)
}

func TestReactionPersistsAppliedStateDuringShutdown(t *testing.T) {
	store := contextAwareReactionStore{reactionResultStore: &reactionResultStore{turn: Turn{ID: "turn", BindingID: "binding", Status: TurnQueued}}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	provider := shutdownReactionProvider{cancel: cancel}
	options, err := (RuntimeOptions{}).Normalize()
	require.NoError(t, err)
	runtime := &Runtime{store: store, options: options}
	d := NewMessageReactionDelivery(store.turn, Message{ChatID: "chat", MessageID: "message"})
	err = runtime.processMessageReaction(ctx, Connection{Provider: provider}, d)
	require.NoError(t, err)
	require.NotNil(t, store.result.ReactionState)
	require.Equal(t, ReactionQueued, store.result.ReactionState.Applied)
}

func TestReactionShutdownPreservesInfrastructureErrors(t *testing.T) {
	for _, cause := range []error{
		NewStoreFailure(context.Canceled),
		errors.Join(ErrLeaseLost, context.Canceled),
		errors.Join(ErrUnsupportedWork, context.Canceled),
	} {
		store := contextAwareReactionStore{
			reactionResultStore: &reactionResultStore{turn: Turn{ID: "turn", BindingID: "binding", Status: TurnQueued}},
			readErr:             cause,
		}
		ctx, cancel := context.WithCancel(t.Context())
		provider := shutdownReactionProvider{cancel: cancel}
		options, err := (RuntimeOptions{}).Normalize()
		require.NoError(t, err)
		runtime := &Runtime{store: store, options: options}
		delivery := NewMessageReactionDelivery(store.turn, Message{ChatID: "chat", MessageID: "message"})
		err = runtime.processMessageReaction(ctx, Connection{Provider: provider}, delivery)
		cancel()
		require.ErrorIs(t, err, cause)
		require.Nil(t, store.result.ReactionState, "a global error must not be mistaken for local cancellation")
	}
}
