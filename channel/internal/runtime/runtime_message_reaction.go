// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"time"
)

const reactionRefreshInterval = 30 * time.Second

func (r *Runtime) processMessageReaction(ctx context.Context, connection Connection, delivery Delivery) error {
	reporter, ok := providerPort[MessageReactionReporter](connection.Provider)
	if !ok {
		return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryFailed, ErrorKind: "reaction_unsupported"})
	}
	turn, err := r.store.GetTurn(ctx, delivery.Source.ID)
	if err != nil {
		return NewStoreFailure(err)
	}
	if turn == nil || turn.BindingID != delivery.BindingID {
		return NewStoreFailure(ErrNotFound)
	}
	state := MessageReactionState{}
	if delivery.ReactionState != nil {
		state = *delivery.ReactionState
	}
	want, now := ReactionForTurn(*turn), r.options.Clock()
	// 同状态只读等待；终态延后再验证一次，覆盖临近迁移时的在途请求。
	if !state.CheckedAt.IsZero() && state.Applied == want && !turn.Status.Terminal() && now.Before(state.CheckedAt.Add(reactionRefreshInterval)) {
		return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryPending, NextAttemptAt: now.Add(r.options.PollInterval)})
	}
	if err := checkDeliveryLease(ctx); err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, r.options.DeliveryTimeout)
	err = reporter.SyncMessageReaction(callCtx, delivery.ReplyTarget, want)
	cancel()
	if err != nil {
		return r.failMessageReaction(ctx, delivery, err)
	}
	previous := state
	state = MessageReactionState{Applied: want, CheckedAt: now}
	latest, err := r.store.GetTurn(ctx, turn.ID)
	if err != nil {
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) && !isStoreFailure(err) && !isUnsupportedWork(err) && !errors.Is(err, ErrLeaseLost) {
			// The provider acknowledged this state before shutdown. Preserve it
			// without assuming whether the Turn became terminal concurrently.
			return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryPending, ReactionState: &state, NextAttemptAt: now.Add(r.options.PollInterval)})
		}
		return NewStoreFailure(err)
	}
	if latest == nil {
		return NewStoreFailure(ErrNotFound)
	}
	if turn.Status.Terminal() && latest.Status.Terminal() && ReactionForTurn(*latest) == want && previous.Applied == want && !previous.CheckedAt.IsZero() && !now.Before(previous.CheckedAt.Add(reactionRefreshInterval)) {
		return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryDone, ReactionState: &state})
	}
	next := now.Add(r.options.PollInterval)
	if latest.Status.Terminal() && ReactionForTurn(*latest) == want {
		next = now.Add(reactionRefreshInterval)
	}
	return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryPending, ReactionState: &state, NextAttemptAt: next})
}

func (r *Runtime) failMessageReaction(ctx context.Context, delivery Delivery, err error) error {
	state := MessageReactionState{}
	if delivery.ReactionState != nil {
		state = *delivery.ReactionState
	}
	state.Failures++
	kind, terminal := "reaction_unconfirmed", state.Failures >= r.options.DeliveryMaxAttempts
	var failure DeliveryFailure
	if errors.As(err, &failure) {
		kind = failure.ErrorKind()
		terminal = terminal || !failure.Uncertain() && !failure.Retryable()
	}
	status := DeliveryPending
	if terminal {
		status = DeliveryFailed
	}
	return r.completeDelivery(ctx, delivery, DeliveryResult{Status: status, ReactionState: &state, ErrorKind: kind, NextAttemptAt: r.options.Clock().Add(time.Second * time.Duration(1<<min(state.Failures, 6)))})
}
