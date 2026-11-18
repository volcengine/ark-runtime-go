// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"sync"
	"unicode/utf8"
)

func callTextStream(ctx context.Context, provider TextStreamProvider, d Delivery, save SaveTextStream) (result TextStreamResult, err error) {
	defer func() {
		if recover() != nil {
			err = errProgressPanic
		}
	}()
	return provider.DeliverTextStream(ctx, d, save)
}

func (r *Runtime) processTextStream(ctx context.Context, connection Connection, d Delivery) error {
	provider, supported := providerPort[TextStreamProvider](connection.Provider)
	store, canSave := r.store.(TextStreamStore)
	if !supported || !canSave || d.TextStream == nil {
		return r.completeDelivery(ctx, d, DeliveryResult{Status: DeliveryFailed, ErrorKind: "streaming_unsupported"})
	}
	state := *d.TextStream
	if state.Failures >= r.options.DeliveryMaxAttempts {
		return r.completeDelivery(ctx, d, DeliveryResult{Status: DeliveryFailed, ErrorKind: "stream_unconfirmed"})
	}
	callCtx, cancel := context.WithTimeout(ctx, r.options.DeliveryTimeout)
	var mu sync.Mutex
	save := func(saveCtx context.Context, next TextStreamState) error {
		mu.Lock()
		defer mu.Unlock()
		if err := callCtx.Err(); err != nil {
			return err
		}
		if next.Failures != state.Failures {
			return ErrInvalid
		}
		if err := state.ValidateUpdate(next); err != nil {
			return err
		}
		if err := checkDeliveryLease(saveCtx); err != nil {
			return err
		}
		if err := store.UpdateTextStream(saveCtx, d.Lease, next); err != nil {
			return progressStoreError(err)
		}
		state = next
		return nil
	}
	result, err := callTextStream(callCtx, provider, d, save)
	cancel()
	mu.Lock()
	defer mu.Unlock()
	if errors.Is(err, ErrLeaseLost) || isStoreFailure(err) {
		return err
	}
	if err == nil && result.MessageID != "" && result.MessageID == state.MessageID && state.Done && state.Position == utf8.RuneCountInString(d.Body.Text) && result.RetryAt.IsZero() {
		return r.completeDelivery(ctx, d, DeliveryResult{Status: DeliveryDone, RemoteMessageID: result.MessageID})
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err == nil && result.MessageID == "" && !result.RetryAt.IsZero() && !state.Done {
		return r.completeDelivery(ctx, d, DeliveryResult{Status: DeliveryPending, NextAttemptAt: result.RetryAt})
	}
	status, kind := DeliveryUncertain, "stream_unconfirmed"
	var failure DeliveryFailure
	if errors.As(err, &failure) && !failure.Uncertain() {
		kind = failure.ErrorKind()
		status = DeliveryPending
		if !failure.Retryable() {
			status = DeliveryFailed
		}
	}
	return r.failTextStream(ctx, d, state, status, kind)
}

func (r *Runtime) failTextStream(ctx context.Context, d Delivery, state TextStreamState, status DeliveryStatus, kind string) error {
	store, ok := r.store.(TextStreamStore)
	if !ok {
		return r.completeDelivery(ctx, d, DeliveryResult{Status: DeliveryFailed, ErrorKind: "streaming_unsupported"})
	}
	if err := checkDeliveryLease(ctx); err != nil {
		return err
	}
	if state.Failures >= r.options.DeliveryMaxAttempts {
		return r.completeDelivery(ctx, d, DeliveryResult{Status: DeliveryFailed, ErrorKind: kind})
	}
	state.Failures++
	if err := store.UpdateTextStream(ctx, d.Lease, state); err != nil {
		return progressStoreError(err)
	}
	if state.Failures >= r.options.DeliveryMaxAttempts {
		status = DeliveryFailed
	}
	return r.completeDelivery(ctx, d, DeliveryResult{Status: status, ErrorKind: kind, NextAttemptAt: r.options.Clock().Add(r.options.PollInterval)})
}
