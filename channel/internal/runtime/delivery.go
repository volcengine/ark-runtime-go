// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"time"
)

const providerDedupeWindow = time.Hour

func (r *Runtime) deliveryWorker(ctx context.Context, owner string) error {
	for ctx.Err() == nil {
		delivery, err := r.store.LeaseDelivery(ctx, owner, r.options.LeaseTTL)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if isUnsupportedWork(err) {
				if r.waitUnsupported(ctx, Observation{}) != nil {
					return nil
				}
				continue
			}
			if errors.Is(err, ErrLeaseLost) {
				if r.waitPoll(ctx) != nil {
					return nil
				}
				continue
			}
			return err
		}
		if delivery == nil {
			if err := r.waitPoll(ctx); err != nil {
				return nil
			}
			continue
		}
		if err := r.runLeasedDelivery(ctx, *delivery); err != nil {
			if isUnsupportedWork(err) {
				if r.waitUnsupported(ctx, Observation{BindingID: delivery.BindingID, DeliveryID: delivery.ID, TurnID: delivery.Source.ID}) != nil {
					return nil
				}
				continue
			}
			if !errors.Is(err, ErrLeaseLost) {
				return err
			}
		}
	}
	return nil
}

// processDelivery 执行一次投递尝试。投递记录自带 BindingID，按它路由连接；同一来源内的顺序
// 由存储层的队首领取保证。
func (r *Runtime) processDelivery(ctx context.Context, delivery Delivery) error {
	if err := deliveryWorkError(delivery); err != nil {
		return err
	}
	if delivery.Body.Type != BodyReaction && (delivery.FirstAttemptAt.IsZero() || !r.options.Clock().Before(delivery.FirstAttemptAt.Add(providerDedupeWindow))) {
		return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryFailed, ErrorKind: "provider_result_window_expired"})
	}
	if !delivery.Body.IsTextStream() && delivery.Body.Type != BodyReaction && delivery.Attempt > r.options.DeliveryMaxAttempts {
		return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryFailed, ErrorKind: "delivery_unconfirmed"})
	}

	connection, ok := r.connections[delivery.BindingID]
	if r.options.Resolver != nil {
		var err error
		connection, err = r.options.Resolver.ResolveForDelivery(ctx, delivery)
		if err != nil {
			if errors.Is(err, ErrLeaseLost) || isStoreFailure(err) || isUnsupportedWork(err) {
				return err
			}
			if delivery.Body.Type == BodyReaction {
				return r.failMessageReaction(ctx, delivery, err)
			}
			if delivery.Body.IsTextStream() {
				if delivery.TextStream == nil {
					return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryFailed, ErrorKind: "streaming_unsupported"})
				}
				return r.failTextStream(ctx, delivery, *delivery.TextStream, DeliveryPending, "connection_unavailable")
			}
			if delivery.Attempt >= r.options.DeliveryMaxAttempts {
				return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryFailed, ErrorKind: "connection_unavailable"})
			}
			return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryPending, NextAttemptAt: r.options.Clock().Add(r.options.PollInterval), ErrorKind: "connection_unavailable"})
		}
		ok = connection.Provider != nil
	}
	defer releaseConnection(connection)
	if !ok || connection.Binding.ID != delivery.BindingID {
		return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryFailed, ErrorKind: "connection_invalid"})
	}
	if delivery.Snapshot != nil {
		b := delivery.Snapshot.Binding
		if b.AppID != connection.Binding.AppID || b.Owner != connection.Binding.Owner || b.BindingEpoch != connection.Binding.BindingEpoch {
			return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryFailed, ErrorKind: "connection_invalid"})
		}
	}
	if err := checkDeliveryLease(ctx); err != nil {
		return err
	}
	var remoteID string
	var err error
	switch delivery.Body.Type {
	case BodyRichText:
		deliverer, ok := providerPort[RichTextDeliverer](connection.Provider)
		if !ok {
			return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryFailed, ErrorKind: "rich_text_unsupported"})
		}
		callCtx, cancel := context.WithTimeout(ctx, r.options.DeliveryTimeout)
		remoteID, err = deliverer.DeliverRichText(callCtx, delivery)
		cancel()
	case BodyReaction:
		return r.processMessageReaction(ctx, connection, delivery)
	case BodyStreamText, BodyImageStream:
		return r.processTextStream(ctx, connection, delivery)
	case BodyProgress:
		reporter, supported := providerPort[DurableProgressReporter](connection.Provider)
		attemptStore, canMark := r.store.(ProgressDeliveryAttemptStore)
		if !supported || !canMark || delivery.Body.Progress == nil {
			return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryFailed, ErrorKind: "progress_unconfirmed"})
		}
		if err := attemptStore.MarkProgressDeliveryAttempt(ctx, delivery.Lease); err != nil {
			if errors.Is(err, ErrConflict) {
				return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryFailed, ErrorKind: "progress_unconfirmed"})
			}
			return err
		}
		callCtx, cancel := context.WithTimeout(ctx, progressCallTimeout(r.options.DeliveryTimeout))
		_, err = callDurableProgress(callCtx, reporter, delivery.Body.Progress.Checkpoint, delivery.Body.Progress.Update)
		cancel()
		if err != nil {
			return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryFailed, ErrorKind: "progress_unconfirmed"})
		}
		remoteID = delivery.Body.Progress.Checkpoint.Handle
	case BodyArtifact, BodySessionFile, BodyOutputFile:
		remoteID, err = r.deliverArtifact(ctx, connection, delivery)
	case BodyText:
		callCtx, cancel := context.WithTimeout(ctx, r.options.DeliveryTimeout)
		remoteID, err = connection.Provider.Deliver(callCtx, delivery)
		cancel()
	default:
		return errors.Join(ErrUnsupportedWork, ErrUnknownBody)
	}
	if err == nil && remoteID != "" {
		return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryDone, RemoteMessageID: remoteID})
	}
	var failure DeliveryFailure
	if errors.As(err, &failure) && !failure.Uncertain() && !failure.Retryable() {
		kind := failure.ErrorKind()
		if delivery.Attempt > 1 || delivery.Status == DeliveryUncertain {
			kind = "delivery_unconfirmed"
		}
		return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryFailed, ErrorKind: kind})
	}
	if delivery.Attempt >= r.options.DeliveryMaxAttempts {
		return r.completeDelivery(ctx, delivery, DeliveryResult{Status: DeliveryFailed, ErrorKind: "delivery_unconfirmed"})
	}
	kind := "provider_result_unknown"
	status := DeliveryUncertain
	if errors.As(err, &failure) {
		kind = failure.ErrorKind()
		if !failure.Uncertain() && failure.Retryable() {
			status = DeliveryPending
		}
	}
	return r.completeDelivery(ctx, delivery, DeliveryResult{Status: status, NextAttemptAt: r.options.Clock().Add(r.options.PollInterval), ErrorKind: kind})
}

// completeDelivery 保留已获得的渠道结果；自身预算耗尽时只重试落盘，禁止重新发往渠道。
func (r *Runtime) completeDelivery(ctx context.Context, delivery Delivery, result DeliveryResult) error {
	for {
		// 发送刚成功时即使正在停机，也保留一次独立的有界结算机会。
		callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.options.DeliveryTimeout)
		err := r.store.CompleteDelivery(callCtx, delivery.Lease, result)
		expired := errors.Is(callCtx.Err(), context.DeadlineExceeded)
		cancel()
		if err == nil {
			break
		}
		if isStoreFailure(err) || isUnsupportedWork(err) || errors.Is(err, ErrLeaseLost) || !expired || !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if err := r.waitPoll(ctx); err != nil {
			return err
		}
		if err := checkDeliveryLease(ctx); err != nil {
			return err
		}
	}
	r.afterDelivery(delivery, result)
	status := map[DeliveryStatus]string{DeliveryDone: "done", DeliveryPending: "retry", DeliveryUncertain: "uncertain", DeliveryFailed: "failed"}[result.Status]
	item := Observation{Name: EventDeliveryResult, Status: status, ErrorKind: result.ErrorKind, BindingID: delivery.BindingID, DeliveryID: delivery.ID, Count: int(delivery.Attempt)}
	if delivery.Source.Kind == SourceTurn {
		item.TurnID = delivery.Source.ID
	}
	if !delivery.CreatedAt.IsZero() && result.Status.Terminal() {
		item.Duration = r.options.Clock().Sub(delivery.CreatedAt)
	}
	r.report(ctx, item)
	return nil
}
