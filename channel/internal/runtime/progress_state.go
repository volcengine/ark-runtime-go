// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"time"
)

// ProgressCheckpoint 保存非密过程句柄和游标；不保存文本、工具参数或 token。
type ProgressCheckpoint struct {
	Handle      string    `json:"handle,omitempty"`
	Cursor      string    `json:"cursor,omitempty"`
	Attempted   bool      `json:"attempted,omitempty"`
	Failed      bool      `json:"failed,omitempty"`
	Sequence    uint64    `json:"sequence,omitempty"`
	LastAt      time.Time `json:"last_at,omitempty"`
	ContentHash string    `json:"content_hash,omitempty"`
}

// DurableProgressReporter 由支持原生生命周期的 Provider 实现；调用方先持久写入 Attempted。
type DurableProgressReporter interface {
	ReportDurableProgress(context.Context, ProgressCheckpoint, ProgressUpdate) (ProgressCheckpoint, error)
}

func callDurableProgress(ctx context.Context, reporter DurableProgressReporter, checkpoint ProgressCheckpoint, update ProgressUpdate) (next ProgressCheckpoint, err error) {
	next = checkpoint
	defer func() {
		if recover() != nil {
			next, err = checkpoint, errProgressPanic
		}
	}()
	return reporter.ReportDurableProgress(ctx, checkpoint, update)
}

// ProgressDelivery 是已结算轮次的过程闭合意图，使用独立 Delivery 租约。
type ProgressDelivery struct {
	Checkpoint ProgressCheckpoint `json:"checkpoint"`
	Update     ProgressUpdate     `json:"update"`
}

// SplitTurnDeliveries 校验过程闭合来源，并与普通答案分组以使用独立队首。
func SplitTurnDeliveries(turnID string, deliveries []Delivery) ([]Delivery, []Delivery, error) {
	var replies, progress []Delivery
	for _, d := range deliveries {
		if d.Body.Type == BodyProgress {
			if d.Source != (DeliverySource{Kind: SourceProgress, ID: turnID}) || !d.ValidNew(d.Source) {
				return nil, nil, ErrConflict
			}
			progress = append(progress, d)
		} else {
			replies = append(replies, d)
		}
	}
	return replies, progress, nil
}

func progressCallTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 || timeout > 5*time.Second {
		return 5 * time.Second
	}
	return timeout
}

func (r *Runtime) saveProgress(j *job, checkpoint ProgressCheckpoint) error {
	ctx, cancel := shortContext()
	defer cancel()
	return r.updateTurn(ctx, j, TurnUpdate{Phase: j.Phase, Submit: j.Submit, Progress: &checkpoint})
}

func (r *Runtime) reportNativeProgress(j *job, update ProgressUpdate, flush bool) {
	reporter, ok := providerPort[DurableProgressReporter](r.jobConnection(j).Provider)
	if !ok {
		r.reportTurn(EventProgress, "unsupported", "", j)
		return
	}
	var checkpoint ProgressCheckpoint
	if j.Progress != nil {
		checkpoint = *j.Progress
	}
	if checkpoint.Failed || checkpoint.Attempted {
		return
	}
	hash := digestJSON(struct {
		Steps        []ProgressStep
		TextID, Text string
	}{update.Steps, update.TextID, update.Text})
	now := r.options.Clock()
	if checkpoint.Handle != "" && (checkpoint.ContentHash == hash || !flush && now.Sub(checkpoint.LastAt) < r.options.ProgressInterval) {
		return
	}
	checkpoint.Attempted, checkpoint.LastAt = true, now
	checkpoint.Sequence++
	if err := r.saveProgress(j, checkpoint); err != nil {
		j.progressErr = progressStoreError(err)
		return
	}
	ctx, cancel := context.WithTimeout(j.workContext(), progressCallTimeout(r.options.DeliveryTimeout))
	defer cancel()
	if err := j.checkLease(ctx); err != nil {
		return
	}
	next, err := callDurableProgress(ctx, reporter, checkpoint, update)
	next.Attempted, next.Failed = false, err != nil
	next.ContentHash = hash
	if saveErr := r.saveProgress(j, next); saveErr != nil {
		j.progressErr = progressStoreError(saveErr)
		return
	}
	status := "updated"
	if err != nil {
		status = "unconfirmed"
	}
	r.reportTurn(EventProgress, status, "", j)
}

func progressStoreError(err error) error {
	if errors.Is(err, ErrLeaseLost) || errors.Is(err, context.Canceled) {
		return err
	}
	return NewStoreFailure(err)
}

func (r *Runtime) progressDelivery(j *job, state ProgressState, summary string) []Delivery {
	if j.binding.ProgressProtocol != "native_cot" || j.Progress == nil || j.Progress.Handle == "" {
		return nil
	}
	checkpoint := *j.Progress
	update := ProgressUpdate{TurnID: j.ID, SessionID: j.Session.SessionID, ReplyTarget: ReplyTargetFor(j.msg, j.binding), State: state, Summary: summary, At: r.options.Clock(), Protocol: "native_cot"}
	delivery := NewDelivery(DeliverySource{Kind: SourceProgress, ID: j.ID}, 0, "progress-final", j.BindingID, update.ReplyTarget, DeliveryBody{Type: BodyProgress, Progress: &ProgressDelivery{Checkpoint: checkpoint, Update: update}})
	delivery.NextAttemptAt = checkpoint.LastAt.Add(progressCallTimeout(r.options.DeliveryTimeout))
	return []Delivery{delivery}
}
