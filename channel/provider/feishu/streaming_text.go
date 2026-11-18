// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/volcengine/ark-runtime-go/channel/internal/markdown"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type textStreamWriter struct {
	provider *Provider
	delivery core.Delivery
	state    core.TextStreamState
	save     core.SaveTextStream
	text     []rune
}

// DeliverTextStream 只发送已确认的最终答案，恢复时复用原卡片和消息。
func (p *Provider) DeliverTextStream(ctx context.Context, d core.Delivery, save core.SaveTextStream) (core.TextStreamResult, error) {
	if ctx == nil || p.client == nil || p.cardKitLimiter == nil || save == nil || !d.Body.IsTextStream() || d.TextStream == nil || !core.CanStreamText(d.Body.Text) || d.UUID == "" {
		return core.TextStreamResult{}, failure{"invalid streaming delivery", false, false, "invalid_stream_delivery"}
	}
	s := *d.TextStream
	if err := (core.TextStreamState{}).ValidateUpdate(s); err != nil {
		return core.TextStreamResult{}, err
	}
	if s.Handle != "" && (!validCOTID(s.Handle) || len(s.Handle) > 20) {
		return core.TextStreamResult{}, core.ErrInvalid
	}
	w := textStreamWriter{provider: p, delivery: d, state: s, save: save, text: []rune(d.Body.Text)}
	if s.Position > len(w.text) || s.PendingPosition > len(w.text) {
		return core.TextStreamResult{}, core.ErrInvalid
	}
	if s.Done {
		return core.TextStreamResult{MessageID: s.MessageID}, nil
	}
	if err := w.publish(ctx); err != nil {
		return core.TextStreamResult{}, err
	}
	if err := w.write(ctx); err != nil {
		return core.TextStreamResult{}, err
	}
	ready := w.state.LastContentAt.Add(w.printWindow())
	if time.Now().Before(ready) {
		return core.TextStreamResult{RetryAt: ready}, nil
	}
	if err := w.configure(ctx, false); err != nil {
		return core.TextStreamResult{}, err
	}
	s = w.state
	s.Done = true
	if err := w.persist(ctx, s); err != nil {
		return core.TextStreamResult{}, err
	}
	return core.TextStreamResult{MessageID: s.MessageID}, nil
}

func (w *textStreamWriter) persist(ctx context.Context, next core.TextStreamState) error {
	if err := w.save(ctx, next); err != nil {
		return err
	}
	w.state = next
	return nil
}

func (w *textStreamWriter) publish(ctx context.Context) error {
	initial := markdown.PrefixEnd(w.delivery.Body.Text, 1)
	if w.state.ImagesAsCode {
		initial = len(w.text)
	}
	if w.state.Handle == "" {
		id, err := w.provider.createTextStream(ctx, w.prefix(initial), len(w.text))
		if w.canQuoteImages(err) {
			if err = w.quoteImages(ctx); err != nil {
				return err
			}
			initial = len(w.text)
			id, err = w.provider.createTextStream(ctx, w.prefix(initial), len(w.text))
		}
		if err != nil {
			return err
		}
		s := w.state
		s.Handle = id
		if err := w.persist(ctx, s); err != nil {
			return err
		}
	}
	if w.state.MessageID != "" {
		return nil
	}
	if !w.state.FirstSendAt.IsZero() && !time.Now().Before(w.state.FirstSendAt.Add(time.Hour)) {
		return failure{"stream send window expired", true, false, "stream_send_window_expired"}
	}
	if w.state.FirstSendAt.IsZero() {
		s := w.state
		s.FirstSendAt = time.Now()
		if err := w.persist(ctx, s); err != nil {
			return err
		}
	}
	content, _ := json.Marshal(map[string]any{"type": "card", "data": map[string]string{"card_id": w.state.Handle}})
	id, err := w.provider.send(ctx, w.delivery, "interactive", string(content))
	if err != nil {
		return err
	}
	s := w.state
	s.MessageID, s.Position, s.PendingPosition, s.LastContentAt = id, initial, initial, time.Now()
	return w.persist(ctx, s)
}

func (w *textStreamWriter) reserve(ctx context.Context, position int) error {
	if w.state.Sequence >= math.MaxInt32 {
		return failure{"stream sequence exhausted", false, false, "stream_sequence_exhausted"}
	}
	s := w.state
	s.Sequence++
	s.PendingPosition = max(s.PendingPosition, position)
	return w.persist(ctx, s)
}

func (w *textStreamWriter) operationID() string {
	return deliveryUUID("stream:" + w.delivery.ID + ":" + strconv.FormatUint(w.state.Sequence, 10))
}

func (w *textStreamWriter) configure(ctx context.Context, open bool) error {
	if err := w.reserve(ctx, w.state.PendingPosition); err != nil {
		return err
	}
	return w.provider.configureTextStream(ctx, w.state, w.operationID(), open, w.delivery.Body.Text)
}

func (w *textStreamWriter) write(ctx context.Context) error {
	if w.state.Position == len(w.text) {
		return nil
	}
	// 飞书可能自动关闭流式；恢复只重开原卡片，永不补发一条新消息。
	if err := w.configure(ctx, true); err != nil {
		return err
	}
	step := max(1, (len(w.text)+3)/4)
	for w.state.Position < len(w.text) {
		end := min(len(w.text), max(w.state.Position+step, w.state.PendingPosition))
		end = markdown.PrefixEnd(w.delivery.Body.Text, end)
		if w.state.ImagesAsCode {
			end = len(w.text)
		}
		if err := w.reserve(ctx, end); err != nil {
			return err
		}
		if err := w.provider.writeTextStream(ctx, w.state.Handle, w.prefix(end), w.state.Sequence, w.operationID()); err != nil {
			if !w.canQuoteImages(err) {
				return err
			}
			if err := w.quoteImages(ctx); err != nil {
				return err
			}
			end = len(w.text)
			if err := w.reserve(ctx, end); err != nil {
				return err
			}
			if err := w.provider.writeTextStream(ctx, w.state.Handle, w.prefix(end), w.state.Sequence, w.operationID()); err != nil {
				return err
			}
		}
		s := w.state
		s.Position, s.LastContentAt = end, time.Now()
		if err := w.persist(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

func (w *textStreamWriter) prefix(end int) string {
	text := string(w.text[:end])
	if w.state.ImagesAsCode {
		if w.delivery.Body.ImageFallbackText != "" {
			return w.delivery.Body.ImageFallbackText
		}
		return markdown.ReplaceImages(w.delivery.Body.Text, func(image markdown.Image) string { return image.Code() })
	}
	return text
}

func (w *textStreamWriter) canQuoteImages(err error) bool {
	var failure core.DeliveryFailure
	return !w.state.ImagesAsCode && errors.As(err, &failure) && failure.ErrorKind() == "invalid_image" && len(markdown.Images(w.delivery.Body.Text)) > 0
}

func (w *textStreamWriter) quoteImages(ctx context.Context) error {
	s := w.state
	s.ImagesAsCode = true
	return w.persist(ctx, s)
}

// printWindow 按卡片实际呈现长度等待，原始路径的代码引用可能长于上传后的图片 key。
func (w *textStreamWriter) printWindow() time.Duration {
	length := utf8.RuneCountInString(w.prefix(len(w.text)))
	step := streamPrintStep(len(w.text))
	return time.Duration((length+step-1)/step)*50*time.Millisecond + 100*time.Millisecond
}
