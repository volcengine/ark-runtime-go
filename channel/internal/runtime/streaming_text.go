// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// MaxStreamTextRunes 限制流式结果的文本长度。
	MaxStreamTextRunes = 100000
	// MaxStreamTextJSONBytes 为卡片其余结构预留空间，约束编码后的正文大小。
	MaxStreamTextJSONBytes = 24000
)

// CanStreamText 判断最终文本是否适合单张流式卡片；不适合时保留完整普通文本。
func CanStreamText(text string) bool {
	if strings.TrimSpace(text) == "" || !utf8.ValidString(text) || utf8.RuneCountInString(text) > MaxStreamTextRunes {
		return false
	}
	raw, err := json.Marshal(text)
	return err == nil && len(raw) <= MaxStreamTextJSONBytes
}

// TextStreamState 保存非密投递进度，正文仍由 Delivery body 的加密通路持有。
type TextStreamState struct {
	Handle          string    `json:"handle,omitempty"`
	MessageID       string    `json:"message_id,omitempty"`
	Sequence        uint64    `json:"sequence,omitempty"`
	Position        int       `json:"position,omitempty"`
	PendingPosition int       `json:"pending_position,omitempty"`
	FirstSendAt     time.Time `json:"first_send_at,omitempty"`
	LastContentAt   time.Time `json:"last_content_at,omitempty"`
	Done            bool      `json:"done,omitempty"`
	Failures        uint64    `json:"failures,omitempty"`
	// ImagesAsCode 表示当前卡片已因图片被明确拒绝而降级；位置仍按持久正文计数。
	ImagesAsCode bool `json:"images_as_code,omitempty"`
}

func (s TextStreamState) valid() bool {
	if len(s.Handle) > 512 || len(s.MessageID) > 512 || s.Sequence > math.MaxInt32 || s.Failures > 100 ||
		s.Position < 0 || s.PendingPosition < s.Position || s.PendingPosition > MaxStreamTextRunes {
		return false
	}
	if s.Handle == "" && (s.MessageID != "" || s.Sequence != 0 || !s.FirstSendAt.IsZero()) {
		return false
	}
	return s.validPhase()
}

func (s TextStreamState) validPhase() bool {
	if s.MessageID != "" && s.FirstSendAt.IsZero() {
		return false
	}
	if s.Position > 0 && (s.MessageID == "" || s.LastContentAt.IsZero()) {
		return false
	}
	return !s.Done || s.Position > 0 && s.Position == s.PendingPosition && s.Sequence > 0
}

// ValidateUpdate 校验普通 checkpoint 更新；显式人工 Retry 可单独重置失败预算。
func (s TextStreamState) ValidateUpdate(next TextStreamState) error {
	if !next.valid() {
		return ErrInvalid
	}
	if !s.sameIdentity(next) || next.Sequence < s.Sequence || next.Position < s.Position ||
		next.PendingPosition < s.PendingPosition || next.Failures < s.Failures || s.Done && !next.Done {
		return ErrConflict
	}
	if s.ImagesAsCode && !next.ImagesAsCode {
		return ErrConflict
	}
	if !s.LastContentAt.IsZero() && next.LastContentAt.Before(s.LastContentAt) {
		return ErrConflict
	}
	return nil
}

func (s TextStreamState) sameIdentity(next TextStreamState) bool {
	return (s.Handle == "" || next.Handle == s.Handle) && (s.MessageID == "" || next.MessageID == s.MessageID) &&
		(s.FirstSendAt.IsZero() || next.FirstSendAt.Equal(s.FirstSendAt))
}

// TextStreamResult 只在已确认关闭后返回 MessageID；打印等待通过 RetryAt 继续。
type TextStreamResult struct {
	MessageID string
	RetryAt   time.Time
}

// SaveTextStream 在远端操作前后保存当前租约下的非密 checkpoint。
type SaveTextStream func(context.Context, TextStreamState) error

// TextStreamProvider 将已确认最终文本发送到同一条可恢复结果消息。
type TextStreamProvider interface {
	DeliverTextStream(context.Context, Delivery, SaveTextStream) (TextStreamResult, error)
}

// TextStreamStore 以当前活 Delivery 租约原子更新非密进度，不进入 KMS。
type TextStreamStore interface {
	UpdateTextStream(context.Context, Lease, TextStreamState) error
}

// StreamTextBody 构造已确认最终答案的流式投递载荷。
func StreamTextBody(text string) DeliveryBody { return DeliveryBody{Type: BodyStreamText, Text: text} }
