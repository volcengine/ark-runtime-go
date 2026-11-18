// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStreamTextBodyRoundTrip(t *testing.T) {
	body := DeliveryBody{Type: BodyType("stream_text"), Text: "最终答案🙂"}
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	var restored DeliveryBody
	require.NoError(t, json.Unmarshal(raw, &restored))
	require.Equal(t, body, restored)
}

func TestImageStreamBodyPreservesOriginalFallbackAndRequiresHandler(t *testing.T) {
	body := DeliveryBody{Type: BodyImageStream, Text: "![图](img_uploaded)", ImageFallbackText: "`![图](/mnt/session/outputs/original.png)`"}
	d := NewDelivery(DeliverySource{Kind: SourceTurn, ID: "turn"}, 0, "answer", "binding", ReplyTarget{ChatID: "chat"}, body)
	require.True(t, d.ValidNew(d.Source))
	raw, err := EncodeStored(d)
	require.NoError(t, err)
	var restored Delivery
	require.NoError(t, DecodeStored(raw, &restored))
	require.Equal(t, body, restored.Body)
	require.True(t, SupportsDeliveryBody(restored.Body))
	// 新语义必须使用独立 body.type，旧 stream_text handler 无权接管。
	require.NotEqual(t, BodyStreamText, restored.Body.Type)
	d.Body.ImageFallbackText = ""
	require.False(t, d.ValidNew(d.Source))
}

func TestTextStreamStateRejectsRegression(t *testing.T) {
	now := time.Now()
	old := TextStreamState{Handle: "card", MessageID: "message", Sequence: 4, Position: 2, PendingPosition: 3, FirstSendAt: now, LastContentAt: now, Failures: 1}
	require.NoError(t, old.ValidateUpdate(old))
	for _, mutate := range []func(*TextStreamState){
		func(s *TextStreamState) { s.Handle = "other" },
		func(s *TextStreamState) { s.MessageID = "other" },
		func(s *TextStreamState) { s.Sequence-- },
		func(s *TextStreamState) { s.Position-- },
		func(s *TextStreamState) { s.PendingPosition-- },
		func(s *TextStreamState) { s.FirstSendAt = now.Add(time.Second) },
		func(s *TextStreamState) { s.LastContentAt = now.Add(-time.Second) },
		func(s *TextStreamState) { s.Failures = 0 },
		func(s *TextStreamState) { s.Done = true },
	} {
		next := old
		mutate(&next)
		require.Error(t, old.ValidateUpdate(next))
	}
	next := old
	next.Position, next.PendingPosition, next.Done = 3, 3, true
	require.NoError(t, old.ValidateUpdate(next))
	closed := next
	closed.Done = false
	require.Error(t, next.ValidateUpdate(closed))
	next.ImagesAsCode = true
	require.NoError(t, old.ValidateUpdate(next))
	regression := next
	regression.ImagesAsCode = false
	require.Error(t, next.ValidateUpdate(regression), "恢复不能重新打开已经被拒绝的图片解析")
}

func TestTextStreamNewDeliveryAndRetryPreserveIdentity(t *testing.T) {
	d := NewDelivery(DeliverySource{Kind: SourceTurn, ID: "turn"}, 0, "answer", "binding", ReplyTarget{ChatID: "chat"}, StreamTextBody("final"))
	require.True(t, d.ValidNew(d.Source))
	require.NotNil(t, d.TextStream)
	d.TextStream.Handle, d.TextStream.MessageID = "card", "message"
	d.TextStream.FirstSendAt, d.TextStream.Sequence, d.TextStream.Failures = time.Now(), 6, 3
	d.Status = DeliveryFailed
	next, err := d.Retry()
	require.NoError(t, err)
	require.Zero(t, next.TextStream.Failures)
	require.EqualValues(t, 3, d.TextStream.Failures, "retry must not mutate the previous pointer")
	state := *d.TextStream
	state.Failures = 0
	require.Equal(t, state, *next.TextStream)
}

func TestStreamTextEligibilityBoundsEncodedCardSize(t *testing.T) {
	for _, tc := range []struct {
		text string
		want bool
	}{
		{"final🙂", true}, {" ", false},
		{strings.Repeat("a", 23998), true}, {strings.Repeat("a", 23999), false},
		{strings.Repeat("\x01", 4100), false}, {strings.Repeat("中文", 5000), false},
	} {
		require.Equal(t, tc.want, CanStreamText(tc.text))
	}
}
