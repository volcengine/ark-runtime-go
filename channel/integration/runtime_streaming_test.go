// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type finalStreamProvider struct {
	runtimeProvider
	streamMu                 sync.Mutex
	creates, sends, finishes int
	allowClose               atomic.Bool
	failOnce                 bool
	text                     string
}

func (p *finalStreamProvider) DeliverTextStream(ctx context.Context, d core.Delivery, save core.SaveTextStream) (core.TextStreamResult, error) {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	s := *d.TextStream
	if s.Handle == "" {
		s.Handle = "card"
		if err := save(ctx, s); err != nil {
			return core.TextStreamResult{}, err
		}
		p.creates++
	}
	if s.MessageID == "" {
		s.FirstSendAt = time.Now()
		if err := save(ctx, s); err != nil {
			return core.TextStreamResult{}, err
		}
		s.MessageID = "stream-result"
		if err := save(ctx, s); err != nil {
			return core.TextStreamResult{}, err
		}
		p.sends++
	}
	if p.failOnce {
		p.failOnce = false
		return core.TextStreamResult{}, runtimeDeliveryError{kind: "stream_unknown", uncertain: true}
	}
	s.Sequence++
	s.Position, s.PendingPosition, s.LastContentAt = utf8.RuneCountInString(d.Body.Text), utf8.RuneCountInString(d.Body.Text), time.Now()
	if err := save(ctx, s); err != nil {
		return core.TextStreamResult{}, err
	}
	p.text = d.Body.Text
	if !p.allowClose.Load() {
		return core.TextStreamResult{RetryAt: time.Now().Add(20 * time.Millisecond)}, nil
	}
	s.Done = true
	if err := save(ctx, s); err != nil {
		return core.TextStreamResult{}, err
	}
	p.finishes++
	return core.TextStreamResult{MessageID: s.MessageID}, nil
}

func (p *finalStreamProvider) streamCounts() (int, int, int) {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	return p.creates, p.sends, p.finishes
}

func TestFinalStreamWaitsForConfirmedHistoryAndDoesNotDuplicateReply(t *testing.T) {
	p := &finalStreamProvider{failOnce: true}
	p.allowClose.Store(true)
	b := runtimeBinding()
	b.StreamFinalAnswer = true
	var reports atomic.Int32
	t.Cleanup(func() { require.EqualValues(t, 1, reports.Load()) })
	opts := testRuntimeOptions()
	opts.Hooks.AfterDelivery = func(context.Context, core.DeliveryReport) { reports.Add(1) }
	h := newHarnessWith(t, &conformance.MAServer{}, p, b, opts)
	a := h.enqueue(runtimeMessage())
	h.run()
	h.fixture.AppendAfterInput(t, map[string]any{"id": "process", "type": "agent.message", "content": []map[string]string{{"type": "text", "text": "我查一下"}}})
	turn, err := h.store.GetTurn(t.Context(), a.Turn.ID)
	require.NoError(t, err)
	require.False(t, turn.Status.Terminal())
	c, _, _ := p.streamCounts()
	require.Zero(t, c)
	h.fixture.AppendAfterInput(t, conformance.AnswerEvents("确定的最终答案")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 3*time.Second)
	require.Len(t, state.export.Deliveries, 1)
	require.Equal(t, core.BodyStreamText, state.export.Deliveries[0].Body.Type)
	require.True(t, state.export.Deliveries[0].TextStream.Done)
	require.EqualValues(t, 1, state.export.Deliveries[0].TextStream.Failures)
	c, s, f := p.streamCounts()
	require.Equal(t, 1, c)
	require.Equal(t, 1, s)
	require.Equal(t, 1, f)
	require.Zero(t, p.count(), "streamed answer must not also send a normal final message")
	require.Eventually(t, func() bool { return reports.Load() == 1 }, time.Second, time.Millisecond)
	p.streamMu.Lock()
	require.Equal(t, "确定的最终答案", p.text)
	p.streamMu.Unlock()
}

func TestFinalStreamDoesNotApplyToFailedTurn(t *testing.T) {
	p := &finalStreamProvider{}
	b := runtimeBinding()
	b.StreamFinalAnswer = true
	h := newHarnessWith(t, &conformance.MAServer{}, p, b, testRuntimeOptions())
	h.enqueue(runtimeMessage())
	h.run()
	h.fixture.AppendAfterInput(t, map[string]any{"id": "failed", "type": "session.error", "error": map[string]string{"retry_status": "terminal", "code": "fixture"}})
	h.waitSettled(core.TurnFailed, core.DeliveryDone, 3*time.Second)
	c, _, _ := p.streamCounts()
	require.Zero(t, c)
	require.Equal(t, 1, p.count())
}

func TestFinalStreamPrintingWaitDoesNotConsumeFailureBudget(t *testing.T) {
	p := &finalStreamProvider{}
	b := runtimeBinding()
	b.StreamFinalAnswer = true
	opts := testRuntimeOptions()
	opts.DeliveryMaxAttempts = 1
	h := newHarnessWith(t, &conformance.MAServer{}, p, b, opts)
	h.enqueue(runtimeMessage())
	h.run()
	h.fixture.AppendAfterInput(t, conformance.AnswerEvents("final")...)
	require.Eventually(t, func() bool { c, s, _ := p.streamCounts(); return c == 1 && s == 1 }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		d, e := h.store.ListDeliveries(t.Context(), core.DeliveryQuery{})
		return e == nil && len(d) == 1 && d[0].Attempt >= 3
	}, time.Second, time.Millisecond)
	p.allowClose.Store(true)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 3*time.Second)
	c, s, _ := p.streamCounts()
	require.Equal(t, 1, c)
	require.Equal(t, 1, s)
}
