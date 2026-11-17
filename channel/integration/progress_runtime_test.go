// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type recoveringCOTResolver struct {
	fixtureResolver
	progressCalls atomic.Int32
}

func (r *recoveringCOTResolver) ResolveForDelivery(ctx context.Context, d core.Delivery) (core.Connection, error) {
	if d.Body.Type == core.BodyProgress && r.progressCalls.Add(1) == 1 {
		return core.Connection{}, errors.New("temporary connection failure before any remote request")
	}
	return r.fixtureResolver.ResolveForDelivery(ctx, d)
}

func TestCOTCompletionRetriesResolverBeforeRemoteAttempt(t *testing.T) {
	p := &nativeProgressProvider{}
	b := runtimeBinding()
	b.ProgressProtocol = "native_cot"
	f := &conformance.MAServer{}
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	resolver := &recoveringCOTResolver{fixtureResolver: fixtureResolver{connections: map[string]core.Connection{
		b.ID: {Binding: b, Provider: p, MAClient: newMAForHarness(t, f)},
	}}}
	opts := testRuntimeOptions()
	opts.Resolver = resolver
	runtime, err := core.NewRuntime(store, nil, opts)
	require.NoError(t, err)
	mustEnqueue(t, runtime, b.ID, runtimeMessage())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })
	require.Eventually(t, func() bool { n, _ := p.counts(); return n == 1 }, time.Second, time.Millisecond)
	f.AppendAfterInput(t, conformance.AnswerEvents("answer")...)
	waitLatest(t, store, core.TurnDone, core.DeliveryDone, 4*time.Second)
	_, closes := p.counts()
	require.Equal(t, 1, closes)
	require.EqualValues(t, 2, resolver.progressCalls.Load())
}

type nativeProgressProvider struct {
	runtimeProvider
	progressMu    sync.Mutex
	creates       int
	completions   int
	createUnknown bool
	closeUnknown  bool
	appendUnknown bool
	appends       int
	panicProgress bool
	lastText      string
	exposedTools  int
}

func (p *nativeProgressProvider) ReportDurableProgress(_ context.Context, cp core.ProgressCheckpoint, u core.ProgressUpdate) (core.ProgressCheckpoint, error) {
	p.progressMu.Lock()
	defer p.progressMu.Unlock()
	if p.panicProgress {
		panic("fixture progress failure")
	}
	p.lastText = u.Text
	p.exposedTools += len(u.Steps)
	if cp.Handle == "" {
		p.creates++
		if p.createUnknown {
			return cp, runtimeDeliveryError{kind: "unknown", uncertain: true}
		}
		cp.Handle = "known-cot"
	}
	if u.State != core.ProgressRunning {
		p.completions++
		if p.closeUnknown {
			return cp, runtimeDeliveryError{kind: "unknown", uncertain: true}
		}
	} else {
		p.appends++
		if p.appendUnknown {
			return cp, runtimeDeliveryError{kind: "unknown", uncertain: true}
		}
	}
	return cp, nil
}

func TestCOTUpdateUnknownDoesNotReplayTextDelta(t *testing.T) {
	p := &nativeProgressProvider{appendUnknown: true}
	b := runtimeBinding()
	b.ProgressProtocol, b.StreamProgress = "native_cot", true
	h := newHarnessWith(t, &conformance.MAServer{}, p, b, testRuntimeOptions())
	h.enqueue(runtimeMessage())
	h.run()
	require.Eventually(t, func() bool { n, _ := p.counts(); return n == 1 }, time.Second, time.Millisecond)
	h.fixture.AppendAfterInput(t, conformance.AnswerEvents("final answer")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 4*time.Second)
	p.progressMu.Lock()
	defer p.progressMu.Unlock()
	require.Equal(t, 1, p.appends, "unknown append must not replay even with a known handle")
	require.Equal(t, 1, p.completions, "known handle can still be closed independently")
	require.Equal(t, 1, p.creates)
}

func TestCOTPanicDoesNotBlockFinalReply(t *testing.T) {
	p := &nativeProgressProvider{panicProgress: true}
	b := runtimeBinding()
	b.ProgressProtocol = "native_cot"
	h := newHarnessWith(t, &conformance.MAServer{}, p, b, testRuntimeOptions())
	accepted := h.enqueue(runtimeMessage())
	h.run()
	require.Eventually(t, func() bool {
		turn, err := h.store.GetTurn(context.Background(), accepted.Turn.ID)
		return err == nil && turn.Progress != nil
	}, time.Second, 5*time.Millisecond)
	h.fixture.AppendAfterInput(t, conformance.AnswerEvents("最终答案")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 3*time.Second)
	require.Equal(t, 1, p.count())
}

func TestCOTProcessTextDoesNotExposeToolsWhenHidden(t *testing.T) {
	p := &nativeProgressProvider{}
	b := runtimeBinding()
	b.ProgressProtocol, b.StreamProgress = "native_cot", true
	opts := testRuntimeOptions()
	opts.ProgressInterval = time.Millisecond
	h := newHarnessWith(t, &conformance.MAServer{}, p, b, opts)
	h.enqueue(runtimeMessage())
	h.run()
	h.fixture.AppendAfterInput(t,
		map[string]any{"id": "tool", "type": "agent.tool_use", "name": "hidden-tool"},
		map[string]any{"id": "progress", "type": "agent.message", "content": []map[string]string{{"type": "text", "text": "处理中"}}})
	require.Eventually(t, func() bool {
		p.progressMu.Lock()
		defer p.progressMu.Unlock()
		return p.lastText == "处理中"
	}, 2*time.Second, 5*time.Millisecond)
	p.progressMu.Lock()
	exposed := p.exposedTools
	p.progressMu.Unlock()
	require.Zero(t, exposed)
	h.fixture.AppendAfterInput(t, conformance.AnswerEvents("最终答案")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 3*time.Second)
}

func TestCOTCompletionDoesNotInvokeFinalDeliveryHook(t *testing.T) {
	p := &nativeProgressProvider{}
	b := runtimeBinding()
	b.ProgressProtocol = "native_cot"
	var reports atomic.Int32
	opts := testRuntimeOptions()
	opts.Hooks.AfterDelivery = func(context.Context, core.DeliveryReport) { reports.Add(1) }
	h := newHarnessWith(t, &conformance.MAServer{}, p, b, opts)
	t.Cleanup(func() { require.EqualValues(t, 1, reports.Load(), "COT completion is not the user reply receipt") })
	h.enqueue(runtimeMessage())
	h.run()
	require.Eventually(t, func() bool { creates, _ := p.counts(); return creates == 1 }, time.Second, 5*time.Millisecond)
	h.fixture.AppendAfterInput(t, conformance.AnswerEvents("最终答案")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 3*time.Second)
	require.Len(t, state.export.Deliveries, 2)
	for _, d := range state.export.Deliveries {
		if d.Body.Type == core.BodyProgress {
			require.Equal(t, core.SourceProgress, d.Source.Kind)
		}
	}
}

func (p *nativeProgressProvider) counts() (int, int) {
	p.progressMu.Lock()
	defer p.progressMu.Unlock()
	return p.creates, p.completions
}

func TestCOTCreateUnknownDoesNotCreateAgain(t *testing.T) {
	p := &nativeProgressProvider{createUnknown: true}
	b := runtimeBinding()
	b.ProgressProtocol = "native_cot"
	h := newHarnessWith(t, &conformance.MAServer{}, p, b, testRuntimeOptions())
	h.enqueue(runtimeMessage())
	h.run()
	require.Eventually(t, func() bool { creates, _ := p.counts(); return creates == 1 }, 3*time.Second, 10*time.Millisecond)
	h.fixture.AppendAfterInput(t, conformance.AnswerEvents("最终结果")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 3*time.Second)
	creates, _ := p.counts()
	require.Equal(t, 1, creates)
	require.Equal(t, 1, p.count(), "only the final answer uses normal delivery")
}

func TestCOTAndAnswerHaveIndependentHeads(t *testing.T) {
	p := &nativeProgressProvider{closeUnknown: true}
	b := runtimeBinding()
	b.ProgressProtocol = "native_cot"
	h := newHarnessWith(t, &conformance.MAServer{}, p, b, testRuntimeOptions())
	h.enqueue(runtimeMessage())
	h.run()
	require.Eventually(t, func() bool { creates, _ := p.counts(); return creates == 1 }, 3*time.Second, 10*time.Millisecond)
	h.fixture.AppendAfterInput(t, conformance.AnswerEvents("最终结果")...)
	require.Eventually(t, func() bool {
		ex, err := h.store.Export(context.Background())
		if err != nil || len(ex.Deliveries) != 2 {
			return false
		}
		for _, d := range ex.Deliveries {
			if d.Body.Type == core.BodyText && d.Status != core.DeliveryDone {
				return false
			}
			if d.Body.Type == core.BodyProgress && d.Status != core.DeliveryFailed {
				return false
			}
		}
		return true
	}, 4*time.Second, 10*time.Millisecond)
	require.Equal(t, 1, p.count())
	creates, closes := p.counts()
	require.Equal(t, 1, creates)
	require.Equal(t, 1, closes, "unknown native completion must not be blindly repeated")
}
