// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type reactionRuntimeProvider struct {
	runtimeProvider
	reactionMu sync.Mutex
	states     map[string][]core.MessageReaction
}

func (p *reactionRuntimeProvider) SyncMessageReaction(_ context.Context, target core.ReplyTarget, state core.MessageReaction) error {
	p.reactionMu.Lock()
	defer p.reactionMu.Unlock()
	if p.states == nil {
		p.states = map[string][]core.MessageReaction{}
	}
	p.states[target.MessageID] = append(p.states[target.MessageID], state)
	return nil
}

func (p *reactionRuntimeProvider) has(messageID string, state core.MessageReaction) bool {
	p.reactionMu.Lock()
	defer p.reactionMu.Unlock()
	for _, got := range p.states[messageID] {
		if got == state {
			return true
		}
	}
	return false
}

func TestRuntimeMessageReactionFollowsQueuedRunningAndDone(t *testing.T) {
	f, p := &conformance.MAServer{}, &reactionRuntimeProvider{}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	next := runtimeMessage()
	next.MessageID, next.ProviderEventID = "second-message", "second-event"
	next.TrackReactions = true
	second := h.enqueue(next)
	h.run()
	require.Eventually(t, func() bool { return p.has(next.MessageID, core.ReactionQueued) }, time.Second, 5*time.Millisecond)
	f.AppendAfterInput(t, conformance.AnswerEvents("first")...)
	require.Eventually(t, func() bool { return p.has(next.MessageID, core.ReactionRunning) }, time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool { f.Lock(); defer f.Unlock(); return f.SendCount == 2 }, time.Second, 5*time.Millisecond)
	f.AppendAfterInput(t, conformance.AnswerEvents("second")...)
	require.Eventually(t, func() bool { return p.has(next.MessageID, core.ReactionDone) }, time.Second, 5*time.Millisecond)
	turn, err := h.store.GetTurn(context.Background(), second.Turn.ID)
	require.NoError(t, err)
	require.Equal(t, core.TurnDone, turn.Status)
	require.Eventually(t, func() bool { return p.count() == 2 }, time.Second, 5*time.Millisecond)
}
