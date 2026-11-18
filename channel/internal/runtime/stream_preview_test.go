// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStreamPreviewReconstructsIndexedText(t *testing.T) {
	p := newStreamPreview("sesn-main", "input")
	p.confirm([]Event{{ID: "input", Type: "user.message"}}, displayJob())
	p.accept(Event{Type: "event_start", SessionThreadID: "sthr-main", Preview: &EventPreview{EventID: "message", EventType: "agent.message"}})
	p.accept(Event{Type: "event_delta", SessionThreadID: "sthr-main", Preview: &EventPreview{EventID: "message", Index: 1, Text: "第二块"}})
	p.accept(Event{Type: "event_delta", SessionThreadID: "sthr-main", Preview: &EventPreview{EventID: "message", Text: "第一"}})
	p.accept(Event{Type: "event_delta", SessionThreadID: "sthr-main", Preview: &EventPreview{EventID: "message", Text: "块"}})
	id, text := p.snapshot()
	require.Equal(t, "message", id)
	require.Equal(t, "第一块\n第二块", text)
}

func TestPreviewCannotLeakThinkingOrForeignThread(t *testing.T) {
	for _, event := range []Event{
		{Type: "event_start", SessionThreadID: "sthr-child", Preview: &EventPreview{EventID: "private", EventType: "agent.message"}},
		{Type: "event_start", Preview: &EventPreview{EventID: "private", EventType: "agent.message"}},
		{Type: "event_start", SessionThreadID: "sthr-main", Preview: &EventPreview{EventID: "private", EventType: "agent.thinking"}},
	} {
		p := newStreamPreview("sesn-main", "input")
		p.confirm([]Event{{ID: "input", Type: "user.message"}}, displayJob())
		p.accept(event)
		p.accept(Event{Type: "event_delta", SessionThreadID: "sthr-main", Preview: &EventPreview{EventID: "private", Text: "private"}})
		_, text := p.snapshot()
		require.Empty(t, text)
	}
}

func TestDroppedPreviewDoesNotChangeFinalAnswer(t *testing.T) {
	p := newStreamPreview("sesn-main", "input")
	p.confirm([]Event{displayMessage("old", "old"), {ID: "input", Type: "user.message"}}, displayJob())
	p.accept(Event{Type: "event_start", SessionThreadID: "sthr-main", Preview: &EventPreview{EventID: "old", EventType: "agent.message"}})
	p.accept(Event{Type: "event_delta", SessionThreadID: "sthr-main", Preview: &EventPreview{EventID: "old", Text: "private"}})
	_, text := p.snapshot()
	require.Empty(t, text)
	p.accept(Event{Type: "event_start", SessionThreadID: "sthr-main", Preview: &EventPreview{EventID: "new", EventType: "agent.message"}})
	p.accept(Event{Type: "event_delta", SessionThreadID: "sthr-main", Preview: &EventPreview{EventID: "new", Text: strings.Repeat("x", 16001)}})
	_, text = p.snapshot()
	require.Empty(t, text)
	events := []Event{{ID: "input", Type: "user.message"}, displayMessage("final", "完整结果"), {ID: "idle", Type: "session.status_idle", StopReason: "end_turn"}}
	p.confirm(events, displayJob())
	_, text = p.snapshot()
	require.Equal(t, "完整结果", text)
	require.Equal(t, "完整结果", judgeTurn(events, displayJob()).Text)
}

func TestPreviewDuplicateStartWaitsForHistory(t *testing.T) {
	p := newStreamPreview("sesn-main", "input")
	p.confirm([]Event{{ID: "input", Type: "user.message"}}, displayJob())
	start := Event{Type: "event_start", SessionThreadID: "sthr-main", Preview: &EventPreview{EventID: "message", EventType: "agent.message"}}
	delta := Event{Type: "event_delta", SessionThreadID: "sthr-main", Preview: &EventPreview{EventID: "message", Text: "片段"}}
	p.accept(start)
	p.accept(delta)
	p.accept(start)
	p.accept(delta)
	_, text := p.snapshot()
	require.Empty(t, text, "ambiguous replay must not append duplicate text")
}
