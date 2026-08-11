// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"io"
	"strings"
	"testing"
)

// helper: build a decoder from a canned SSE body.
func decode(t *testing.T, body string) []Event {
	t.Helper()
	dec := NewEventStreamDecoder(io.NopCloser(strings.NewReader(body)))
	var evts []Event
	for dec.Next() {
		evts = append(evts, dec.Event())
	}
	return evts
}

func TestSSE_EventLineExplicitType(t *testing.T) {
	// Classic SSE: server sends `event: <type>`. We should honor it as-is.
	body := "event: chat.completion.chunk\ndata: {\"foo\":\"bar\"}\n\n"
	evts := decode(t, body)
	if len(evts) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evts))
	}
	if evts[0].Type != "chat.completion.chunk" {
		t.Errorf("expected Type from `event:` line, got %q", evts[0].Type)
	}
}

func TestSSE_TypePeekFromJSONData(t *testing.T) {
	// ark-managed-agents flavor: no `event:` line, `type` lives in the
	// JSON payload. Ensure evt.Type is populated by the JSON peek so
	// callers don't have to re-parse evt.Data manually.
	body := "data: {\"type\":\"agent.message\",\"content\":\"hi\"}\n\n"
	evts := decode(t, body)
	if len(evts) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evts))
	}
	if evts[0].Type != "agent.message" {
		t.Errorf("expected JSON peek to populate Type, got %q", evts[0].Type)
	}
}

func TestSSE_ExplicitEventBeatsJSONPeek(t *testing.T) {
	// If the server *does* send `event:`, prefer it over any JSON `type`
	// field (they may differ; the SSE-level type is authoritative).
	body := "event: wire.override\ndata: {\"type\":\"json.other\"}\n\n"
	evts := decode(t, body)
	if len(evts) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evts))
	}
	if evts[0].Type != "wire.override" {
		t.Errorf("expected explicit `event:` to win, got %q", evts[0].Type)
	}
}

func TestSSE_NonJSONDataLeavesTypeEmpty(t *testing.T) {
	// the standard `[DONE]` sentinel — not JSON. Type must stay empty.
	body := "data: [DONE]\n\n"
	evts := decode(t, body)
	if len(evts) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evts))
	}
	if evts[0].Type != "" {
		t.Errorf("expected Type='' for non-JSON payload, got %q", evts[0].Type)
	}
}

func TestSSE_JSONWithoutTypeLeavesTypeEmpty(t *testing.T) {
	// Well-formed JSON that just doesn't carry a `type` field. Peek should
	// no-op and leave Type empty.
	body := "data: {\"foo\":\"bar\"}\n\n"
	evts := decode(t, body)
	if len(evts) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evts))
	}
	if evts[0].Type != "" {
		t.Errorf("expected Type='' when payload lacks type, got %q", evts[0].Type)
	}
}
