// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type historyPageClient struct {
	MAClient
	pages map[string]EventPage
}

func (c historyPageClient) ListEvents(_ context.Context, _ string, q EventQuery) (EventPage, error) {
	return c.pages[q.Page], nil
}

func TestScanEventsLargeMediaKeepsAnswerAndFiles(t *testing.T) {
	pages := map[string]EventPage{"": {Events: []Event{{ID: "input", Type: "user.message"}}, NextPage: "0"}}
	for i := range 3 {
		raw, err := json.Marshal(map[string]any{"content": []any{
			map[string]any{"type": "image", "source": map[string]string{"type": "base64", "data": strings.Repeat("A", 6<<20)}},
			map[string]any{"type": "document", "title": "report.pdf", "source": map[string]string{"type": "file", "file_id": "file-report"}},
		}})
		require.NoError(t, err)
		pages[fmt.Sprint(i)] = EventPage{Events: []Event{{ID: fmt.Sprint("result-", i), Type: "agent.tool_result", Raw: raw, Content: []TextBlock{{Type: "image"}, {Type: "document"}}}}, NextPage: fmt.Sprint(i + 1)}
	}
	pages["3"] = EventPage{Events: []Event{
		{ID: "answer", Type: "agent.message", Content: []TextBlock{{Type: "text", Text: "完成，见附件。"}}},
		{ID: "idle", Type: "session.status_idle", StopReason: "end_turn"},
	}}
	events, err := scanEvents(t.Context(), historyPageClient{pages: pages}, "sesn-1", nil)
	require.NoError(t, err)
	outcome := judgeTurn(events, &job{Turn: Turn{InputEventID: "input", Session: TurnSession{SessionID: "sesn-1"}}})
	require.Equal(t, "done", outcome.Status)
	require.Equal(t, "完成，见附件。", outcome.Text)
	require.Equal(t, []Artifact{{Kind: "document", FileID: "file-report", FileName: "report.pdf"}}, outcome.Artifacts)
	for _, event := range events {
		require.Less(t, len(event.Raw), 1024)
	}
}

func TestScanEventsDuplicatePagesDoNotConsumeHistoryBudget(t *testing.T) {
	event := Event{ID: "answer", Type: "agent.message", Content: []TextBlock{{Type: "text", Text: strings.Repeat("x", 4<<20)}}}
	pages := map[string]EventPage{}
	for i, cursor := range []string{"", "1", "2", "3", "4"} {
		page := EventPage{Events: []Event{event}}
		if i < 4 {
			page.NextPage = fmt.Sprint(i + 1)
		}
		pages[cursor] = page
	}
	events, err := scanEvents(t.Context(), historyPageClient{pages: pages}, "sesn-1", nil)
	require.NoError(t, err)
	require.Equal(t, []Event{event}, events)
}

func TestScanEventsRetainsConflictAndSizeGuards(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pages map[string]EventPage
		want  error
	}{
		{"different content", map[string]EventPage{"": {Events: []Event{{ID: "same", Type: "agent.message"}}, NextPage: "1"}, "1": {Events: []Event{{ID: "same", Type: "agent.message", Content: []TextBlock{{Type: "text", Text: "changed"}}}}}}, errHistoryConflict},
		{"repeated cursor", map[string]EventPage{"": {NextPage: "1"}, "1": {NextPage: "1"}}, errHistoryLimit},
		{"large retained text", map[string]EventPage{"": {Events: []Event{{ID: "answer", Type: "agent.message", Content: []TextBlock{{Type: "text", Text: strings.Repeat("x", 17<<20)}}}}}}, errHistoryLimit},
		{"large tool arguments", map[string]EventPage{"": {Events: []Event{{ID: "tool", Type: "agent.tool_use", ToolInput: strings.Repeat("x", 17<<20)}}}}, errHistoryLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := scanEvents(t.Context(), historyPageClient{pages: tc.pages}, "sesn-1", nil)
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestScanEventsBoundsEmptyCollections(t *testing.T) {
	for _, tc := range []struct {
		name  string
		event Event
	}{
		{"content", Event{Content: make([]TextBlock, 600000)}},
		{"actions", Event{ActionEventIDs: make([]string, 1100000)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.event.ID, tc.event.Type = "large", "agent.message"
			_, err := scanEvents(t.Context(), historyPageClient{pages: map[string]EventPage{"": {Events: []Event{tc.event}}}}, "sesn-1", nil)
			require.ErrorIs(t, err, errHistoryLimit)
		})
	}
}
