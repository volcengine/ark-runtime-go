// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type incrementalHistoryClient struct {
	MAClient
	pages map[string]EventPage
	fail  map[string]error
	reads []string
}

func (c *incrementalHistoryClient) ListEvents(_ context.Context, _ string, q EventQuery) (EventPage, error) {
	c.reads = append(c.reads, q.Page)
	if err := c.fail[q.Page]; err != nil {
		return EventPage{}, err
	}
	return c.pages[q.Page], nil
}

func incrementalFixture() (*incrementalHistoryClient, *historyScanner, *time.Time) {
	now := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	c := &incrementalHistoryClient{pages: map[string]EventPage{
		"":   {Events: []Event{{ID: "input", Type: "user.message"}}, NextPage: "p1"},
		"p1": {Events: []Event{{ID: "tool", Type: "agent.tool_use"}}, NextPage: "p2"},
		"p2": {Events: []Event{{ID: "result", Type: "agent.tool_result", ToolUseID: "tool"}}},
	}}
	return c, &historyScanner{client: c, sessionID: "sesn-1", clock: func() time.Time { return now }}, &now
}

func TestIncrementalHistoryReadsOnlyTailAfterInitialScan(t *testing.T) {
	c, scanner, _ := incrementalFixture()
	_, err := scanner.read(t.Context(), false)
	require.NoError(t, err)
	c.reads = nil
	c.pages["p2"] = EventPage{Events: []Event{{ID: "result", Type: "agent.tool_result", ToolUseID: "tool"}}, NextPage: "p3"}
	c.pages["p3"] = EventPage{Events: []Event{{ID: "message", Type: "agent.message", Content: []TextBlock{{Type: "text", Text: "working"}}}}}
	events, err := scanner.read(t.Context(), false)
	require.NoError(t, err)
	require.Equal(t, []string{"p2", "p3"}, c.reads)
	require.Len(t, events, 4)
	c.reads = nil
	_, err = scanner.read(t.Context(), false)
	require.NoError(t, err)
	require.Equal(t, []string{"p3"}, c.reads)
}

func TestIncrementalHistoryRefreshFindsLateEvents(t *testing.T) {
	c, scanner, now := incrementalFixture()
	_, err := scanner.read(t.Context(), false)
	require.NoError(t, err)
	c.pages["p1"] = EventPage{Events: []Event{{ID: "late", Type: "agent.tool_use"}, {ID: "tool", Type: "agent.tool_use"}}, NextPage: "p2"}
	*now = now.Add(30 * time.Second)
	c.reads = nil
	events, err := scanner.read(t.Context(), false)
	require.NoError(t, err)
	require.Equal(t, []string{"", "p1", "p2"}, c.reads)
	require.Equal(t, "late", events[1].ID)
}

func TestIncrementalHistoryFailureDoesNotAdvanceCache(t *testing.T) {
	c, scanner, _ := incrementalFixture()
	_, err := scanner.read(t.Context(), false)
	require.NoError(t, err)
	c.pages["p2"] = EventPage{Events: []Event{{ID: "uncommitted", Type: "agent.message"}}, NextPage: "p3"}
	c.fail = map[string]error{"p3": errors.New("temporary failure")}
	_, err = scanner.read(t.Context(), false)
	require.EqualError(t, err, "temporary failure")
	delete(c.fail, "p3")
	c.pages["p2"] = EventPage{Events: []Event{{ID: "result", Type: "agent.tool_result", ToolUseID: "tool"}}}
	c.reads = nil
	events, err := scanner.read(t.Context(), false)
	require.NoError(t, err)
	require.Equal(t, []string{"p2"}, c.reads)
	require.Len(t, events, 3)
	require.Equal(t, "result", events[2].ID)
}

func TestIncrementalHistoryVerifiesTerminalAndApprovals(t *testing.T) {
	for _, reason := range []string{"end_turn", "requires_action"} {
		t.Run(reason, func(t *testing.T) {
			c, scanner, _ := incrementalFixture()
			j := &job{Turn: Turn{InputEventID: "input", Session: TurnSession{SessionID: "sesn-1"}}}
			_, err := scanner.readTurn(t.Context(), j)
			require.NoError(t, err)
			c.pages["p2"] = EventPage{Events: []Event{{ID: "idle", Type: "session.status_idle", StopReason: reason}}}
			// 输入在旧 cursor 之前迟到；终态校验必须阻止把另一条输入的结果用于当前轮次。
			c.pages["p1"] = EventPage{Events: []Event{{ID: "foreign", Type: "user.message"}}, NextPage: "p2"}
			c.reads = nil
			events, err := scanner.readTurn(t.Context(), j)
			require.NoError(t, err)
			require.Equal(t, []string{"p2", "", "p1", "p2"}, c.reads)
			require.Equal(t, "unsupported_contract", judgeTurn(events, j).Status)
		})
	}
}

func TestIncrementalHistoryBudgetIncludesCachedPrefix(t *testing.T) {
	c, scanner, _ := incrementalFixture()
	c.pages[""] = EventPage{Events: []Event{{ID: "input", Type: "user.message", Content: []TextBlock{{Type: "text", Text: strings.Repeat("x", 9<<20)}}}}, NextPage: "p2"}
	_, err := scanner.read(t.Context(), false)
	require.NoError(t, err)
	c.pages["p2"] = EventPage{Events: []Event{{ID: "large", Type: "agent.message", Content: []TextBlock{{Type: "text", Text: strings.Repeat("y", 9<<20)}}}}}
	_, err = scanner.read(t.Context(), false)
	require.ErrorIs(t, err, errHistoryLimit)
}

type expiredHistoryCursor struct{}

func (expiredHistoryCursor) Error() string   { return "cursor expired" }
func (expiredHistoryCursor) HTTPStatus() int { return 400 }
func (expiredHistoryCursor) ErrorCode() string {
	return "InvalidParameter.page"
}

func TestIncrementalHistoryExpiredCursorRebuildsFromWindow(t *testing.T) {
	c, scanner, _ := incrementalFixture()
	_, err := scanner.read(t.Context(), false)
	require.NoError(t, err)
	c.fail = map[string]error{"p2": expiredHistoryCursor{}}
	c.pages[""] = EventPage{Events: []Event{{ID: "input", Type: "user.message"}}}
	c.reads = nil
	events, err := scanner.read(t.Context(), false)
	require.NoError(t, err)
	require.Equal(t, []string{"p2", ""}, c.reads)
	require.Len(t, events, 1)
}

func TestIncrementalHistoryRebuildsBeforeReportingCachedBudgetLimit(t *testing.T) {
	c, scanner, _ := incrementalFixture()
	c.pages[""] = EventPage{Events: []Event{{ID: "old", Type: "agent.message", Content: []TextBlock{{Type: "text", Text: strings.Repeat("x", 9<<20)}}}}, NextPage: "p2"}
	_, err := scanner.read(t.Context(), false)
	require.NoError(t, err)
	// 旧大事件被删除，增量前缀暂时仍含它；真实完整历史只剩新的大事件。
	c.pages["p2"] = EventPage{Events: []Event{{ID: "new", Type: "agent.message", Content: []TextBlock{{Type: "text", Text: strings.Repeat("y", 9<<20)}}}}}
	c.pages[""] = c.pages["p2"]
	c.reads = nil
	events, err := scanner.read(t.Context(), false)
	require.NoError(t, err)
	require.Equal(t, []string{"p2", ""}, c.reads)
	require.Equal(t, "new", events[0].ID)
}
