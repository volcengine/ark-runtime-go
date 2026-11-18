// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// MAClientFactory 为指向 baseURL（模拟服务根地址）的 MA 服务创建被测 client。
type MAClientFactory func(t *testing.T, baseURL string) core.MAClient

// RunMAClient 用 MAServer 验证 client 满足 Runtime 依赖的公开 MA 契约：资源投影、分页、
// 事件过滤参数、错误码分类，以及失败的写入不得自动重放。
func RunMAClient(t *testing.T, factory MAClientFactory) {
	start := func(t *testing.T, server *MAServer) core.MAClient {
		t.Helper()
		httpServer := httptest.NewServer(server)
		t.Cleanup(httpServer.Close)
		return factory(t, httpServer.URL)
	}
	binding := core.Binding{ID: "binding", AgentID: "agent", AgentVersion: "v1", EnvironmentID: "env"}

	t.Run("CreateAndProjectSession", func(t *testing.T) {
		server := &MAServer{}
		client := start(t, server)
		session, err := client.CreateSession(context.Background(), core.CreateSessionSpec{Binding: binding, CreationKey: "create-1", LookupKey: "route-1"})
		require.NoError(t, err)
		require.Equal(t, "sesn-demo", session.ID)
		require.Equal(t, "agent", session.AgentID)
		require.Equal(t, "1", session.AgentVersion)
		require.Equal(t, "env", session.EnvironmentID)
		require.Equal(t, "route-1", session.Tags["channel_session_key"])
		server.Lock()
		require.Equal(t, "create-1", server.CreationKey, "creation key travels as Idempotency-Key")
		require.Equal(t, "route-1", server.LookupKey)
		server.Unlock()
		got, err := client.GetSession(context.Background(), "sesn-demo")
		require.NoError(t, err)
		require.Equal(t, "idle", got.Status)
	})

	t.Run("ListSessionsByLookupTag", func(t *testing.T) {
		server := &MAServer{CreateCode: 503, CreatePersistThenError: true}
		client := start(t, server)
		_, err := client.CreateSession(context.Background(), core.CreateSessionSpec{Binding: binding, CreationKey: "create-1", LookupKey: "route-1"})
		require.Error(t, err)
		page, err := client.ListSessions(context.Background(), "route-1", "")
		require.NoError(t, err)
		require.Len(t, page.Sessions, 1)
		require.Equal(t, "route-1", page.Sessions[0].Tags["channel_session_key"])
	})

	t.Run("SendEchoAndInterrupt", func(t *testing.T) {
		server := &MAServer{}
		client := start(t, server)
		sent, err := client.SendMessage(context.Background(), "sesn-demo", "sevt-input", core.UserInput{Text: "hello"})
		require.NoError(t, err)
		require.Len(t, sent.Events, 1)
		require.Equal(t, "sevt-input", sent.Events[0].ID)
		require.Equal(t, "user.message", sent.Events[0].Type)
		require.Equal(t, "hello", sent.Events[0].Content[0].Text)
		_, err = client.SendInterrupt(context.Background(), "sesn-demo", "sevt-stop")
		require.NoError(t, err)
		server.Lock()
		require.Equal(t, []string{"sevt-input"}, server.InputIDs)
		require.Equal(t, []string{"sevt-stop"}, server.InterruptIDs)
		server.Unlock()
	})

	t.Run("ListEventsFiltersAndPages", func(t *testing.T) {
		server := &MAServer{PageSize: 2}
		server.Events = append(server.Events,
			map[string]any{"id": "e1", "type": "user.message", "content": []map[string]string{{"type": "text", "text": "q"}}},
			map[string]any{"id": "e2", "type": "agent.thinking"},
			map[string]any{"id": "e3", "type": "agent.message", "content": []map[string]string{{"type": "text", "text": "a"}}},
			map[string]any{"id": "e4", "type": "session.status_idle", "stop_reason": map[string]string{"type": "end_turn"}},
		)
		client := start(t, server)
		since := time.Date(2026, 10, 13, 9, 2, 3, 0, time.FixedZone("CST", 8*3600))
		query := core.EventQuery{Limit: 200, Types: []string{"user.message", "agent.message", "session.status_idle"}, CreatedAtGte: &since}
		var ids []string
		for i := 0; i < 5; i++ {
			page, err := client.ListEvents(context.Background(), "sesn-demo", query)
			require.NoError(t, err)
			for _, event := range page.Events {
				ids = append(ids, event.ID)
			}
			if page.NextPage == "" {
				break
			}
			query.Page = page.NextPage
		}
		require.Equal(t, []string{"e1", "e3", "e4"}, ids)
		server.Lock()
		require.Equal(t, []string{"user.message", "agent.message", "session.status_idle"}, server.LastTypes, "types are repeated query parameters")
		require.Equal(t, "2026-10-13T01:02:03Z", server.LastCreatedAtGte, "created_at[gte] is RFC3339 UTC")
		server.Unlock()
	})

	t.Run("ProjectsStopReasonAndThread", func(t *testing.T) {
		server := &MAServer{}
		server.Events = append(server.Events,
			map[string]any{"id": "idle", "type": "session.thread_status_idle", "session_thread_id": "sthr-child", "stop_reason": map[string]string{"type": "requires_action"}},
			map[string]any{"id": "err", "type": "session.error", "error": map[string]any{"retry_status": map[string]string{"type": "exhausted"}}},
		)
		client := start(t, server)
		page, err := client.ListEvents(context.Background(), "sesn-demo", core.EventQuery{})
		require.NoError(t, err)
		require.Len(t, page.Events, 2)
		require.Equal(t, "requires_action", page.Events[0].StopReason)
		require.Equal(t, "sthr-child", page.Events[0].SessionThreadID)
		require.Equal(t, "exhausted", page.Events[1].RetryStatus)
	})

	t.Run("ErrorsCarryStatusAndCode", func(t *testing.T) {
		cases := []struct {
			name   string
			server *MAServer
			call   func(core.MAClient) error
			status int
			code   string
		}{
			{"quota", &MAServer{CreateCode: http.StatusTooManyRequests, CreateErrorCode: "SessionQuotaExceeded"}, func(c core.MAClient) error {
				_, err := c.CreateSession(context.Background(), core.CreateSessionSpec{Binding: binding, CreationKey: "k", LookupKey: "l"})
				return err
			}, 429, "SessionQuotaExceeded"},
			{"terminated", &MAServer{SendCode: http.StatusForbidden, SendErrorCode: "OperationDenied.SessionTerminated"}, func(c core.MAClient) error {
				_, err := c.SendMessage(context.Background(), "sesn-demo", "sevt-1", core.UserInput{Text: "hi"})
				return err
			}, 403, "OperationDenied.SessionTerminated"},
			{"requires action", &MAServer{SendCode: http.StatusBadRequest, SendErrorCode: "RequiresActionRejected"}, func(c core.MAClient) error {
				_, err := c.SendMessage(context.Background(), "sesn-demo", "sevt-1", core.UserInput{Text: "hi"})
				return err
			}, 400, "RequiresActionRejected"},
			{"deleted", &MAServer{SessionStatus: map[string]string{"sesn-gone": "deleted"}}, func(c core.MAClient) error {
				_, err := c.GetSession(context.Background(), "sesn-gone")
				return err
			}, 404, "InvalidSession.NotFound"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				client := start(t, tc.server)
				err := tc.call(client)
				require.Error(t, err)
				coded, ok := asAPIError(err)
				require.True(t, ok, "errors must implement core.APIErrorCode")
				require.Equal(t, tc.status, coded.HTTPStatus())
				require.Equal(t, tc.code, coded.ErrorCode())
			})
		}
	})

	t.Run("FailedWritesReturnErrors", func(t *testing.T) {
		server := &MAServer{CreateCode: http.StatusServiceUnavailable, SendCode: http.StatusServiceUnavailable}
		client := start(t, server)
		_, err := client.CreateSession(context.Background(), core.CreateSessionSpec{Binding: binding, CreationKey: "k", LookupKey: "l"})
		require.Error(t, err)
		_, err = client.SendMessage(context.Background(), "sesn-demo", "sevt-1", core.UserInput{Text: "hi"})
		require.Error(t, err)
		server.Lock()
		defer server.Unlock()
		require.Positive(t, server.CreateCount)
		require.Positive(t, server.SendCount)
	})
}

func asAPIError(err error) (core.APIErrorCode, bool) {
	var coded core.APIErrorCode
	ok := errors.As(err, &coded)
	return coded, ok
}
