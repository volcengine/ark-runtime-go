// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestFetchHistoryScopesAndFilters(t *testing.T) {
	var queries []string
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		require.Equal(t, "/open-apis/im/v1/messages", r.URL.Path)
		queries = append(queries, r.URL.RawQuery)
		w.Header().Set("Content-Type", "application/json")
		raw := `{"code":0,"data":{"has_more":false,"items":[
			{"message_id":"om-now","msg_type":"text","create_time":"1700000010000","body":{"content":"{\"text\":\"trigger\"}"},"sender":{"id":"ou-a","sender_type":"user"}},
			{"message_id":"om-3","msg_type":"text","create_time":"1700000003000","body":{"content":"{\"text\":\"第三条\"}"},"sender":{"id":"cli-bot","sender_type":"app"}},
			{"message_id":"om-x","msg_type":"text","create_time":"1700000002500","deleted":true,"body":{"content":"{\"text\":\"已撤回\"}"},"sender":{"id":"ou-b","sender_type":"user"}},
			{"message_id":"om-card","msg_type":"interactive","create_time":"1700000002400","body":{"content":"{}"},"sender":{"id":"cli-bot","sender_type":"app"}},
			{"message_id":"om-2","msg_type":"post","create_time":"1700000002000","body":{"content":"{\"title\":\"\",\"content\":[[{\"tag\":\"text\",\"text\":\"第二条\"}]]}"},"sender":{"id":"ou-b","sender_type":"user"}},
			{"message_id":"om-1","msg_type":"text","create_time":"1700000001000","body":{"content":"{\"text\":\"第一条\"}"},"sender":{"id":"ou-a","sender_type":"user"}}
		]}}`
		var response struct {
			Code int `json:"code"`
			Data struct {
				HasMore bool              `json:"has_more"`
				Items   []*larkim.Message `json:"items"`
			} `json:"data"`
		}
		require.NoError(t, json.Unmarshal([]byte(raw), &response))
		for _, item := range response.Data.Items {
			item.ChatId = str("oc-1")
			item.Sender.TenantKey = str("tenant-1")
			if r.URL.Query().Get("container_id_type") == "thread" {
				item.ThreadId = str("omt-1")
			}
		}
		require.NoError(t, json.NewEncoder(w).Encode(response))
	})
	query := core.HistoryQuery{Message: core.Message{MessageID: "om-now", ChatID: "oc-1", ChatType: "group", TenantKey: "tenant-1", ReceivedAt: time.UnixMilli(1700000010000)}, Since: time.UnixMilli(1699990000000), Limit: 10}
	got, err := p.FetchHistory(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, []core.ContextMessage{
		{MessageID: "om-1", SenderID: "ou-a", Text: "第一条", At: time.UnixMilli(1700000001000)},
		{MessageID: "om-2", SenderID: "ou-b", Text: "第二条", At: time.UnixMilli(1700000002000)},
		{MessageID: "om-3", SenderID: "cli-bot", SenderIsBot: true, Text: "第三条", At: time.UnixMilli(1700000003000)},
	}, got, "oldest first; trigger, recalled and card messages excluded")
	require.Contains(t, queries[0], "container_id_type=chat")
	require.Contains(t, queries[0], "container_id=oc-1")
	require.Contains(t, queries[0], "sort_type=ByCreateTimeDesc")

	query.Message.ThreadID = "omt-1"
	query.Limit = 2
	got, err = p.FetchHistory(context.Background(), query)
	require.NoError(t, err)
	require.Len(t, got, 2, "limit keeps the newest messages")
	require.Equal(t, "第二条", got[0].Text)
	require.Contains(t, queries[1], "container_id_type=thread")
	require.Contains(t, queries[1], "container_id=omt-1")
	require.NotContains(t, queries[1], "start_time=", "thread containers do not support time range parameters")
	require.NotContains(t, queries[1], "end_time=")
}

func TestFetchHistoryFailureIsError(t *testing.T) {
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":230027,"msg":"no permission"}`)
	})
	_, err := p.FetchHistory(context.Background(), core.HistoryQuery{Message: core.Message{ChatID: "oc-1", ChatType: "p2p", TenantKey: "tenant-1"}, Limit: 5})
	require.Error(t, err)
}
