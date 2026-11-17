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

func scopedHistoryItem(id, chat, thread, tenant string) *larkim.Message {
	return &larkim.Message{MessageId: str(id), ChatId: str(chat), ThreadId: str(thread), MsgType: str("text"), CreateTime: str("1700000001000"), Body: &larkim.MessageBody{Content: str(`{"text":"可见消息"}`)}, Sender: &larkim.Sender{Id: str("ou-a"), SenderType: str("user"), TenantKey: str(tenant)}}
}

func TestHistoryRejectsItemsOutsideProvenConversation(t *testing.T) {
	query := core.HistoryQuery{Message: core.Message{MessageID: "om-now", TenantKey: "tenant-a", ChatID: "oc-a", ChatType: "group", ThreadID: "omt-a", ReceivedAt: time.UnixMilli(1700000020000)}, Limit: 20}
	for _, tc := range []struct {
		name string
		item *larkim.Message
	}{
		{"other chat", scopedHistoryItem("om-old", "oc-b", "omt-a", "tenant-a")},
		{"other topic", scopedHistoryItem("om-old", "oc-a", "omt-b", "tenant-a")},
		{"other tenant", scopedHistoryItem("om-old", "oc-a", "omt-a", "tenant-b")},
		{"missing chat proof", scopedHistoryItem("om-old", "", "omt-a", "tenant-a")},
		{"missing tenant proof", scopedHistoryItem("om-old", "oc-a", "omt-a", "")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, ok := (&Provider{}).contextMessage(tc.item, query)
			require.False(t, ok)
		})
	}
}

func TestFetchHistoryDeduplicatesOverlappingPages(t *testing.T) {
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		items := []*larkim.Message{scopedHistoryItem("om-2", "oc-a", "", "tenant-a")}
		hasMore := r.URL.Query().Get("page_token") == ""
		if !hasMore {
			items = append(items, scopedHistoryItem("om-1", "oc-a", "", "tenant-a"))
		}
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"has_more": hasMore, "page_token": "page-2", "items": items}}))
	})
	got, err := p.FetchHistory(context.Background(), core.HistoryQuery{Message: core.Message{MessageID: "om-now", ChatID: "oc-a", TenantKey: "tenant-a", ChatType: "group", ReceivedAt: time.UnixMilli(1700000020000)}, Limit: 2})
	require.NoError(t, err)
	require.Len(t, got, 2)
	// 重叠页中的同一消息不能占满条数预算，必须继续取到另一条唯一消息。
	raw, err := json.Marshal(got)
	require.NoError(t, err)
	require.Contains(t, string(raw), "om-1")
}

func TestFetchReferenceChecksMessageIdentityAndConversation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		id, chat  string
		thread    string
		tenant    string
		wantError bool
	}{
		{"same topic", "om-parent", "oc-a", "omt-a", "tenant-a", false},
		{"other chat", "om-parent", "oc-b", "omt-a", "tenant-a", true},
		{"other topic", "om-parent", "oc-a", "omt-b", "tenant-a", true},
		{"other tenant", "om-parent", "oc-a", "omt-a", "tenant-b", true},
		{"missing tenant proof", "om-parent", "oc-a", "omt-a", "", true},
		{"unrelated merged child", "om-child", "oc-a", "omt-a", "tenant-a", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
					tokenResponse(w)
					return
				}
				require.Equal(t, "/open-apis/im/v1/messages/om-parent", r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": []*larkim.Message{scopedHistoryItem(tc.id, tc.chat, tc.thread, tc.tenant)}}}))
			})
			fetcher, ok := any(p).(interface {
				FetchReference(context.Context, core.Message) (core.ContextMessage, error)
			})
			require.True(t, ok, "the provider must expose explicit reference lookup")
			message := core.Message{MessageID: "om-now", ChatID: "oc-a", ThreadID: "omt-a", TenantKey: "tenant-a", ChatType: "group", ReceivedAt: time.UnixMilli(1700000020000)}
			require.NoError(t, json.Unmarshal([]byte(`{"parent_id":"om-parent"}`), &message))
			got, err := fetcher.FetchReference(context.Background(), message)
			if tc.wantError {
				require.Error(t, err)
				require.Empty(t, got.Text)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "可见消息", got.Text)
		})
	}
}

func TestNormalizeRetainsOnlyExplicitParentReference(t *testing.T) {
	event := inbound("user", "group", "text", `{"text":"请评估这个方案"}`)
	event.Event.Message.RootId = str("om-root")
	event.Event.Message.ParentId = str("om-parent")
	got, ok, err := normalize(event, core.Binding{ID: "binding-1", AppID: "app-1", GroupTrigger: "all"}, "ou-bot")
	require.NoError(t, err)
	require.True(t, ok)
	raw, err := json.Marshal(got)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"parent_id":"om-parent"`)
	event.Event.Message.ParentId = nil
	got, ok, err = normalize(event, core.Binding{ID: "binding-1", AppID: "app-1", GroupTrigger: "all"}, "ou-bot")
	require.NoError(t, err)
	require.True(t, ok)
	require.Empty(t, got.ParentID, "root alone is not an explicit reference")
	require.Equal(t, "om-root", got.Metadata.RootMessageID, "根消息作为定位信息保留，不转成显式引用")
}

func TestFetchReferenceFailureIsError(t *testing.T) {
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		_, _ = io.WriteString(w, `{"code":230027,"msg":"no permission"}`)
	})
	fetcher, ok := any(p).(interface {
		FetchReference(context.Context, core.Message) (core.ContextMessage, error)
	})
	require.True(t, ok)
	message := core.Message{MessageID: "om-now", ChatID: "oc-a", TenantKey: "tenant-a", ChatType: "p2p"}
	require.NoError(t, json.Unmarshal([]byte(`{"parent_id":"om-parent"}`), &message))
	_, err := fetcher.FetchReference(context.Background(), message)
	require.Error(t, err)
}
