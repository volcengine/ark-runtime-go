// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestNormalizePreservesMessageContextMetadata(t *testing.T) {
	event := inbound("user", "group", "text", `{"text":"@_user_1 @_user_2 原文"}`)
	require.NoError(t, json.Unmarshal([]byte(`{"message_id":"om-1","chat_id":"oc-1","chat_type":"group","message_type":"text","content":"{\"text\":\"@_user_1 @_user_2 原文\"}","create_time":"1699999999000","thread_id":"omt-thread","root_id":"om-root","parent_id":"om-parent","mentions":[{"key":"@_user_1","id":{"open_id":"ou-bot"},"name":"机器人"},{"key":"@_user_2","id":{"open_id":"ou-other"},"name":"李四"}]}`), event.Event.Message))
	message, ok, err := normalize(event, core.Binding{ID: "binding", AppID: "app-1", GroupTrigger: "mention"}, "ou-bot")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "@_user_2 原文", message.Text)
	data, err := json.Marshal(message)
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, map[string]any{
		"root_message_id": "om-root", "message_type": "text", "sent_at": "2023-11-14T22:13:19Z",
		"mentions": []any{map[string]any{"key": "@_user_2", "open_id": "ou-other", "name": "李四"}},
	}, got["metadata"])
	require.Equal(t, "omt-thread", message.ThreadID)
	require.Equal(t, "om-parent", message.ParentID)
}
