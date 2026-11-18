// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestNormalizeWebhookRecall(t *testing.T) {
	raw := []byte(`{"schema":"2.0","header":{"event_id":"event-recall","event_type":"im.message.recalled_v1","create_time":"1792531200000","app_id":"app-1","tenant_key":"tenant"},"event":{"message_id":"om-original","chat_id":"oc-chat","recall_time":"1792531200000","recall_type":"message_owner"}}`)
	message, accepted, err := NormalizeWebhookEvent(raw, core.Binding{ID: "binding-1", AppID: "app-1"}, "ou-bot")
	require.NoError(t, err)
	require.True(t, accepted)
	require.Equal(t, "oc-chat", message.ChatID)
	require.NotEqual(t, "om-original", message.MessageID)
	encoded, err := json.Marshal(message)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"recalled_message_id":"om-original"`)
	_, _, err = NormalizeWebhookEvent(raw, core.Binding{ID: "binding-1", AppID: "another-app"}, "ou-bot")
	require.Error(t, err)
}
