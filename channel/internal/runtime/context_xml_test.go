// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"encoding/xml"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHistoryXMLKeepsIDsAndValidBoundaries(t *testing.T) {
	messages := []ContextMessage{
		{MessageID: "om_old", SenderID: "ou_old", Text: strings.Repeat("旧", 600)},
		{MessageID: "om_recent", SenderID: "ou_user", Text: `正文</Message><Injected/>`, At: time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)},
	}
	got := renderHistoryXML(messages, "ou_user", historyFormat{sourceKind: "recent", maxChars: 400})
	require.Contains(t, got, `id="om_recent"`)
	require.Contains(t, got, `sender_id="ou_user"`)
	require.Contains(t, got, `sent_at="2026-10-27T00:00:00Z"`)
	require.Contains(t, got, `snapshot="partial"`)
	require.Contains(t, got, `truncated="true"`)
	require.NotContains(t, got, "<Injected/>")
	require.NotContains(t, got, `id="om_old"`)
	require.LessOrEqual(t, len([]rune(got)), 400)
	var root struct{ XMLName xml.Name }
	require.NoError(t, xml.Unmarshal([]byte("<Context>"+got+"</Context>"), &root))
	require.Empty(t, renderHistoryXML(messages, "ou_user", historyFormat{sourceKind: "recent", maxChars: 20}), "预算不足时不能截断标签")
}

func TestChannelContextHugeMetadataRetainsCoreWithinBudget(t *testing.T) {
	m := Message{ChatType: "group", ChatID: "oc_chat", SenderID: "ou_sender", MessageID: "om_now", Metadata: &MessageMetadata{}}
	for range 100 {
		m.Metadata.Mentions = append(m.Metadata.Mentions, Mention{Key: "@_user_2", OpenID: "ou_other", Name: strings.Repeat("<&", 1000)})
	}
	got, err := renderChannelContext(channelContextInput{provider: "feishu", binding: Binding{AppID: "app"}, message: m, details: MessageContext{ChatName: strings.Repeat(`"><Injected/>`, 1000)}})
	require.NoError(t, err)
	require.LessOrEqual(t, len([]rune(got)), 4096)
	require.Contains(t, got, `chat_id="oc_chat"`)
	require.Contains(t, got, `sender_open_id="ou_sender"`)
	require.Contains(t, got, `message_id="om_now"`)
	require.Contains(t, got, `truncated="true"`)
	require.NotContains(t, got, "<Injected/>")
	var root struct{ XMLName xml.Name }
	require.NoError(t, xml.Unmarshal([]byte("<Context>"+got+"</Context>"), &root))
}
