// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestMessageMetadataPreservesLegacyHashAndNewFacts(t *testing.T) {
	// 旧版 Message 的完整规范 JSON；新增零值字段不得改变既有持久 hash。
	legacy := `{"admit_until":"0001-01-01T00:00:00Z","binding_id":"b","provider_event_id":"","message_id":"m","tenant_key":"t","chat_id":"c","chat_type":"p2p","text":"hello","received_at":"0001-01-01T00:00:00Z"}`
	var message core.Message
	require.NoError(t, json.Unmarshal([]byte(legacy), &message))
	digest := sha256.Sum256([]byte(legacy))
	require.Equal(t, hex.EncodeToString(digest[:]), core.ContentHash(message))
	oldHash := core.ContentHash(message)
	require.NoError(t, json.Unmarshal([]byte(`{"metadata":{"root_message_id":"root","message_type":"text","sent_at":"2026-10-27T00:00:00Z","mentions":[{"key":"@_user_1","open_id":"ou_a","name":"名字"}]}}`), &message))
	require.Equal(t, oldHash, core.ContentHash(message), "可选来源信息不改变滚动升级中旧 worker 的消息身份")
	inbound := core.NewInboundMessage(message, time.Now())
	encoded, err := core.EncodeStored(inbound)
	require.NoError(t, err)
	var restored core.InboundMessage
	require.NoError(t, core.DecodeStored(encoded, &restored))
	require.Equal(t, inbound.ContentHash, core.ContentHash(restored.Message))
	require.Contains(t, string(encoded), "root_message_id")
}
