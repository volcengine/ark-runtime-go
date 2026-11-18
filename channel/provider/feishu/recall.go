// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func normalizeRecall(event *larkim.P2MessageRecalledV1, binding core.Binding) (core.Message, bool, error) {
	if event == nil || event.EventV2Base == nil || event.EventV2Base.Header == nil || event.Event == nil ||
		event.EventV2Base.Header.EventType != "im.message.recalled_v1" || event.EventV2Base.Header.AppID != binding.AppID ||
		event.EventV2Base.Header.EventID == "" || event.EventV2Base.Header.TenantKey == "" || deref(event.Event.MessageId) == "" || deref(event.Event.ChatId) == "" {
		return core.Message{}, false, errors.New("feishu recall event invalid")
	}
	millis, err := strconv.ParseInt(deref(event.Event.RecallTime), 10, 64)
	if err != nil || millis <= 0 {
		return core.Message{}, false, errors.New("feishu recall timestamp invalid")
	}
	id := *event.Event.MessageId
	return core.Message{
		BindingID: binding.ID, ProviderEventID: event.EventV2Base.Header.EventID,
		MessageID: core.RecallMessageID(binding.ID, id), RecalledMessageID: id,
		TenantKey: event.EventV2Base.Header.TenantKey, ChatID: *event.Event.ChatId, ChatType: "recall",
		ReceivedAt: time.UnixMilli(millis),
	}, true, nil
}

func normalizeRecallJSON(raw []byte, binding core.Binding) (core.Message, bool, error) {
	var event larkim.P2MessageRecalledV1
	if err := json.Unmarshal(raw, &event); err != nil {
		return core.Message{}, false, errors.New("feishu recall event invalid")
	}
	return normalizeRecall(&event, binding)
}
