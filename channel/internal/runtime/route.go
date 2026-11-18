// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// RouteFor 按绑定、租户、会话类型、会话与线程生成稳定隔离键。
func RouteFor(message Message) RouteKey {
	parts, _ := json.Marshal([5]string{message.BindingID, message.TenantKey, message.ChatType, message.ChatID, message.ThreadID})
	hash := sha256.Sum256(parts)
	return RouteKey("route:v1:" + hex.EncodeToString(hash[:]))
}
