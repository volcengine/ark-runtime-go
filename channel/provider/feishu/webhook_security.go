// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
)

const webhookSignatureWindow = 5 * time.Minute

func validWebhookTimestamp(value string, now time.Time) bool {
	timestamp, err := strconv.ParseInt(value, 10, 64)
	window := int64(webhookSignatureWindow / time.Second)
	return err == nil && timestamp > 0 && timestamp >= now.Unix()-window && timestamp <= now.Unix()+window
}

func (h webhookHandler) isChallenge(ctx context.Context, req *larkevent.EventReq) (challenge bool) {
	// SDK Handle 对恶意密文有 panic 恢复；预分类也不能把解密异常暴露给 HTTP server。
	defer func() {
		if recover() != nil {
			challenge = false
		}
	}()
	ciphertext, err := h.dispatch.ParseReq(ctx, req)
	if err != nil {
		return false
	}
	plaintext, err := h.dispatch.DecryptEvent(ctx, ciphertext)
	if err != nil {
		return false
	}
	var envelope struct {
		Type string `json:"type"`
	}
	return json.Unmarshal([]byte(plaintext), &envelope) == nil && envelope.Type == string(larkevent.ReqTypeChallenge)
}
