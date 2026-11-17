// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// recallSourcePinned 让去重事实至少与原消息同寿命，避免原消息被其他投递保留时再次应用停止。
func (s *store) recallSourcePinned(ctx context.Context, tx *sql.Tx, messageID string) (bool, error) {
	message, err := loadOne[core.InboundMessage](ctx, tx, "SELECT data FROM "+s.t.messages+" WHERE id=?", messageID)
	if err != nil || message == nil || message.Message.RecalledMessageID == "" {
		return false, err
	}
	var count int
	err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.t.messages+" WHERE binding_id=? AND provider_message_id=?", message.BindingID, message.Message.RecalledMessageID).Scan(&count)
	return count > 0, err
}
