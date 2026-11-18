// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// MarkProgressDeliveryAttempt 在当前活租约下只登记一次原生过程闭合发送意图。
func (s *store) MarkProgressDeliveryAttempt(ctx context.Context, lease core.Lease) error {
	return s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		delivery, err := s.lockedDelivery(ctx, tx, lease.ID)
		if err != nil {
			return err
		}
		if delivery == nil || !delivery.Holds(lease, s.now(ctx)) {
			return core.ErrLeaseLost
		}
		if delivery.Source.Kind != core.SourceProgress {
			return core.ErrInvalid
		}
		if delivery.ProgressAttempted {
			return core.ErrConflict
		}
		delivery.ProgressAttempted = true
		return s.putDelivery(ctx, tx, *delivery, false)
	})
}
