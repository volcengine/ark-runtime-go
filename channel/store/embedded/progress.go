// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"context"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// MarkProgressDeliveryAttempt 在当前活租约下只登记一次原生过程闭合发送意图。
func (s *store) MarkProgressDeliveryAttempt(ctx context.Context, lease core.Lease) error {
	return s.update(ctx, func(tx storeTx) error {
		record, err := getDelivery(tx, lease.ID)
		if err != nil {
			return err
		}
		if record == nil || !record.Holds(lease, s.now()) {
			return core.ErrLeaseLost
		}
		if record.Source.Kind != core.SourceProgress {
			return core.ErrInvalid
		}
		if record.ProgressAttempted {
			return core.ErrConflict
		}
		old := *record
		record.ProgressAttempted = true
		return s.writeDelivery(tx, &old, *record)
	})
}
