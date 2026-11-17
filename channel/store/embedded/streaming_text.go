// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"context"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// UpdateTextStream 在活投递租约下更新非密流式状态，复用原敏感载荷引用。
func (s *store) UpdateTextStream(ctx context.Context, lease core.Lease, next core.TextStreamState) error {
	return s.update(ctx, func(tx storeTx) error {
		d, err := getDelivery(tx, lease.ID)
		if err != nil {
			return err
		}
		if d == nil || !d.Holds(lease, s.now()) {
			return core.ErrLeaseLost
		}
		if d.Source.Kind != core.SourceTurn || d.Seq != 0 || d.TextStream == nil {
			return core.ErrInvalid
		}
		if err := d.TextStream.ValidateUpdate(next); err != nil {
			return err
		}
		old := *d
		d.TextStream = &next
		return s.writeDelivery(tx, &old, *d)
	})
}
