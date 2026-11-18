// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"sync/atomic"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// turnStageCursor 为各执行池保留独立的扫描水位，SQL 条件仅来自已验证的固定枚举。
func (s *store) turnStageCursor(stage core.TurnStage) (*atomic.Uint64, string) {
	switch stage {
	case core.StageCreate:
		return &s.turnStageOffsets[0], "phase IN ('','create') AND "
	case core.StageSubmit:
		return &s.turnStageOffsets[1], "phase IN ('','send') AND "
	case core.StageObserve:
		return &s.turnStageOffsets[2], "phase = 'observe' AND "
	default:
		return &s.turnScanOffset, ""
	}
}
