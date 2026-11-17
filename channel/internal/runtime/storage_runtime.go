// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"time"
)

// SessionHistory 保留每代已绑定 Session，独立于短期 Turn 的清理。
type SessionHistory struct {
	Route     RouteKey       `json:"route"`
	Binding   SessionBinding `json:"binding"`
	CreatedAt time.Time      `json:"created_at"`
}

// SessionHistoryStore 按代分页读取长期绑定事实。
type SessionHistoryStore interface {
	ListSessionHistory(context.Context, RouteKey, uint64, int) ([]SessionHistory, error)
}
