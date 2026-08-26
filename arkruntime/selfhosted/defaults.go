// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package selfhosted

import "time"

const (
	// DefaultMaxIdle 是 session worker 空闲退出的默认时间。
	DefaultMaxIdle = 60 * time.Second
	// DefaultToolTimeout 是单次本地工具执行的默认超时。
	DefaultToolTimeout = 120 * time.Second
)
