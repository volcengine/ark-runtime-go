// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package storage shares persistence errors with internal assembly code.
package storage

import "errors"

var (
	ErrEmbeddedSchema = errors.New("unsupported database structure")
	ErrSQLSchema      = errors.New("unsupported storage structure")
)
