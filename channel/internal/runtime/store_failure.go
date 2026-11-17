// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"io"
	"io/fs"
)

// StoreFailure 标记宿主 resolver 内部真实持久层故障，供共享 Runtime 结束并交由宿主恢复。
// Console、渠道 API 和业务校验失败不得使用此标记。
type StoreFailure interface {
	error
	StoreFailure() bool
}

type storeFailure struct{ cause error }

// Error exposes only a controlled category; driver errors may contain DSNs,
// queries, paths or credentials. Unwrap retains the cause for trusted callers.
func (e *storeFailure) Error() string      { return "runtime store failure: " + storeFailureKind(e.cause) }
func (e *storeFailure) Unwrap() error      { return e.cause }
func (e *storeFailure) StoreFailure() bool { return true }

func storeFailureKind(cause error) string {
	var pathError *fs.PathError
	switch {
	case errors.Is(cause, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(cause, context.Canceled):
		return "canceled"
	case errors.Is(cause, fs.ErrPermission):
		return "permission"
	case errors.Is(cause, io.EOF), errors.Is(cause, io.ErrUnexpectedEOF), errors.Is(cause, io.ErrClosedPipe), errors.As(cause, &pathError):
		return "io"
	default:
		return "generic"
	}
}

// NewStoreFailure 包装真实 Store 错误且保留哨兵判定；nil 原样返回。
func NewStoreFailure(cause error) error {
	if cause == nil {
		return nil
	}
	return &storeFailure{cause: cause}
}
func isStoreFailure(err error) bool {
	var failure StoreFailure
	return errors.As(err, &failure) && failure.StoreFailure()
}
