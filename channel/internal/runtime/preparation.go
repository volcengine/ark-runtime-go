// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import "errors"

// PreparationError 标记宿主已区分来源的配置/外部准备失败；真实 Store 故障不得包装。
// Permanent 为 true 时结束该消息，否则只在消息总期限内重试。
type PreparationError interface {
	error
	Permanent() bool
}

type preparationError struct {
	cause     error
	permanent bool
}

func (e *preparationError) Error() string   { return "session preparation failed" }
func (e *preparationError) Unwrap() error   { return e.cause }
func (e *preparationError) Permanent() bool { return e.permanent }

// NewPreparationError 包装非存储准备错误，隐藏上游文本并保留 errors.Is/As。
func NewPreparationError(cause error, permanent bool) error {
	if cause == nil {
		return nil
	}
	return &preparationError{cause: cause, permanent: permanent}
}

func preparationFailure(err error) (bool, bool) {
	var failure PreparationError
	if errors.As(err, &failure) {
		return failure.Permanent(), true
	}
	return false, false
}
