// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package selfhosted

import (
	"context"
	"errors"
	"net/http"
)

// ClassifyWorkerError 把底层请求错误转换成稳定的 WorkerError。
func ClassifyWorkerError(err error) *WorkerError {
	if err == nil {
		return nil
	}
	out := &WorkerError{
		Kind:    WorkerErrorKindNetwork,
		Message: err.Error(),
		Err:     err,
	}
	if errors.Is(err, context.DeadlineExceeded) {
		out.Kind = WorkerErrorKindTimeout
		out.Retryable = true
		return out
	}
	if errors.Is(err, context.Canceled) {
		out.Kind = WorkerErrorKindTimeout
		return out
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		out.Retryable = true
		return out
	}
	out.RequestID = apiErr.RequestID
	switch apiErr.StatusCode {
	case http.StatusUnauthorized:
		out.Kind = WorkerErrorKindAuth
	case http.StatusForbidden:
		out.Kind = WorkerErrorKindPermission
	case http.StatusConflict, http.StatusPreconditionFailed:
		out.Kind = WorkerErrorKindLeaseConflict
	case http.StatusRequestTimeout:
		out.Kind = WorkerErrorKindTimeout
		out.Retryable = true
	case http.StatusTooManyRequests:
		out.Kind = WorkerErrorKindRateLimit
		out.Retryable = true
	default:
		if apiErr.StatusCode >= 500 {
			out.Kind = WorkerErrorKindNetwork
			out.Retryable = true
		} else {
			out.Kind = WorkerErrorKindInvalidResponse
		}
	}
	return out
}
