// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"errors"
	"net/http"
	"testing"
)

func TestResponseFailureRateLimitClassification(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		status, code         int
		retryable, uncertain bool
	}{
		{"http_429_without_business_code", 429, 0, true, false},
		{"v1_chat_limit", 200, 230020, true, false},
		{"v1_chat_limit_http_400", 400, 230020, true, false},
		{"generic_limit", 200, 99991400, true, false},
		{"generic_limit_http_400", 400, 99991400, true, false},
		{"v4_system_limit", 200, 11232, true, false},
		{"v4_chat_limit", 400, 11233, true, false},
		{"ip_denied", 200, 99991401, false, false},
		{"ip_denied_http_400", 400, 99991401, false, false},
		{"unknown_business_code", 200, 429, false, false},
		{"server_error_remains_unknown", 500, 99991400, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := responseFailure(tc.status, tc.code, false, false)
			var result interface {
				Retryable() bool
				Uncertain() bool
			}
			if !errors.As(err, &result) {
				t.Fatalf("response has no delivery classification: %v", err)
			}
			if result.Retryable() != tc.retryable || result.Uncertain() != tc.uncertain {
				t.Fatalf("retryable=%t uncertain=%t, want %t/%t", result.Retryable(), result.Uncertain(), tc.retryable, tc.uncertain)
			}
		})
	}
	if err := responseFailure(http.StatusOK, 0, true, true); err != nil {
		t.Fatalf("confirmed success was rejected: %v", err)
	}
}
