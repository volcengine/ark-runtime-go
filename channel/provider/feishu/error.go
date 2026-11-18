// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"errors"
	"net/http"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

type failure struct {
	message   string
	uncertain bool
	retryable bool
	kind      string
}

func (f failure) Error() string     { return f.message }
func (f failure) Uncertain() bool   { return f.uncertain }
func (f failure) Retryable() bool   { return f.retryable }
func (f failure) ErrorKind() string { return f.kind }

var errHTTPRateLimited = failure{"feishu delivery rejected", false, true, "api_rejected"}

// The typed IM client discards the response when its body is not valid JSON.
// Capture a definite HTTP rejection before handing the response to the SDK.
type sdkHTTPClient struct{ client *http.Client }

func (c sdkHTTPClient) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.client.Do(req)
	if err == nil && resp.StatusCode == http.StatusTooManyRequests {
		_ = resp.Body.Close()
		return nil, errHTTPRateLimited
	}
	return resp, err
}

func transportFailure(err error) error {
	if errors.Is(err, errHTTPRateLimited) {
		return errHTTPRateLimited
	}
	var dial *larkcore.DialFailedError
	if errors.As(err, &dial) {
		return failure{"feishu connection unavailable", false, true, "dial_failed"}
	}
	return failure{"feishu delivery result unknown", true, false, "transport_unknown"}
}

func responseFailure(status, code int, success, hasMessageID bool) error {
	if status == http.StatusOK && success && hasMessageID {
		return nil
	}
	if status == http.StatusTooManyRequests {
		return errHTTPRateLimited
	}
	if status == http.StatusOK && !success {
		return failure{"feishu delivery rejected", false, rateLimitCode(code), "api_rejected"}
	}
	if status >= 400 && status < 500 && code != 0 {
		return failure{"feishu delivery rejected", false, rateLimitCode(code), "api_rejected"}
	}
	return failure{"feishu delivery result unknown", true, false, "response_unknown"}
}

func rateLimitCode(code int) bool {
	switch code {
	case 230020, 99991400, 11232, 11233:
		return true
	default:
		return false
	}
}
