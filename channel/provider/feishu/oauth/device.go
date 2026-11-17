// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package oauth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
)

const tokenPath = "/open-apis/authen/v2/oauth/token"

// Begin 发起一次 Device 授权，不自动补充或假定已授予的 scope。
func (c *Client) Begin(ctx context.Context, scopes []string) (DeviceAuthorization, error) {
	if len(scopes) > 200 {
		return DeviceAuthorization{}, failure(ErrorConfiguration, OutcomeRejected, 0)
	}
	payload, status, err := c.request(ctx, http.MethodPost, c.accountsBaseURL+"/oauth/v1/device_authorization", url.Values{"client_id": {c.appID}, "scope": {strings.Join(scopes, " ")}}, "", true)
	if err != nil {
		return DeviceAuthorization{}, err
	}
	if err = responseError(payload, status); err != nil {
		return DeviceAuthorization{}, err
	}
	result := DeviceAuthorization{DeviceCode: stringField(payload, "device_code"), UserCode: stringField(payload, "user_code"), VerificationURL: stringField(payload, "verification_uri_complete")}
	if raw, present := payload["verification_uri_complete"]; present {
		var complete string
		if string(raw) == "null" || json.Unmarshal(raw, &complete) != nil {
			return DeviceAuthorization{}, failure(ErrorInvalidResponse, OutcomeUnknown, status)
		}
	}
	if result.VerificationURL == "" {
		result.VerificationURL = stringField(payload, "verification_uri")
	}
	expiry, validExpiry := secondsField(payload, "expires_in")
	interval, validInterval := secondsField(payload, "interval")
	// 授权地址允许 query 携带 user code，但必须使用 HTTPS，且不允许 userinfo 或 fragment。
	parsed, urlErr := url.Parse(result.VerificationURL)
	validURLFlag := urlErr == nil && parsed.Host != "" && parsed.Scheme == "https" && parsed.User == nil && parsed.Fragment == ""
	if strings.TrimSpace(result.DeviceCode) == "" || strings.TrimSpace(result.UserCode) == "" || !validExpiry || !validInterval || !validURLFlag {
		return DeviceAuthorization{}, failure(ErrorInvalidResponse, OutcomeUnknown, status)
	}
	result.ExpiresAt = c.clock().Add(expiry)
	result.Interval = interval
	return result, nil
}

// Poll 查询一次 Device 授权状态，不等待、不重试、不调整间隔。
func (c *Client) Poll(ctx context.Context, deviceCode string) (PollResult, error) {
	if strings.TrimSpace(deviceCode) == "" {
		return PollResult{}, failure(ErrorConfiguration, OutcomeRejected, 0)
	}
	payload, status, err := c.request(ctx, http.MethodPost, c.openBaseURL+tokenPath, c.tokenForm("urn:ietf:params:oauth:grant-type:device_code", "device_code", deviceCode), "", false)
	if err != nil {
		return PollResult{}, err
	}
	// OAuth 符号状态优先于同一响应中的数字诊断码。
	if status >= 200 && status < 500 && status != 429 {
		switch stringField(payload, "error") {
		case "authorization_pending":
			return PollResult{Status: PollPending}, nil
		case "slow_down":
			return PollResult{Status: PollSlowDown}, nil
		case "access_denied":
			return PollResult{Status: PollDenied}, nil
		case "expired_token", "invalid_grant":
			return PollResult{Status: PollExpired}, nil
		}
	}
	if err = responseError(payload, status); err != nil {
		return PollResult{}, err
	}
	tokens, err := c.parseTokens(payload, status)
	if err != nil {
		return PollResult{}, err
	}
	return PollResult{Status: PollSucceeded, Token: &tokens}, nil
}

// String 返回不含 device code、user code 或授权 URL 的安全视图。
func (d DeviceAuthorization) String() string { return "DeviceAuthorization{redacted}" }

// GoString 返回 Go 格式的安全视图。
func (d DeviceAuthorization) GoString() string { return d.String() }
