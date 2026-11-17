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

func (c *Client) tokenForm(grant, field, value string) url.Values {
	return url.Values{"grant_type": {grant}, "client_id": {c.appID}, "client_secret": {c.appSecret}, field: {value}}
}

// Refresh 只交换一次 refresh token，结果未知时由调用方决定恢复策略。
func (c *Client) Refresh(ctx context.Context, refreshToken string) (Tokens, error) {
	if strings.TrimSpace(refreshToken) == "" {
		return Tokens{}, failure(ErrorConfiguration, OutcomeRejected, 0)
	}
	payload, status, err := c.request(ctx, http.MethodPost, c.openBaseURL+tokenPath, c.tokenForm("refresh_token", "refresh_token", refreshToken), "", false)
	if err != nil {
		return Tokens{}, err
	}
	if err = responseError(payload, status); err != nil {
		return Tokens{}, err
	}
	return c.parseTokens(payload, status)
}

func (c *Client) parseTokens(payload map[string]json.RawMessage, status int) (Tokens, error) {
	result := Tokens{AccessToken: stringField(payload, "access_token"), RefreshToken: stringField(payload, "refresh_token")}
	expiry, ok := secondsField(payload, "expires_in")
	if strings.TrimSpace(result.AccessToken) == "" || !ok {
		return Tokens{}, failure(ErrorInvalidResponse, OutcomeUnknown, status)
	}
	if raw, present := payload["refresh_token"]; present {
		var token string
		if string(raw) == "null" || json.Unmarshal(raw, &token) != nil {
			return Tokens{}, failure(ErrorInvalidResponse, OutcomeUnknown, status)
		}
	}
	if raw, present := payload["scope"]; present {
		var scope string
		if string(raw) == "null" || json.Unmarshal(raw, &scope) != nil {
			return Tokens{}, failure(ErrorInvalidResponse, OutcomeUnknown, status)
		}
		result.ScopeKnown = true
		result.GrantedScopes = strings.Fields(scope)
	}
	result.ExpiresAt = c.clock().Add(expiry)
	return result, nil
}

// String 返回不包含 access token 或 refresh token 的安全视图。
func (t Tokens) String() string { return "OAuthTokens{redacted}" }

// GoString 返回 Go 格式的安全视图。
func (t Tokens) GoString() string { return t.String() }
