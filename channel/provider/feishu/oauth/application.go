// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package oauth

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// ApplicationUserScopes 查询并冻结应用当前开放的用户权限，不把 tenant 权限作为用户权限。
func (c *Client) ApplicationUserScopes(ctx context.Context) ([]string, error) {
	body, _ := json.Marshal(map[string]string{"app_id": c.appID, "app_secret": c.appSecret})
	payload, status, err := c.requestBody(ctx, http.MethodPost, c.openBaseURL+"/open-apis/auth/v3/tenant_access_token/internal", bytes.NewReader(body), "application/json", "", false)
	if err != nil {
		return nil, err
	}
	if err = requiredSuccess(payload, status); err != nil {
		return nil, err
	}
	token := stringField(payload, "tenant_access_token")
	if strings.TrimSpace(token) == "" {
		return nil, failure(ErrorInvalidResponse, OutcomeUnknown, status)
	}
	payload, status, err = c.request(ctx, http.MethodGet, c.openBaseURL+"/open-apis/application/v6/applications/"+url.PathEscape(c.appID)+"?lang=zh_cn", nil, token, false)
	if err != nil {
		return nil, err
	}
	if err = requiredSuccess(payload, status); err != nil {
		return nil, err
	}
	var data struct {
		App *struct {
			AppID  string `json:"app_id"`
			Scopes *[]struct {
				Scope      string   `json:"scope"`
				TokenTypes []string `json:"token_types"`
			} `json:"scopes"`
		} `json:"app"`
	}
	if json.Unmarshal(payload["data"], &data) != nil || data.App == nil || data.App.AppID != c.appID || data.App.Scopes == nil {
		return nil, failure(ErrorInvalidResponse, OutcomeUnknown, status)
	}
	scopes := make([]string, 0)
	seen := map[string]bool{}
	for _, item := range *data.App.Scopes {
		if item.Scope == "" || strings.ContainsAny(item.Scope, " \t\r\n\x00") || len(item.TokenTypes) == 0 {
			return nil, failure(ErrorInvalidResponse, OutcomeUnknown, status)
		}
		user := false
		for _, kind := range item.TokenTypes {
			if kind != "user" && kind != "tenant" {
				return nil, failure(ErrorInvalidResponse, OutcomeUnknown, status)
			}
			user = user || kind == "user"
		}
		if user && !seen[item.Scope] {
			scopes = append(scopes, item.Scope)
			seen[item.Scope] = true
		}
	}
	sort.Strings(scopes)
	return scopes, nil
}
func requiredSuccess(payload map[string]json.RawMessage, status int) error {
	if _, ok := payload["code"]; !ok {
		return failure(ErrorInvalidResponse, OutcomeUnknown, status)
	}
	return responseError(payload, status)
}
