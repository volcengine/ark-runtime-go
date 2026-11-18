// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package oauth

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

// GetUserIdentity 从 user_info 取得完整用户与租户身份，AppID 仅使用冻结配置。
func (c *Client) GetUserIdentity(ctx context.Context, accessToken string) (Identity, error) {
	if strings.TrimSpace(accessToken) == "" {
		return Identity{}, failure(ErrorConfiguration, OutcomeRejected, 0)
	}
	payload, status, err := c.request(ctx, http.MethodGet, c.openBaseURL+"/open-apis/authen/v1/user_info", nil, accessToken, false)
	if err != nil {
		return Identity{}, err
	}
	if err = responseError(payload, status); err != nil {
		return Identity{}, err
	}
	var data map[string]json.RawMessage
	if json.Unmarshal(payload["data"], &data) != nil || data == nil {
		return Identity{}, failure(ErrorInvalidResponse, OutcomeUnknown, status)
	}
	identity := Identity{AppID: c.appID, OpenID: stringField(data, "open_id"), TenantKey: stringField(data, "tenant_key")}
	if strings.TrimSpace(identity.OpenID) == "" || strings.TrimSpace(identity.TenantKey) == "" {
		return Identity{}, failure(ErrorInvalidResponse, OutcomeUnknown, status)
	}
	return identity, nil
}
