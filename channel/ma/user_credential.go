// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"context"
	"errors"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// UserAccessTokenSecretName 是个人授权唯一允许写入的环境变量名。
const UserAccessTokenSecretName = "LARKSUITE_CLI_USER_ACCESS_TOKEN"

// UserCredentialTarget 仅引用 Runtime 已 reservation 的个人 Vault 与原凭据 ID。
type UserCredentialTarget = core.UserCredentialTarget

// UserCredentialWriter 是用户授权的凭据写入端口。
type UserCredentialWriter = core.UserCredentialWriter

// SyncUserCredential 按完整 principal 验证 Vault/原凭据归属，只进行一次确定目标的写入。
// 首建响应未知不得按显示名寻找或重建；明确成功 ID 由调用方立即 durable checkpoint。
func (c *Client) SyncUserCredential(ctx context.Context, target UserCredentialTarget, value string) (string, error) {
	reject := func() (string, error) {
		return "", &requestNotSentError{cause: errors.New("ma user credential target invalid")}
	}
	if !target.Principal.Valid() || !userCredentialID(target.VaultID) || value == "" || len(target.AllowedHosts) == 0 {
		return reject()
	}
	for _, host := range target.AllowedHosts {
		if host != "open.feishu.cn" && host != "open.larksuite.com" {
			return reject()
		}
	}
	vault, err := c.GetVault(ctx, target.VaultID)
	if err != nil {
		return "", safeUserCredentialError(err, true)
	}
	if vault.Principal != target.Principal.Key() {
		return reject()
	}
	if target.CredentialID != "" {
		if !userCredentialID(target.CredentialID) {
			return reject()
		}
		var existing wireCredential
		if err = c.readCredential(ctx, target.VaultID, target.CredentialID, &existing); err != nil {
			return "", safeUserCredentialError(err, true)
		}
		if existing.ID != target.CredentialID || existing.Auth.Type != "environment_variable" || existing.Auth.SecretName != UserAccessTokenSecretName || existing.Metadata[managedMetadataKey] != managedMetadataValue || existing.Metadata[vaultPrincipalMetadataKey] != target.Principal.Key() {
			return reject()
		}
	}
	auth := map[string]any{"type": "environment_variable", "secret_value": value, "networking": map[string]any{"type": "limited", "limited": map[string]any{"allowed_hosts": target.AllowedHosts}}}
	body := map[string]any{"auth": auth}
	if target.CredentialID == "" {
		auth["secret_name"] = UserAccessTokenSecretName
		body["display_name"] = "Channel User Access Token"
		body["metadata"] = map[string]string{managedMetadataKey: managedMetadataValue, vaultPrincipalMetadataKey: target.Principal.Key()}
	}
	var result wireCredential
	if err = c.writeCredential(ctx, target.VaultID, target.CredentialID, body, &result); err != nil {
		return "", safeUserCredentialError(err, false)
	}
	if target.CredentialID != "" {
		return target.CredentialID, nil
	}
	if !userCredentialID(result.ID) {
		return "", errors.New("ma user credential response invalid")
	}
	return result.ID, nil
}

func userCredentialID(id string) bool {
	return validVaultID(id) && id != "." && id != ".." && id != "*"
}

func safeUserCredentialError(err error, unsent bool) error {
	var api *APIError
	var before core.RequestNotSent
	if errors.As(err, &before) && before.RequestNotSent() {
		unsent = true
	}
	// 不将任意上游 code、request id 或错误链带入授权日志。
	safe := errors.New("ma user credential request failed")
	if errors.As(err, &api) && isDefinitiveClientError(api.Status) {
		unsent = true
	}
	if unsent {
		return &requestNotSentError{cause: safe}
	}
	return safe
}
