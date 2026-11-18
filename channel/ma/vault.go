// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/vault"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

var _ core.VaultClient = (*Client)(nil)
var _ core.BotVaultProvisioner = (*Client)(nil)

const vaultPrincipalMetadataKey = "ark_channel_principal"
const vaultCreationMetadataKey = "ark_channel_vault_key"

// managedMetadataKey 标记由 Channel 写入的凭据，避免覆盖用户手工维护的同名凭据。
const managedMetadataKey = "managed_by"

const managedMetadataValue = "ark-agent-channel"

type wireCredential struct {
	ID   string `json:"id"`
	Auth struct {
		Type       string `json:"type"`
		SecretName string `json:"secret_name"`
	} `json:"auth"`
	Metadata map[string]string `json:"metadata"`
}

// UpsertEnvCredential 通过公开 Vault API 按 secret_name 原位更新或创建 environment_variable 凭据。
//
// 同名凭据存在但不是 Channel 管理的（metadata.managed_by 不符）时拒绝覆盖。secret_value 只写不读，
// 错误信息不包含任何凭据内容。
func (c *Client) UpsertEnvCredential(ctx context.Context, spec core.BotCredentialSpec, value string) (string, error) {
	if !validVaultID(spec.VaultID) || strings.TrimSpace(spec.SecretName) == "" || value == "" || len(spec.AllowedHosts) == 0 {
		return "", errors.New("ma credential spec invalid")
	}
	existing, err := c.findCredential(ctx, spec.VaultID, spec.SecretName)
	if err != nil {
		return "", err
	}
	auth := map[string]any{
		"type":         "environment_variable",
		"secret_value": value,
		"networking":   map[string]any{"type": "limited", "limited": map[string]any{"allowed_hosts": spec.AllowedHosts}},
	}
	var result wireCredential
	if existing != nil {
		if existing.Metadata[managedMetadataKey] != managedMetadataValue {
			return "", errors.New("ma credential exists and is not channel managed")
		}
		// secret_name 创建后不可改，更新时不重复传。
		body := map[string]any{"auth": auth}
		if err := c.writeCredential(ctx, spec.VaultID, existing.ID, body, &result); err != nil {
			return "", err
		}
		return existing.ID, nil
	}
	auth["secret_name"] = spec.SecretName
	body := map[string]any{"auth": auth, "display_name": "Channel " + spec.SecretName, "metadata": map[string]string{managedMetadataKey: managedMetadataValue}}
	if err := c.writeCredential(ctx, spec.VaultID, "", body, &result); err != nil {
		return "", err
	}
	if result.ID == "" {
		return "", errors.New("ma credential response missing id")
	}
	return result.ID, nil
}

// findCredential 分页查找同名 environment_variable 凭据；最多扫描 10 页。
func (c *Client) findCredential(ctx context.Context, vaultID, secretName string) (*wireCredential, error) {
	base := "/vaults/" + url.PathEscape(vaultID) + "/credentials"
	page := ""
	for i := 0; i < 10; i++ {
		query := url.Values{"limit": {strconv.Itoa(100)}}
		if page != "" {
			query.Set("page", page)
		}
		var wire struct {
			Data     []wireCredential `json:"data"`
			NextPage string           `json:"next_page"`
		}
		result, err := requestSDK(c, ctx, http.MethodGet, base, sdkCallOptions{}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*vault.ListCredentialsResponse, error) {
			params := &vault.CredentialsListParams{}
			options = append(options, arkruntime.WithQuery("limit", query.Get("limit")))
			if page != "" {
				options = append(options, arkruntime.WithQuery("page", page))
			}
			return sdk.ListCredentials(ctx, vaultID, params, options...)
		})
		if err != nil {
			return nil, err
		}
		wire.NextPage, _ = result.NextPage.Get()
		if err := sdkProjection(result.Credentials, &wire.Data); err != nil {
			return nil, err
		}
		for i := range wire.Data {
			if wire.Data[i].Auth.SecretName == secretName {
				return &wire.Data[i], nil
			}
		}
		if wire.NextPage == "" || wire.NextPage == page {
			return nil, nil
		}
		page = wire.NextPage
	}
	return nil, errors.New("ma credential list too long")
}

func validVaultID(id string) bool {
	return id != "" && id != "." && id != ".." && len(id) <= 128 && !strings.ContainsAny(id, "/?#%\\")
}

type wireVault struct {
	ID          string            `json:"id"`
	DisplayName string            `json:"display_name"`
	Metadata    map[string]string `json:"metadata"`
}

// CreateVault 通过公开 POST /vaults 创建 Vault 容器。公开接口没有幂等键：调用方必须由
// core.BotVaultCoordinator 以持久 reservation 保护，本方法不做重试、不删除任何 Vault。
//
// 4xx（非 408/429/425 等可重试状态）视为明确未受理，返回包裹 core.ErrVaultNotCreated 的错误；
// 超时/5xx 属于结果未知，由协调器只查找、不重建。
func (c *Client) CreateVault(ctx context.Context, spec core.BotVaultCreate) (core.VaultRef, error) {
	if strings.TrimSpace(spec.Name) == "" || strings.TrimSpace(spec.DisplayName) == "" {
		return core.VaultRef{}, errors.New("ma vault create spec invalid")
	}
	body := map[string]any{"display_name": spec.DisplayName}
	metadata := make(map[string]string, len(spec.Metadata)+1)
	for key, value := range spec.Metadata {
		metadata[key] = value
	}
	metadata[vaultCreationMetadataKey] = spec.Name
	body["metadata"] = metadata
	request, err := sdkBody[vault.CreateVaultRequest](body)
	if err != nil {
		return core.VaultRef{}, err
	}
	resource, err := requestSDK(c, ctx, http.MethodPost, "/vaults", sdkCallOptions{}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*vault.Vault, error) {
		return sdk.CreateVault(ctx, request, options...)
	})
	if err != nil {
		var apiErr *APIError
		var unsent core.RequestNotSent
		if errors.As(err, &unsent) && unsent.RequestNotSent() || errors.As(err, &apiErr) && isDefinitiveClientError(apiErr.Status) {
			return core.VaultRef{}, errors.Join(core.ErrVaultNotCreated, err)
		}
		return core.VaultRef{}, err
	}
	var result wireVault
	if err := sdkProjection(resource, &result); err != nil {
		return core.VaultRef{}, err
	}
	if result.ID == "" {
		return core.VaultRef{}, errors.New("ma vault response missing id")
	}
	return core.VaultRef{ID: result.ID, Name: vaultName(result), Principal: result.Metadata[vaultPrincipalMetadataKey]}, nil
}

// FindVaultByName 按创建标识 metadata 精确查询；不依赖可修改的展示名。
func (c *Client) FindVaultByName(ctx context.Context, name string) (core.VaultRef, error) {
	if strings.TrimSpace(name) == "" {
		return core.VaultRef{}, errors.New("ma vault name required")
	}
	return c.findVault(ctx, map[string]string{vaultCreationMetadataKey: name}, "", name)
}

// FindVaultByPrincipal 为未知结果查账下推主体过滤，兼容只有主体标识的历史 Vault。
func (c *Client) FindVaultByPrincipal(ctx context.Context, principal, name string) (core.VaultRef, error) {
	if strings.TrimSpace(principal) == "" || strings.TrimSpace(name) == "" {
		return core.VaultRef{}, core.ErrInvalid
	}
	return c.findVault(ctx, map[string]string{vaultPrincipalMetadataKey: principal}, principal, name)
}

func (c *Client) findVault(ctx context.Context, metadata map[string]string, principal, name string) (core.VaultRef, error) {
	filter, err := json.Marshal(metadata)
	if err != nil {
		return core.VaultRef{}, err
	}
	query := url.Values{"limit": {"2"}, "metadata": {string(filter)}}
	var wire struct {
		Data     []wireVault `json:"data"`
		NextPage string      `json:"next_page"`
	}
	result, err := requestSDK(c, ctx, http.MethodGet, "/vaults", sdkCallOptions{}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*vault.ListVaultsResponse, error) {
		options = append(options, arkruntime.WithQuery("limit", query.Get("limit")), arkruntime.WithQuery("metadata", query.Get("metadata")))
		return sdk.ListVaults(ctx, nil, options...)
	})
	if err != nil {
		return core.VaultRef{}, err
	}
	if err := sdkProjection(result, &wire); err != nil {
		return core.VaultRef{}, err
	}
	if len(wire.Data) > 1 || wire.NextPage != "" {
		return core.VaultRef{}, core.ErrVaultNameConflict
	}
	if len(wire.Data) == 0 {
		return core.VaultRef{}, nil
	}
	ref := wire.Data[0]
	for key, value := range metadata {
		if ref.Metadata[key] != value {
			return core.VaultRef{}, core.ErrVaultPrincipalMismatch
		}
	}
	creationKey := ref.Metadata[vaultCreationMetadataKey]
	// 历史 Vault 没有创建标识时，还要证明确定名称对应同一主体；不能仅凭主体认领其它代。
	legacy := principal != "" && creationKey == "" && (ref.DisplayName == name ||
		strings.HasPrefix(principal, "user:") && name == "channel-user-"+strings.TrimPrefix(principal, "user:") && ref.DisplayName == "Channel User")
	if !validVaultID(ref.ID) || creationKey != name && !legacy {
		return core.VaultRef{}, core.ErrVaultPrincipalMismatch
	}
	return core.VaultRef{ID: ref.ID, Name: name, Principal: ref.Metadata[vaultPrincipalMetadataKey]}, nil
}

// vaultName 返回展示名；创建结果查账使用独立的 metadata 标识。
func vaultName(v wireVault) string { return v.DisplayName }

// isDefinitiveClientError 判断 4xx 是否为明确拒绝（可安全解除 reservation）：
// 408/409/425/429 等属于可重试/冲突，不算明确未受理。
func isDefinitiveClientError(status int) bool {
	if status < 400 || status >= 500 {
		return false
	}
	switch status {
	case http.StatusRequestTimeout, http.StatusConflict, http.StatusTooManyRequests, http.StatusTooEarly:
		return false
	default:
		return true
	}
}

// GetVault 按冻结 ID 读取 Vault，名称可以由用户修改，归属以 metadata 中的 principal 为准。
func (c *Client) GetVault(ctx context.Context, id string) (core.VaultRef, error) {
	if !validVaultID(id) || id == "." || id == ".." || id == "*" {
		return core.VaultRef{}, errors.New("ma vault id invalid")
	}
	resource, err := requestSDK(c, ctx, http.MethodGet, "/vaults/"+url.PathEscape(id), sdkCallOptions{}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*vault.Vault, error) {
		return sdk.GetVault(ctx, id, options...)
	})
	if err != nil {
		return core.VaultRef{}, err
	}
	var result wireVault
	if err := sdkProjection(resource, &result); err != nil {
		return core.VaultRef{}, err
	}
	if result.ID != id {
		return core.VaultRef{}, errors.New("ma vault response id mismatch")
	}
	return core.VaultRef{ID: result.ID, Name: result.DisplayName, Principal: result.Metadata[vaultPrincipalMetadataKey]}, nil
}

func (c *Client) writeCredential(ctx context.Context, vaultID, credentialID string, body any, result *wireCredential) error {
	path := "/vaults/" + url.PathEscape(vaultID) + "/credentials"
	if credentialID != "" {
		path += "/" + url.PathEscape(credentialID)
	}
	resource, err := requestSDK(c, ctx, http.MethodPost, path, sdkCallOptions{}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*vault.Credential, error) {
		if credentialID != "" {
			request, err := sdkBody[vault.UpdateCredentialRequest](body)
			if err != nil {
				return nil, err
			}
			return sdk.UpdateCredential(ctx, vaultID, credentialID, request, options...)
		}
		request, err := sdkBody[vault.CreateCredentialRequest](body)
		if err != nil {
			return nil, err
		}
		return sdk.CreateCredential(ctx, vaultID, request, options...)
	})
	if err != nil {
		return err
	}
	return sdkProjection(resource, result)
}

func (c *Client) readCredential(ctx context.Context, vaultID, credentialID string, result *wireCredential) error {
	path := "/vaults/" + url.PathEscape(vaultID) + "/credentials/" + url.PathEscape(credentialID)
	resource, err := requestSDK(c, ctx, http.MethodGet, path, sdkCallOptions{}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*vault.Credential, error) {
		return sdk.GetCredential(ctx, vaultID, credentialID, options...)
	})
	if err != nil {
		return err
	}
	return sdkProjection(resource, result)
}
