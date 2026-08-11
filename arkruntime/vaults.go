// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/vault"
)

const vaultsPrefix = "/vaults"

// ---- Vault CRUD -----------------------------------------------------------

// CreateVault creates a new Vault.
func (c *Client) CreateVault(
	ctx context.Context,
	body *vault.CreateVaultRequest,
	setters ...requestOption,
) (*vault.Vault, error) {
	if body == nil {
		return nil, errors.New("missing required request body")
	}
	opts := append(setters, withBody(body))
	wrap := &vault.VaultResponse{}
	if err := c.Do(ctx, http.MethodPost, c.fullURL(vaultsPrefix), "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Vault, nil
}

// GetVault retrieves a Vault by ID.
func (c *Client) GetVault(ctx context.Context, vaultID string, setters ...requestOption) (*vault.Vault, error) {
	if vaultID == "" {
		return nil, errors.New("missing required vault_id")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", vaultsPrefix, vault.PathEscape(vaultID)))
	opts := append(setters, withBody(nil))
	wrap := &vault.VaultResponse{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Vault, nil
}

// ListVaults lists Vaults.
func (c *Client) ListVaults(
	ctx context.Context,
	params *vault.VaultsListParams,
	setters ...requestOption,
) (*vault.ListVaultsResponse, error) {
	q, qerr := vault.URLQueryVaultsList(params)
	if qerr != nil {
		return nil, qerr
	}
	u := c.fullURL(vaultsPrefix)
	if encoded := q.Encode(); encoded != "" {
		u = u + "?" + encoded
	}
	opts := append(setters, withBody(nil))
	wrap := &vault.ListVaultsResponseWrapper{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.ListVaultsResponse, nil
}

// UpdateVault updates a Vault's display_name / metadata.
func (c *Client) UpdateVault(
	ctx context.Context,
	vaultID string,
	body *vault.UpdateVaultRequest,
	setters ...requestOption,
) (*vault.Vault, error) {
	if vaultID == "" {
		return nil, errors.New("missing required vault_id")
	}
	if body == nil {
		return nil, errors.New("missing required request body")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", vaultsPrefix, vault.PathEscape(vaultID)))
	opts := append(setters, withBody(body))
	wrap := &vault.VaultResponse{}
	if err := c.Do(ctx, http.MethodPost, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Vault, nil
}

// DeleteVault deletes a Vault.
func (c *Client) DeleteVault(
	ctx context.Context,
	vaultID string,
	setters ...requestOption,
) (*vault.DeleteVaultResponse, error) {
	if vaultID == "" {
		return nil, errors.New("missing required vault_id")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", vaultsPrefix, vault.PathEscape(vaultID)))
	opts := append(setters, withBody(nil))
	wrap := &vault.DeleteVaultResponseWrapper{}
	if err := c.Do(ctx, http.MethodDelete, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.DeleteVaultResponse, nil
}

// ---- Credential CRUD (nested under Vault) ---------------------------------

func credentialPrefix(vaultID string) string {
	return fmt.Sprintf("%s/%s/credentials", vaultsPrefix, vault.PathEscape(vaultID))
}

// CreateCredential creates a Credential under the given Vault.
func (c *Client) CreateCredential(
	ctx context.Context,
	vaultID string,
	body *vault.CreateCredentialRequest,
	setters ...requestOption,
) (*vault.Credential, error) {
	if vaultID == "" {
		return nil, errors.New("missing required vault_id")
	}
	if body == nil {
		return nil, errors.New("missing required request body")
	}
	opts := append(setters, withBody(body))
	wrap := &vault.CredentialResponse{}
	if err := c.Do(ctx, http.MethodPost, c.fullURL(credentialPrefix(vaultID)), "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Credential, nil
}

// GetCredential retrieves a Credential by ID.
func (c *Client) GetCredential(
	ctx context.Context,
	vaultID, credentialID string,
	setters ...requestOption,
) (*vault.Credential, error) {
	if vaultID == "" || credentialID == "" {
		return nil, errors.New("missing required vault_id / credential_id")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", credentialPrefix(vaultID), vault.PathEscape(credentialID)))
	opts := append(setters, withBody(nil))
	wrap := &vault.CredentialResponse{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Credential, nil
}

// ListCredentials lists Credentials under a Vault.
func (c *Client) ListCredentials(
	ctx context.Context,
	vaultID string,
	params *vault.CredentialsListParams,
	setters ...requestOption,
) (*vault.ListCredentialsResponse, error) {
	if vaultID == "" {
		return nil, errors.New("missing required vault_id")
	}
	q, qerr := vault.URLQueryCredentialsList(params)
	if qerr != nil {
		return nil, qerr
	}
	u := c.fullURL(credentialPrefix(vaultID))
	if encoded := q.Encode(); encoded != "" {
		u = u + "?" + encoded
	}
	opts := append(setters, withBody(nil))
	wrap := &vault.ListCredentialsResponseWrapper{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.ListCredentialsResponse, nil
}

// UpdateCredential updates a Credential's mutable fields.
func (c *Client) UpdateCredential(
	ctx context.Context,
	vaultID, credentialID string,
	body *vault.UpdateCredentialRequest,
	setters ...requestOption,
) (*vault.Credential, error) {
	if vaultID == "" || credentialID == "" {
		return nil, errors.New("missing required vault_id / credential_id")
	}
	if body == nil {
		return nil, errors.New("missing required request body")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", credentialPrefix(vaultID), vault.PathEscape(credentialID)))
	opts := append(setters, withBody(body))
	wrap := &vault.CredentialResponse{}
	if err := c.Do(ctx, http.MethodPost, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Credential, nil
}

// DeleteCredential deletes a Credential.
func (c *Client) DeleteCredential(
	ctx context.Context,
	vaultID, credentialID string,
	setters ...requestOption,
) (*vault.DeleteCredentialResponse, error) {
	if vaultID == "" || credentialID == "" {
		return nil, errors.New("missing required vault_id / credential_id")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", credentialPrefix(vaultID), vault.PathEscape(credentialID)))
	opts := append(setters, withBody(nil))
	wrap := &vault.DeleteCredentialResponseWrapper{}
	if err := c.Do(ctx, http.MethodDelete, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.DeleteCredentialResponse, nil
}

// ValidateCredential probes the credential's MCP server (mcp_oauth type).
func (c *Client) ValidateCredential(
	ctx context.Context,
	vaultID, credentialID string,
	setters ...requestOption,
) (*vault.CredentialValidation, error) {
	if vaultID == "" || credentialID == "" {
		return nil, errors.New("missing required vault_id / credential_id")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s/mcp_oauth_validate", credentialPrefix(vaultID), vault.PathEscape(credentialID)))
	opts := append(setters, withBody(nil))
	wrap := &vault.CredentialValidationResponse{}
	if err := c.Do(ctx, http.MethodPost, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.CredentialValidation, nil
}
