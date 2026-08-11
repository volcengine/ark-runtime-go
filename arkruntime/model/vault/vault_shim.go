// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package vault

import (
	"net/url"

	"github.com/volcengine/ark-runtime-go/arkruntime/model"
	"github.com/volcengine/ark-runtime-go/arkruntime/pkg/apiquery"
)

// --- Vault ------------------------------------------------------------------

type VaultResponse struct {
	Vault
	model.HttpHeader
}

type ListVaultsResponseWrapper struct {
	ListVaultsResponse
	model.HttpHeader
}

type DeleteVaultResponseWrapper struct {
	DeleteVaultResponse
	model.HttpHeader
}

// --- Credential -------------------------------------------------------------

type CredentialResponse struct {
	Credential
	model.HttpHeader
}

type ListCredentialsResponseWrapper struct {
	ListCredentialsResponse
	model.HttpHeader
}

type DeleteCredentialResponseWrapper struct {
	DeleteCredentialResponse
	model.HttpHeader
}

type CredentialValidationResponse struct {
	CredentialValidation
	model.HttpHeader
}

// --- URL helpers ------------------------------------------------------------

func URLQueryVaultsList(req *VaultsListParams) (url.Values, error) {
	if req == nil {
		return url.Values{}, nil
	}
	return apiquery.Marshal(req)
}

func URLQueryCredentialsList(req *CredentialsListParams) (url.Values, error) {
	if req == nil {
		return url.Values{}, nil
	}
	return apiquery.Marshal(req)
}

func PathEscape(s string) string { return url.PathEscape(s) }
