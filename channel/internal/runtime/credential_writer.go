// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import "context"

// UserCredentialTarget 引用已保留的个人 Vault 和凭据位置。
type UserCredentialTarget struct {
	Principal             UserPrincipal
	VaultID, CredentialID string
	AllowedHosts          []string
}

// UserCredentialWriter 隔离 Device 授权与具体 MA Adapter。
type UserCredentialWriter interface {
	SyncUserCredential(context.Context, UserCredentialTarget, string) (string, error)
}
