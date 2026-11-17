// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package userauth

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestDeviceRefreshesBeforeAccessTokenExpiry(t *testing.T) {
	for _, driver := range []string{"embedded", "sqlite"} {
		t.Run(driver, func(t *testing.T) {
			f := newFixture(t, driver)
			f.start()
			f.now = f.now.Add(5 * time.Second)
			status := f.status()
			f.ready(status)
			query := core.UserGrantQuery{Principal: f.p, VaultID: "vault", CredentialID: "credential", RequiredScopes: []string{"read"}}
			f.now = status.Grant.ExpiresAt.Add(-31 * time.Second)
			grant, err := f.auth.Lookup(context.Background(), query)
			require.NoError(t, err)
			require.Equal(t, core.AuthReady, grant.Status)
			require.Equal(t, status.Grant.ExpiresAt, grant.ExpiresAt)
			require.Zero(t, f.refreshes)

			f.now = status.Grant.ExpiresAt.Add(-30 * time.Second)
			grant, err = f.auth.Lookup(context.Background(), query)
			require.NoError(t, err)
			require.Equal(t, 1, f.refreshes)
			require.Equal(t, core.AuthReady, grant.Status)
			require.Equal(t, f.now.Add(time.Hour), grant.ExpiresAt, "retain the actual upstream expiry")
		})
	}
}

func TestDeviceWithoutRefreshTokenRemainsUsableUntilExpiry(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.now = f.now.Add(5 * time.Second)
	f.pollBody = `{"access_token":"uat-secret","expires_in":60,"scope":"read"}`
	status := f.status()
	f.ready(status)
	query := core.UserGrantQuery{Principal: f.p, VaultID: "vault", CredentialID: "credential", RequiredScopes: []string{"read"}}
	f.now = status.Grant.ExpiresAt.Add(-10 * time.Second)
	grant, err := f.auth.Lookup(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, core.AuthReady, grant.Status)
	require.Zero(t, f.refreshes)
	f.now = status.Grant.ExpiresAt
	grant, err = f.auth.Lookup(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, core.AuthReauthRequired, grant.Status)
}
