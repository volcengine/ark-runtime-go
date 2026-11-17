// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetVaultUsesFrozenID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v3/vaults/vlt-frozen", r.URL.Path)
		require.Equal(t, "GET", r.Method)
		_, _ = w.Write([]byte(`{"id":"vlt-frozen","display_name":"customer-renamed","metadata":{"ark_channel_principal":"principal"},"type":"vault","created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}`))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "key", ClientOptions{})
	require.NoError(t, err)
	ref, err := client.GetVault(context.Background(), "vlt-frozen")
	require.NoError(t, err)
	require.Equal(t, "vlt-frozen", ref.ID)
	require.Equal(t, "customer-renamed", ref.Name)
	require.Equal(t, "principal", ref.Principal)
}

func TestGetVaultRejectsInvalidOrMismatchedID(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(`{"id":"vlt-other","type":"vault","display_name":"fixture","created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}`))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "key", ClientOptions{})
	require.NoError(t, err)
	for _, id := range []string{"", ".", "..", "*", "vlt/path", "vlt?query", "vlt#fragment", "vlt%2fescape"} {
		_, err = client.GetVault(context.Background(), id)
		require.Error(t, err)
	}
	require.Zero(t, calls)
	_, err = client.GetVault(context.Background(), "vlt-expected")
	require.Error(t, err)
	require.Equal(t, 1, calls)
}
