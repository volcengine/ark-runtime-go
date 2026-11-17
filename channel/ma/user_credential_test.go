// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestUserCredentialUsesVerifiedPrincipalAndExactID(t *testing.T) {
	p := core.UserPrincipal{Owner: "owner", Project: "project", ChannelID: "channel", Epoch: 1, AppID: "app", TenantKey: "tenant", OpenID: "user"}
	for _, tc := range []struct{ name, id, mismatch string }{{"create", "", ""}, {"update", "credential", ""}, {"wrong_principal", "credential", "principal"}, {"wrong_type", "credential", "type"}, {"wrong_secret", "credential", "secret"}} {
		t.Run(tc.name, func(t *testing.T) {
			writes := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.NotEqual(t, "/api/v3/vaults/vault/credentials", r.URL.Path+"?"+r.URL.RawQuery)
				if r.Method == "GET" {
					if r.URL.Path == "/api/v3/vaults/vault" {
						_ = json.NewEncoder(w).Encode(map[string]any{"id": "vault", "type": "vault", "display_name": "fixture", "created_at": "2026-10-13T00:00:00Z", "updated_at": "2026-10-13T00:00:00Z", "metadata": map[string]string{vaultPrincipalMetadataKey: p.Key()}})
						return
					}
					require.Equal(t, "/api/v3/vaults/vault/credentials/credential", r.URL.Path)
					principal, kind, secret := p.Key(), "environment_variable", UserAccessTokenSecretName
					switch tc.mismatch {
					case "principal":
						principal = "other"
					case "type":
						kind = "oauth"
					case "secret":
						secret = "OTHER"
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"id": "credential", "type": "vault_credential", "vault_id": "vault", "created_at": "2026-10-13T00:00:00Z", "updated_at": "2026-10-13T00:00:00Z", "auth": map[string]string{"type": kind, "secret_name": secret}, "metadata": map[string]string{managedMetadataKey: managedMetadataValue, vaultPrincipalMetadataKey: principal}})
					return
				}
				writes++
				var body map[string]any
				require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				auth := body["auth"].(map[string]any)
				require.Equal(t, "synthetic-secret", auth["secret_value"])
				require.Equal(t, "limited", auth["networking"].(map[string]any)["type"])
				require.Equal(t, map[string]any{"type": "limited", "limited": map[string]any{"allowed_hosts": []any{"open.feishu.cn"}}}, auth["networking"])
				if tc.id == "" {
					require.Equal(t, UserAccessTokenSecretName, auth["secret_name"])
					require.Equal(t, p.Key(), body["metadata"].(map[string]any)[vaultPrincipalMetadataKey])
				} else {
					require.NotContains(t, auth, "secret_name")
				}
				_, _ = fmt.Fprint(w, `{"id":"credential","type":"vault_credential","vault_id":"vault","auth":{"type":"environment_variable"},"created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}`)
			}))
			defer server.Close()
			client, err := NewClient(server.URL, "key", ClientOptions{})
			require.NoError(t, err)
			id, err := client.SyncUserCredential(context.Background(), UserCredentialTarget{Principal: p, VaultID: "vault", CredentialID: tc.id, AllowedHosts: []string{"open.feishu.cn"}}, "synthetic-secret")
			if tc.mismatch != "" {
				require.Error(t, err)
				require.Zero(t, writes)
				var unsent core.RequestNotSent
				require.ErrorAs(t, err, &unsent)
				require.True(t, unsent.RequestNotSent())
				return
			}
			require.NoError(t, err)
			require.Equal(t, "credential", id)
			require.Equal(t, 1, writes)
		})
	}
}
