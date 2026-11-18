// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBindingIdentityLegacyJSONCompat(t *testing.T) {
	for _, tc := range []struct {
		name string
		json string
		want IdentityPolicy
	}{
		{"empty", `{}`, IdentityPolicy{}},
		{"legacy bot", `{"identity":"bot"}`, IdentityPolicy{Group: IdentityBot, P2P: IdentityBot}},
		{"legacy none", `{"identity":"none"}`, IdentityPolicy{}},
		{"structured group bot", `{"identity":{"group":"bot"}}`, IdentityPolicy{Group: IdentityBot}},
		{"structured p2p user", `{"identity":{"p2p":"user"}}`, IdentityPolicy{P2P: IdentityUser}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var binding Binding
			require.NoError(t, json.Unmarshal([]byte(tc.json), &binding))
			require.Equal(t, tc.want, binding.Identity)
		})
	}
	t.Run("legacy user rejected", func(t *testing.T) {
		var binding Binding
		// user 允许反序列化（不丢数据），由 validateIdentity 在 S9 前启动拒绝。
		require.NoError(t, json.Unmarshal([]byte(`{"identity":"user"}`), &binding))
		require.Equal(t, IdentityPolicy{P2P: IdentityUser}, binding.Identity)
	})
	t.Run("invalid legacy value", func(t *testing.T) {
		var binding Binding
		require.Error(t, json.Unmarshal([]byte(`{"identity":"weird"}`), &binding))
	})
}

func TestBindingIdentityRoundTripsStructured(t *testing.T) {
	original := Binding{AppID: "app", Identity: IdentityPolicy{Group: IdentityBot, P2P: IdentityBot}}
	data, err := json.Marshal(original)
	require.NoError(t, err)
	var decoded Binding
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, original.Identity, decoded.Identity)
}
