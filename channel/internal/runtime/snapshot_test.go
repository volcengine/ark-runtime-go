// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSessionSnapshotRequestIntegrity(t *testing.T) {
	spec := CreateSessionSpec{Binding: Binding{ID: "binding"}, CreationKey: "creation", LookupKey: "lookup", EnvironmentType: "cloud", EnvironmentOverride: &EnvironmentOverride{Type: "cloud", Env: map[string]string{"CUSTOM": "original"}}}
	snapshot := NewSessionSnapshot("route", 1, spec)
	raw, err := json.Marshal(snapshot)
	require.NoError(t, err)
	for _, change := range []struct{ name, before, after string }{
		{"environment", "original", "modified"},
		{"hash", `"request_hash":"`, `"request_hash":"bad`},
		{"schema", `"schema_version":1`, `"schema_version":2`},
	} {
		t.Run(change.name, func(t *testing.T) {
			var got SessionSnapshot
			require.NoError(t, json.Unmarshal([]byte(strings.Replace(string(raw), change.before, change.after, 1)), &got))
			require.False(t, got.Valid())
		})
	}
}

func TestSessionSnapshotEnvironmentTypeRoundTrip(t *testing.T) {
	for _, environmentType := range []string{"", "cloud", "self_hosted"} {
		t.Run(environmentType, func(t *testing.T) {
			spec := CreateSessionSpec{Binding: Binding{ID: "binding"}, CreationKey: "creation", LookupKey: "lookup", EnvironmentType: environmentType}
			original := NewSessionSnapshot("route", 1, spec)
			raw, err := json.Marshal(original)
			require.NoError(t, err)
			require.Contains(t, string(raw), `"EnvironmentType"`)
			var restored SessionSnapshot
			require.NoError(t, json.Unmarshal(raw, &restored))
			require.True(t, restored.Valid())
			require.Equal(t, original, restored)
		})
	}
}
