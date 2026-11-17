// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoreCaseCatalog(t *testing.T) {
	names := map[string]bool{}
	for _, c := range storeCases() {
		require.False(t, names[c.name], "duplicate store contract case %s", c.name)
		require.NotNil(t, c.run, c.name)
		names[c.name] = true
	}
	_, err := selectStoreCases(payloadCaseNames)
	require.NoError(t, err)
	_, err = selectStoreCases([]string{"NoSuchCase"})
	require.Error(t, err)
}
