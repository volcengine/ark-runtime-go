// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
)

func TestRemoteOperations(t *testing.T) {
	conformance.RunRemoteOperationStore(t, func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, e := openInstalledFixture(filepath.Join(t.TempDir(), "remote.db"), Options{Clock: c.Clock, NoSync: true})
		require.NoError(t, e)
		return s
	})
	conformance.RunRemoteOperationFences(t, func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, e := openInstalledFixture(filepath.Join(t.TempDir(), "remote.db"), Options{Clock: c.Clock, NoSync: true})
		require.NoError(t, e)
		return s
	})
	conformance.RunRemoteCreationBinding(t, func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, e := openInstalledFixture(filepath.Join(t.TempDir(), "remote.db"), Options{Clock: c.Clock, NoSync: true})
		require.NoError(t, e)
		return s
	})
}
