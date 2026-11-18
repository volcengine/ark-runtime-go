// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package memory_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	memory "github.com/volcengine/ark-runtime-go/channel/internal/testutil/memorystore"
)

func TestRemoteOperations(t *testing.T) {
	conformance.RunRemoteOperationStore(t, func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, e := memory.New(memory.Options{Clock: c.Clock})
		require.NoError(t, e)
		return s
	})
	conformance.RunRemoteOperationFences(t, func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, e := memory.New(memory.Options{Clock: c.Clock})
		require.NoError(t, e)
		return s
	})
	conformance.RunRemoteCreationBinding(t, func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, e := memory.New(memory.Options{Clock: c.Clock})
		require.NoError(t, e)
		return s
	})
}
