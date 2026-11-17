// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
)

func TestUserAuthMissingOptionalBucketDoesNotBlockBotOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optional.db")
	s, e := openInstalledFixture(path, Options{})
	require.NoError(t, e)
	require.NoError(t, s.db.Update(func(tx *bbolt.Tx) error { return tx.DeleteBucket(bucketUserAuth) }))
	require.NoError(t, s.Close())
	s, e = open(path, Options{})
	require.NoError(t, e)
	defer func() { require.NoError(t, s.Close()) }()
	_, e = s.Ingest(t.Context(), conformance.SampleVersion(), conformance.SampleMessage("bot"))
	require.NoError(t, e)
	require.NoError(t, s.ExpandUserAuth(t.Context()))
}
