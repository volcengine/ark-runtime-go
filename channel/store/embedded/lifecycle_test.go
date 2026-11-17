// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

func TestOpenMissingDoesNotCreateDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "must-not-create.db")
	s, err := open(path, Options{})
	if s != nil {
		require.NoError(t, s.Close())
	}
	require.Error(t, err)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

func TestEncryptedOpenRejectsMissingNamespaceWithoutRepair(t *testing.T) {
	path := filepath.Join(t.TempDir(), "encrypted.db")
	options := Options{PayloadCodec: &payloadTestCodec{}, PayloadNamespace: "required", RequirePayloadEncryption: true}
	require.NoError(t, Install(path, options))
	db, err := bbolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error { return tx.Bucket(bucketMeta).Delete(keyPayloadNamespace) }))
	require.NoError(t, db.Close())
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	s, err := open(path, options)
	if s != nil {
		require.NoError(t, s.Close())
	}
	require.ErrorIs(t, err, payload.ErrInvalid)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestInstallCreatesUsablePlainAndEncryptedStore(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		name := "plain"
		options := Options{NoSync: true}
		if encrypted {
			name = "encrypted"
			options.PayloadCodec = &payloadTestCodec{}
			options.PayloadNamespace = "test"
			options.RequirePayloadEncryption = true
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "installed.db")
			require.NoError(t, Install(path, options))
			s, err := open(path, options)
			require.NoError(t, err)
			accepted, err := s.Ingest(context.Background(), conformance.SampleVersion(), conformance.SampleMessage("installed"))
			require.NoError(t, err)
			require.NoError(t, s.Close())
			s, err = open(path, options)
			require.NoError(t, err)
			defer func() { require.NoError(t, s.Close()) }()
			got, err := s.GetTurn(context.Background(), accepted.Turn.ID)
			require.NoError(t, err)
			require.NotNil(t, got)
			again, err := s.Ingest(context.Background(), conformance.SampleVersion(), conformance.SampleMessage("installed"))
			require.NoError(t, err)
			require.True(t, again.Duplicate)
			require.Equal(t, accepted.Turn.ID, again.Turn.ID)
		})
	}
}

func TestInstallRejectsExistingDatabaseWithoutWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.db")
	require.NoError(t, Install(path, Options{}))
	s, err := open(path, Options{})
	require.NoError(t, err)
	_, err = s.Ingest(t.Context(), conformance.SampleVersion(), conformance.SampleMessage("preserved"))
	require.NoError(t, err)
	require.NoError(t, s.Close())
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	require.ErrorIs(t, Install(path, Options{}), core.ErrConflict)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestOpenRejectsMissingRequiredBucketsWithoutRepair(t *testing.T) {
	for _, name := range append([][]byte{bucketMeta}, allBuckets...) {
		t.Run(string(name), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "incomplete.db")
			require.NoError(t, Install(path, Options{}))
			db, err := bbolt.Open(path, 0o600, nil)
			require.NoError(t, err)
			require.NoError(t, db.Update(func(tx *bbolt.Tx) error { return tx.DeleteBucket(name) }))
			require.NoError(t, db.Close())
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			for _, readOnly := range []bool{true, false} {
				s, err := open(path, Options{ReadOnly: readOnly})
				require.ErrorIs(t, err, errSchema)
				require.Nil(t, s)
			}
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestEncryptedOpenRejectsInconsistentPolicyWithoutWrites(t *testing.T) {
	for _, mode := range []string{"", "unknown"} {
		t.Run("mode="+mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "invalid-policy.db")
			options := Options{PayloadCodec: &payloadTestCodec{}, PayloadNamespace: "expected", RequirePayloadEncryption: true}
			require.NoError(t, Install(path, options))
			db, err := bbolt.Open(path, 0o600, nil)
			require.NoError(t, err)
			require.NoError(t, db.Update(func(tx *bbolt.Tx) error { return tx.Bucket(bucketMeta).Put(keyPayloadMode, []byte(mode)) }))
			require.NoError(t, db.Close())
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			s, err := open(path, options)
			require.Error(t, err)
			require.Nil(t, s)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}
