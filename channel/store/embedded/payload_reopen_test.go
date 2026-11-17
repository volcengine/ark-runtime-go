// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

func embeddedDomainRows(t *testing.T, db *bbolt.DB) map[string]string {
	t.Helper()
	result := make(map[string]string)
	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, bucket *bbolt.Bucket) error {
			return bucket.ForEach(func(key, value []byte) error {
				result[string(name)+"/"+string(key)] = string(value)
				return nil
			})
		})
	}))
	return result
}

func TestPopulatedEmbeddedOpenPreservesRecordsAndPayloads(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		name := "plain"
		options := Options{}
		c := &payloadTestCodec{}
		if encrypted {
			name = "encrypted"
			options.PayloadCodec = c
			options.PayloadNamespace = "reopen"
			options.RequirePayloadEncryption = true
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "populated.db")
			s, err := openInstalledFixture(path, options)
			require.NoError(t, err)
			_, err = s.Ingest(t.Context(), conformance.SampleVersion(), conformance.SampleMessage("preserved"))
			require.NoError(t, err)
			before := embeddedDomainRows(t, s.db)
			seals, opens := c.counts()
			require.NoError(t, s.Close())
			fileBefore, err := os.ReadFile(path)
			require.NoError(t, err)
			if encrypted {
				_, err = open(path, Options{})
				require.ErrorIs(t, err, payload.ErrPolicyMismatch)
			}
			for _, readOnly := range []bool{true, false} {
				opts := options
				opts.ReadOnly = readOnly
				s, err = open(path, opts)
				require.NoError(t, err)
				require.Equal(t, before, embeddedDomainRows(t, s.db), "opening must preserve every persisted record")
				require.NoError(t, s.Close())
			}
			nextSeals, nextOpens := c.counts()
			require.Equal(t, seals, nextSeals, "opening must not re-encrypt payloads")
			require.Equal(t, opens, nextOpens, "opening must not decrypt payloads")
			fileAfter, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, fileBefore, fileAfter)
		})
	}
}
