// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"go.etcd.io/bbolt"

	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

var keyPayloadNamespace = []byte("payload_namespace")

func (s *store) checkPayloadOpen(tx *bbolt.Tx) error {
	meta := tx.Bucket(bucketMeta)
	mode, namespace := string(meta.Get(keyPayloadMode)), string(meta.Get(keyPayloadNamespace))
	if mode != "" && mode != "encrypted-v1" {
		return payload.ErrInvalid
	}
	if s.payloadCodec == nil {
		if mode != "" {
			return payload.ErrPolicyMismatch
		}
		if namespace != "" {
			return payload.ErrInvalid
		}
		return nil
	}
	if mode != "encrypted-v1" {
		return payload.ErrPolicyMismatch
	}
	if namespace != s.payloadNamespace {
		return payload.ErrInvalid
	}
	return nil
}
