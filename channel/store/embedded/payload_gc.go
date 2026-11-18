// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import "github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"

// Rebuild links from live raw records in the same write transaction as Prune.
// The snapshot bucket is a root even after its originating turn is gone.
func (s *store) prunePayloads(tx storeTx) error {
	refs := tx.Bucket(bucketPayloadRefs)
	c := refs.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		if err := c.Delete(); err != nil {
			return err
		}
	}
	for _, root := range []struct {
		kind   string
		bucket []byte
	}{
		{"message", bucketMessages}, {"turn", bucketTurns},
		{"snapshot", bucketSnapshots}, {"delivery", bucketDeliveries},
		{"interaction", bucketInteractions},
	} {
		if err := tx.Bucket(root.bucket).ForEach(func(id, raw []byte) error {
			links, err := payload.References(s.payloadNamespace, root.kind, string(id), raw)
			if err != nil {
				return err
			}
			for field, ref := range links {
				if err := refs.Put(key([]byte(root.kind), id, []byte(field)), []byte(ref)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	live := map[string]bool{}
	if err := refs.ForEach(func(_, ref []byte) error {
		live[string(ref)] = true
		return nil
	}); err != nil {
		return err
	}
	c = tx.Bucket(bucketPayloads).Cursor()
	for id, _ := c.First(); id != nil; id, _ = c.Next() {
		if !live[string(id)] {
			if err := c.Delete(); err != nil {
				return err
			}
		}
	}
	return nil
}
