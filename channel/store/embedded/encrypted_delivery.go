// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"bytes"
	"context"
	"encoding/binary"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func (s *store) leaseEncryptedDelivery(ctx context.Context, owner string, ttl time.Duration) (*core.Delivery, error) {
	var after []byte
	for ctx.Err() == nil {
		var candidate *deliveryRecord
		var original []byte
		err := s.view(ctx, func(tx storeTx) error {
			c := tx.Bucket(bucketDeliveryReady).Cursor()
			k, id := c.First()
			if after != nil {
				k, id = c.Seek(after)
				if bytes.Equal(k, after) {
					k, id = c.Next()
				}
			}
			for ; k != nil && binary.BigEndian.Uint64(k[:8]) <= nanos(s.now()); k, id = c.Next() {
				after = bytes.Clone(k)
				ok, e := supportedDelivery(tx, string(id))
				if e != nil {
					return e
				}
				if !ok {
					continue
				}
				r, e := getDelivery(tx, string(id))
				if e != nil {
					return e
				}
				if r == nil || r.Status.Terminal() || !r.Head {
					return core.ErrConflict
				}
				candidate = r
				original = bytes.Clone(tx.Bucket(bucketDeliveries).Get(id))
				return nil
			}
			return nil
		})
		if err != nil || candidate == nil {
			return nil, err
		}
		hydrated := candidate.Delivery
		if err = s.hydrate(ctx, &hydrated); err != nil {
			return nil, err
		}
		if !supportedDeliveryContent(hydrated) {
			continue
		}
		var claimed *core.Delivery
		err = s.update(ctx, func(tx storeTx) error {
			if !bytes.Equal(tx.Bucket(bucketDeliveries).Get([]byte(candidate.ID)), original) {
				return nil
			}
			r, e := getDelivery(tx, candidate.ID)
			if e != nil {
				return e
			}
			if r == nil || r.Status.Terminal() || !r.Head || r.Eligible > nanos(s.now()) {
				return nil
			}
			old := *r
			r.Delivery = r.Claim(owner, s.now(), ttl)
			if e = s.writeDelivery(tx, &old, *r); e != nil {
				return e
			}
			next := r.Delivery
			next.Body = hydrated.Body
			claimed = &next
			return nil
		})
		if err != nil || claimed != nil {
			return claimed, err
		}
	}
	return nil, ctx.Err()
}
