// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"bytes"
	"context"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

var bucketRemoteOperations = []byte("remote_operations")

func remoteTurnPinned(tx storeTx, id string) (bool, error) {
	pinned := false
	e := tx.Bucket(bucketRemoteOperations).ForEach(func(k, v []byte) error {
		o, e := getJSON[core.RemoteOperation](tx, bucketRemoteOperations, k)
		supported, kindErr := legacyRecord(tx, bucketRemoteOperations, k)
		if kindErr != nil {
			return kindErr
		}
		if e == nil && o.TurnID == id && (!o.Resolved || !supported) {
			pinned = true
		}
		return e
	})
	return pinned, e
}
func (s *store) remoteQuota(tx storeTx, t core.Turn) (string, *accountQuota, error) {
	owner, q, e := s.quota(tx, t)
	if e != nil {
		return "", nil, e
	}
	if owner == "" {
		owner = "binding:" + t.BindingID
		q, e = getJSON[accountQuota](tx, bucketQuotas, []byte(owner))
		if e != nil {
			return "", nil, e
		}
	}
	if q == nil {
		q = &accountQuota{Held: map[string]bool{}}
	}
	if q.Remote == nil {
		q.Remote = map[string]core.RemoteKind{}
	}
	return owner, q, nil
}
func (s *store) updateRemoteTurn(ctx context.Context, l core.Lease, u core.TurnUpdate) (core.Turn, error) {
	var inputBefore *core.Turn
	if s.payloadCodec != nil && (u.Rebind || u.Input.ContextFrozen || u.Input.BlocksFrozen || u.Input.MountsFrozen) {
		var err error
		inputBefore, err = s.GetTurn(ctx, l.ID)
		if err != nil {
			return core.Turn{}, err
		}
		if inputBefore == nil {
			return core.Turn{}, core.ErrLeaseLost
		}
	}
	var result core.Turn
	e := s.update(ctx, func(tx storeTx) error {
		record, e := s.leasedTurn(tx, l)
		if e != nil {
			return e
		}
		if inputBefore != nil {
			left, _ := core.EncodeStored(record.Payloads)
			right, _ := core.EncodeStored(inputBefore.Payloads)
			if !bytes.Equal(left, right) {
				return core.ErrConflict
			}
			record.Input = inputBefore.Input
		}

		if u.RemoteIntent != nil && record.Session.SnapshotID != "" {
			snapshot, e := getJSON[core.SessionSnapshot](tx, bucketSnapshots, []byte(record.Session.SnapshotID))
			if e != nil {
				return e
			}
			record.Turn, e = record.WithSessionIdentity(snapshot)
			if e != nil {
				return e
			}
		}
		next, e := record.Apply(u)
		if e != nil {
			return e
		}
		if e = s.applyRemoteTurn(tx, next, u); e != nil {
			return e
		}
		old := *record
		record.Turn = next
		result = next
		if e = s.writeTurn(tx, &old, *record); e != nil {
			return e
		}
		if s.payloadCodec != nil {
			stored, err := getTurn(tx, result.ID)
			if err != nil {
				return err
			}
			if stored == nil {
				return payload.ErrInvalid
			}
			result = stored.Turn
		}
		return nil
	})
	if e == nil {
		e = s.hydrate(ctx, &result)
	}
	return result, e
}
func (s *store) applyRemoteTurn(tx storeTx, t core.Turn, u core.TurnUpdate) error {
	kind := u.RemoteKind
	if u.RemoteIntent != nil {
		kind = u.RemoteIntent.Kind
	}
	if kind == "" {
		return nil
	}
	owner, q, e := s.remoteQuota(tx, t)
	if e != nil {
		return e
	}
	if e = checkUserIntent(tx, owner, u.RemoteIntent); e != nil {
		return e
	}
	id := core.RemoteOperationID(t, kind)
	o, e := getJSON[core.RemoteOperation](tx, bucketRemoteOperations, []byte(id))
	if e != nil {
		return e
	}
	if u.RemoteIntent != nil {
		if o != nil && !o.Resolved && (o.UserAuthKey != u.RemoteIntent.UserAuthKey || o.GrantVersion != u.RemoteIntent.GrantVersion) {
			return core.ErrConflict
		}
		if kind != core.RemoteInput && kind != core.RemoteCreate || u.RemoteIntent.Limit < 0 || t.Submit.WriteAttempts == 0 || kind == core.RemoteInput && t.Session.SessionID == "" {
			return core.ErrInvalid
		}
		if o == nil || o.Resolved {
			if kind == core.RemoteInput {
				e = tx.Bucket(bucketRemoteOperations).ForEach(func(k, v []byte) error {
					other, e := getJSON[core.RemoteOperation](tx, bucketRemoteOperations, k)
					if e != nil {
						return e
					}
					if other.ID != id && other.Owner == owner && other.SessionID == t.Session.SessionID && !other.Resolved {
						return core.ErrRemoteBarrier
					}
					return nil
				})
				if e != nil {
					return e
				}
			}
			count := 0
			for _, k := range q.Remote {
				if k == kind {
					count++
				}
			}
			if u.RemoteIntent.Limit > 0 && count >= u.RemoteIntent.Limit {
				return core.ErrRemoteLimit
			}
			next := core.NewRemoteOperation(t, owner, kind)
			if u.RemoteIntent.UserAuthKey != "" {
				next.WorkKind = core.WorkKindUserAuth
			}
			next.UserAuthKey = u.RemoteIntent.UserAuthKey
			next.GrantVersion = u.RemoteIntent.GrantVersion
			o = &next
			q.Remote[id] = kind
		}
	}
	if o == nil {
		return nil
	}
	if o.Lease.Owner != "" {
		return core.ErrLeaseLost
	}
	if u.RemoteProof != nil && u.RemoteProof.Kind == core.RemoteInterrupted && !o.CleanupStarted {
		return core.ErrRemoteBarrier
	}
	if u.RemoteCleanup {
		if o.Resolved || o.CleanupStarted {
			return core.ErrInterruptStarted
		}
		o.State = "closing"
		o.CleanupStarted = true
		o.AttentionRequired = true
		o.Revision++
	}
	if u.RemoteProof != nil {
		next, e := o.Resolve(*u.RemoteProof, s.now())
		if e != nil {
			return e
		}
		o = &next
		delete(q.Remote, id)
	}
	if t.Session.SessionID != "" {
		o.SessionID = t.Session.SessionID
	}
	if e = putJSON(tx, bucketRemoteOperations, []byte(id), o); e != nil {
		return e
	}
	return putJSON(tx, bucketQuotas, []byte(owner), q)
}

// ListRemoteOperations 按稳定 ID 分页读取远端操作。
func (s *store) ListRemoteOperations(ctx context.Context, q core.RemoteOperationQuery) ([]core.RemoteOperation, error) {
	limit, e := core.PageLimit(q.Limit)
	if e != nil {
		return nil, e
	}
	var out []core.RemoteOperation
	e = s.view(ctx, func(tx storeTx) error {
		c := tx.Bucket(bucketRemoteOperations).Cursor()
		for k, _ := c.Seek([]byte(q.After)); k != nil && len(out) < limit; k, _ = c.Next() {
			if string(k) <= q.After {
				continue
			}
			o, e := getJSON[core.RemoteOperation](tx, bucketRemoteOperations, k)
			if e != nil {
				return e
			}
			if q.OpenOnly && o.Resolved || q.TurnID != "" && o.TurnID != q.TurnID {
				continue
			}
			out = append(out, *o)
		}
		return nil
	})
	return out, e
}

// LeaseRemoteOperation 仅领取本地终态操作，并持久转移 fence。
func (s *store) LeaseRemoteOperation(ctx context.Context, c core.RemoteClaim) (*core.RemoteOperation, error) {
	if c.Owner == "" || c.TTL <= 0 {
		return nil, core.ErrInvalid
	}
	var out *core.RemoteOperation
	e := s.update(ctx, func(tx storeTx) error {
		cur := tx.Bucket(bucketRemoteOperations).Cursor()
		for k, _ := cur.First(); k != nil; k, _ = cur.Next() {
			supported, e := legacyRecord(tx, bucketRemoteOperations, k)
			if e != nil {
				return e
			}
			if !supported && c.UserAuth {
				kind, e := core.StoredWorkKind(tx.Bucket(bucketRemoteOperations).Get(k))
				if e != nil {
					return e
				}
				supported = kind == core.WorkKindUserAuth
			}
			if !supported {
				continue
			}
			o, e := getJSON[core.RemoteOperation](tx, bucketRemoteOperations, k)
			if e != nil {
				return e
			}
			if o.Resolved || o.Lease.Live(s.now()) || s.now().Before(o.NextReconcileAt) {
				continue
			}
			t, e := getTurn(tx, o.TurnID)
			if e != nil {
				return e
			}
			if t == nil || !t.Status.Terminal() {
				continue
			}
			o.Lease = core.Lease{ID: o.ID, Owner: c.Owner, Fence: o.Lease.Fence + 1, ExpiresAt: s.now().Add(c.TTL)}
			o.Revision++
			out = o
			return putJSON(tx, bucketRemoteOperations, k, o)
		}
		return nil
	})
	return out, e
}

// UpdateRemoteOperation 按独立 fence 保存证明与清理意图。
func (s *store) UpdateRemoteOperation(ctx context.Context, l core.Lease, u core.RemoteUpdate) (core.RemoteOperation, error) {
	var out core.RemoteOperation
	e := s.update(ctx, func(tx storeTx) error {
		o, e := getJSON[core.RemoteOperation](tx, bucketRemoteOperations, []byte(l.ID))
		if e != nil {
			return e
		}
		if o == nil || o.Resolved || l.Owner == "" || o.Lease.Owner != l.Owner || o.Lease.Fence != l.Fence || !o.Lease.Live(s.now()) {
			return core.ErrLeaseLost
		}
		t, e := getTurn(tx, o.TurnID)
		if e != nil {
			return e
		}
		if t == nil {
			return core.ErrConflict
		}
		owner, q, e := s.remoteQuota(tx, t.Turn)
		if e != nil {
			return e
		}
		next, e := o.Apply(u, s.now())
		if e != nil {
			return e
		}
		o = &next
		if o.Resolved {
			delete(q.Remote, o.ID)
		}
		if e = putJSON(tx, bucketRemoteOperations, []byte(o.ID), o); e != nil {
			return e
		}
		out = *o
		return putJSON(tx, bucketQuotas, []byte(owner), q)
	})
	return out, e
}

// GetRemoteOperationContext 读取原始输入及冻结绑定，不重新领取 Turn。
func (s *store) GetRemoteOperationContext(ctx context.Context, id string) (*core.TurnLease, error) {
	var out *core.TurnLease
	e := s.view(ctx, func(tx storeTx) error {
		o, e := getJSON[core.RemoteOperation](tx, bucketRemoteOperations, []byte(id))
		if e != nil {
			return e
		}
		if o == nil {
			return core.ErrNotFound
		}
		t, e := getTurn(tx, o.TurnID)
		if e != nil {
			return e
		}
		if t == nil {
			return core.ErrNotFound
		}
		m, e := getJSON[core.InboundMessage](tx, bucketMessages, []byte(t.MessageID))
		if e != nil {
			return e
		}
		v, e := getJSON[core.BindingVersion](tx, bucketVersions, key([]byte(t.BindingID), []byte(t.BindingVersion)))
		if e != nil {
			return e
		}
		if m == nil || v == nil {
			return core.ErrConflict
		}
		out = &core.TurnLease{Turn: t.Turn, Message: *m, Binding: *v}
		return nil
	})
	if e == nil {
		e = s.hydrateTurnLease(ctx, out)
	}
	return out, e
}
