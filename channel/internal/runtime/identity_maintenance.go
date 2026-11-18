// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"sync"
)

// maintainBotIdentities 只遍历未终态引用，每页等待有限 worker 退出后再读取下一页。
func (r *Runtime) maintainBotIdentities(ctx context.Context) error {
	for after := ""; ; {
		turns, err := r.store.ListTurns(ctx, TurnQuery{OpenOnly: true, After: after, Limit: MaxPageSize})
		if err != nil {
			if isUnsupportedWork(err) {
				return err
			}
			return NewStoreFailure(err)
		}
		if err := r.maintainIdentityPage(ctx, turns); err != nil {
			return err
		}
		if len(turns) < MaxPageSize {
			break
		}
		after = turns[len(turns)-1].ID
	}
	return r.maintainRemoteIdentities(ctx)
}

// maintainRemoteIdentities 在同一个维护循环恢复本地终态但远端未解的冻结主体。
func (r *Runtime) maintainRemoteIdentities(ctx context.Context) error {
	store, ok := r.store.(RemoteOperationStore)
	if !ok {
		return nil
	}
	for after := ""; ; {
		ops, err := store.ListRemoteOperations(ctx, RemoteOperationQuery{OpenOnly: true, After: after, Limit: MaxPageSize})
		if err != nil {
			if isUnsupportedWork(err) {
				return err
			}
			return NewStoreFailure(err)
		}
		var turns []Turn
		for _, op := range ops {
			if err := remoteWorkError(op); err != nil {
				if !isUnsupportedWork(err) {
					return err
				}
				r.report(ctx, Observation{Name: EventWorkBlocked, Status: "blocked", ErrorKind: "unsupported_work", BindingID: op.BindingID, TurnID: op.TurnID, ConversationID: op.ConversationID})
				continue
			}
			turn, err := r.store.GetTurn(ctx, op.TurnID)
			if err != nil {
				return NewStoreFailure(err)
			}
			if turn == nil || !turn.Status.Terminal() {
				continue
			}
			turn.Session = TurnSession{SnapshotID: op.SnapshotID, Generation: op.Generation, SessionID: op.SessionID, CreationKey: op.CreationKey}
			turns = append(turns, *turn)
		}
		if err := r.maintainIdentityPage(ctx, turns); err != nil {
			return err
		}
		if len(ops) < MaxPageSize {
			return nil
		}
		after = ops[len(ops)-1].ID
	}
}

func (r *Runtime) maintainIdentityPage(ctx context.Context, turns []Turn) error {
	pageCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan Turn)
	failures := make(chan error, 1)
	var wg sync.WaitGroup
	for i := 0; i < r.options.IdentityRenewalConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if recover() != nil {
					select {
					case failures <- errIdentityUnavailable:
					default:
					}
					cancel()
				}
			}()
			for turn := range jobs {
				if pageCtx.Err() != nil {
					return
				}
				if err := r.maintainTurnIdentity(pageCtx, turn); err != nil {
					if isUnsupportedWork(err) {
						r.report(pageCtx, Observation{Name: EventWorkBlocked, Status: "blocked", ErrorKind: "unsupported_work", BindingID: turn.BindingID, TurnID: turn.ID, ConversationID: turn.ConversationID})
						continue
					}
					if isStoreFailure(err) {
						select {
						case failures <- err:
						default:
						}
						cancel()
						return
					}
					if pageCtx.Err() == nil {
						r.report(pageCtx, Observation{Name: EventAuth, Status: "failed", ErrorKind: "identity_renewal", BindingID: turn.BindingID})
					}
				}
			}
		}()
	}
send:
	for _, turn := range turns {
		select {
		case <-pageCtx.Done():
			break send
		case jobs <- turn:
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-failures:
		return err
	default:
		return ctx.Err()
	}
}

func (r *Runtime) maintainTurnIdentity(ctx context.Context, turn Turn) error {
	if err := turnWorkError(turn); err != nil {
		return err
	}
	if turn.Session.SnapshotID == "" {
		return nil
	}
	callCtx, cancel := context.WithTimeout(ctx, r.options.DeliveryTimeout*5)
	defer cancel()
	snapshot, err := r.store.GetSessionSnapshot(callCtx, turn.Session.SnapshotID)
	if err != nil {
		if isUnsupportedWork(err) {
			return err
		}
		return NewStoreFailure(err)
	}
	if err := sessionWorkError(snapshot); err != nil {
		return err
	}
	if snapshot == nil || !snapshot.Valid() || snapshot.Route != turn.ConversationID || snapshot.Generation != turn.Session.Generation {
		return ErrConflict
	}
	frozen := snapshot.Spec.Binding
	chatType := snapshot.Spec.ChatType
	if err := validateIdentityForChat(frozen, chatType); err != nil {
		return err
	}
	if effectiveIdentity(frozen, chatType) != IdentityBot {
		return nil
	}
	connection, err := r.options.Resolver.ResolveForTurn(callCtx, turn, snapshot)
	// 解析失败也可能带有已获取的租约，所有出口均归还。
	defer releaseConnection(connection)
	if err != nil {
		return err
	}
	b := connection.Binding
	if b.ID != frozen.ID || b.AppID != frozen.AppID || b.Owner != frozen.Owner || b.Project != frozen.Project || b.ChannelID != frozen.ChannelID || b.BindingEpoch != frozen.BindingEpoch {
		return ErrRuntimeConfig
	}
	// 即使 resolver 返回当前默认配置，续期位置始终使用 Session 已冻结的引用。
	connection.Binding = frozen
	return r.refreshBotIdentity(callCtx, connection, chatType, true)
}
