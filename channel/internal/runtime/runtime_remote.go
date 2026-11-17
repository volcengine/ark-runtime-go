// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
)

func remoteKind(j *job) RemoteKind {
	if j.Phase == PhaseCreate {
		return RemoteCreate
	}
	return RemoteInput
}
func (r *Runtime) beginRemote(ctx context.Context, j *job) error {
	kind := remoteKind(j)
	limit := r.options.AccountLimit
	if kind == RemoteCreate {
		limit = r.options.CreationLimit
	}
	intent := &RemoteIntent{Kind: kind, Limit: limit}
	if j.userCredential != nil {
		intent.UserAuthKey = j.userCredential.Principal.Key()
		intent.GrantVersion = j.userCredential.GrantVersion
	}
	return r.updateTurn(ctx, j, TurnUpdate{Phase: j.Phase, Session: j.Session, Submit: j.Submit, RemoteIntent: intent})
}
func (r *Runtime) proveRemote(j *job, kind RemoteKind, proof RemoteProofKind, reference string) error {
	ctx, cancel := shortContext()
	defer cancel()
	return r.updateTurn(ctx, j, TurnUpdate{Phase: j.Phase, Session: j.Session, Submit: j.Submit, RemoteKind: kind, RemoteProof: &RemoteProof{Kind: proof, Reference: reference}})
}
func (r *Runtime) remoteLoop(ctx context.Context) error {
	store, ok := r.store.(RemoteOperationStore)
	if !ok {
		return nil
	}
	for ctx.Err() == nil {
		op, err := store.LeaseRemoteOperation(ctx, RemoteClaim{UserAuth: r.supportsUserAuth(), Owner: "remote-" + r.instanceID, TTL: r.options.DeliveryTimeout * 2})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if isUnsupportedWork(err) {
				if r.waitUnsupported(ctx, Observation{}) != nil {
					return nil
				}
				continue
			}
			return err
		}
		if op == nil {
			if r.waitPoll(ctx) != nil {
				return nil
			}
			continue
		}
		if err := r.reconcileRemote(ctx, store, *op); err != nil {
			if isUnsupportedWork(err) {
				if r.waitUnsupported(ctx, Observation{BindingID: op.BindingID, TurnID: op.TurnID, ConversationID: op.ConversationID}) != nil {
					return nil
				}
				continue
			}
			return err
		}
	}
	return nil
}
func (r *Runtime) reconcileRemote(ctx context.Context, store RemoteOperationStore, op RemoteOperation) error {
	if err := remoteWorkError(op); err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, r.options.DeliveryTimeout)
	defer cancel()
	// 清理始终绑定原 SessionSnapshot，不通过当前 Conversation 重新定位。
	original, err := store.GetRemoteOperationContext(callCtx, op.ID)
	if err != nil {
		if isUnsupportedWork(err) {
			return err
		}
		return NewStoreFailure(err)
	}
	if original == nil {
		return NewStoreFailure(ErrNotFound)
	}
	j := &job{Turn: original.Turn, msg: original.Message.Message, binding: original.Binding.Content}
	if err := turnWorkError(j.Turn); err != nil {
		return err
	}
	j.Session = TurnSession{SnapshotID: op.SnapshotID, Generation: op.Generation, SessionID: op.SessionID, CreationKey: op.CreationKey}
	j.InputEventID = op.InputEventID
	j.Submit.SubmittedAt = op.SubmittedAt
	connection, err := r.resolveCleanup(callCtx, j)
	defer releaseConnection(connection)
	if isStoreFailure(err) || isUnsupportedWork(err) {
		return err
	}
	update := RemoteUpdate{NextReconcileAt: r.options.Clock().Add(r.options.PollInterval), AttentionRequired: true}
	if err == nil {
		if op.Kind == RemoteCreate {
			proof, e := r.findCreatedProof(callCtx, j, connection.MAClient)
			if isStoreFailure(e) {
				return e
			}
			if e == nil {
				update.Proof = proof
			}
		} else {
			session, e := connection.MAClient.GetSession(callCtx, op.SessionID)
			if e != nil && sessionGoneError(e) || e == nil && unusableSession(session.Status) {
				update.Proof = &RemoteProof{Kind: RemoteGone, Reference: op.SessionID}
			} else if e == nil {
				events, e := scanEvents(callCtx, connection.MAClient, op.SessionID, historyWindow(j))
				if e == nil && op.CleanupStarted {
					if proof := interruptProof(events, j); proof != nil {
						// 历史扫描后再次读权威状态，不能用扫描前的 idle 覆盖后来恢复的执行。
						latest, lookupErr := connection.MAClient.GetSession(callCtx, op.SessionID)
						if lookupErr == nil && latest.Status == "idle" {
							update.Proof = proof
						}
					}
				}
				if e == nil && !op.CleanupStarted {
					outcome := judgeTurn(events, j)
					if outcome.Status == "done" || outcome.Status == "agent_failed" {
						update.Proof = &RemoteProof{Kind: RemoteTerminal, Reference: op.InputEventID}
					}
					if outcome.Status == "session_gone" {
						update.Proof = &RemoteProof{Kind: RemoteGone, Reference: op.SessionID}
					}
				}
				if update.Proof == nil && !op.CleanupStarted {
					// 中断意图先持久化；旧 fence、迟到 HTTP、取消均不能撤回这个屏障。
					held, e := store.UpdateRemoteOperation(callCtx, op.Lease, RemoteUpdate{CleanupStarted: true, AttentionRequired: true})
					if errors.Is(e, ErrLeaseLost) {
						return nil
					}
					if e != nil {
						return NewStoreFailure(e)
					}
					op = held
					_, _ = connection.MAClient.SendInterrupt(callCtx, op.SessionID, interruptEventID(j))
				}
			}
		}
	}
	finishCtx, finishCancel := shortContext()
	defer finishCancel()
	_, err = store.UpdateRemoteOperation(finishCtx, op.Lease, update)
	if err != nil && !errors.Is(err, ErrLeaseLost) {
		return NewStoreFailure(err)
	}
	return nil
}

// cancelRemoteBeforeHTTP 将尚未调用 transport 的确定性事实落盘；失租时旧 fence 仍不得释放。
func (r *Runtime) cancelRemoteBeforeHTTP(j *job, previous Submission, cause error) error {
	j.Submit = previous
	if err := r.proveRemote(j, remoteKind(j), RemoteNotAccepted, "cancelled_before_http"); err != nil {
		return err
	}
	return cause
}
