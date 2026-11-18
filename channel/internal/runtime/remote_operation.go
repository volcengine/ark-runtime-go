// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"strconv"
	"time"
)

// ErrRemoteLimit 表示账号远端预算已满，观察和清理仍可继续。
var ErrRemoteLimit = errors.New("remote operation limit reached")

// ErrRemoteBarrier 表示同代 Session 仍有未收敛操作。
var ErrRemoteBarrier = errors.New("remote session barrier")

// ErrInterruptStarted 表示中断发送权已被领取，接管者只能对账，不能再次发送。
var ErrInterruptStarted = errors.New("remote interrupt already started")

// RemoteKind 区分创建预算和执行预算。
type RemoteKind string

const (
	// RemoteCreate 表示 Session 创建。
	RemoteCreate RemoteKind = "create"
	// RemoteInput 表示输入执行。
	RemoteInput RemoteKind = "input"
)

// RemoteProofKind 是可审计的远端收敛证明。
type RemoteProofKind string

const (
	// RemoteNotAccepted 证明该次写入明确未发出或未受理。
	RemoteNotAccepted RemoteProofKind = "not_accepted"
	// RemoteCreated 证明已找回唯一匹配的 Session。
	RemoteCreated RemoteProofKind = "created"
	// RemoteTerminal 证明对应输入已消费且出现相关终态。
	RemoteTerminal RemoteProofKind = "terminal"
	// RemoteInterrupted 按本轮中断回显、终态和当前 idle 尽力确认可继续；不证明后台取消已执行完。
	RemoteInterrupted RemoteProofKind = "interrupted"
	// RemoteGone 证明远端 Session 权威终止或删除。
	RemoteGone RemoteProofKind = "gone"
)

// RemoteProof 保存证明类型与引用，不保存响应正文。
type RemoteProof struct {
	Kind      RemoteProofKind
	Reference string
	At        time.Time
}

// RemoteIntent 随 TurnUpdate 原子写入并占用对应预算。
type RemoteIntent struct {
	UserAuthKey  string
	GrantVersion string
	Kind         RemoteKind
	Limit        int
}

// RemoteOperation 独立保留远端写入事实，本地终态不释放其预算。
type RemoteOperation struct {
	WorkKind     string `json:"_work_kind,omitempty"`
	UserAuthKey  string `json:"user_auth_key,omitempty"`
	GrantVersion string `json:"grant_version,omitempty"`
	StoredRecord `json:"-"`
	// Project、ChannelID、BindingEpoch 保存冻结授权作用域。
	Project        string     `json:"project,omitempty"`
	ChannelID      string     `json:"channel_id,omitempty"`
	BindingEpoch   uint64     `json:"binding_epoch,omitempty"`
	RequestHash    string     `json:"request_hash"`
	State          string     `json:"state"`
	ID             string     `json:"id"`
	Owner          string     `json:"owner"`
	TurnID         string     `json:"turn_id"`
	BindingID      string     `json:"binding_id"`
	BindingVersion string     `json:"binding_version"`
	SnapshotID     string     `json:"snapshot_id"`
	ConversationID RouteKey   `json:"conversation_id"`
	Generation     uint64     `json:"generation"`
	Kind           RemoteKind `json:"kind"`
	SessionID      string     `json:"session_id"`
	InputEventID   string     `json:"input_event_id"`
	CreationKey    string     `json:"creation_key"`
	SubmittedAt    time.Time  `json:"submitted_at"`
	Resolved       bool       `json:"resolved"`
	Counted        bool       `json:"counted"`
	// CleanupStarted 是中断发送意图；SDK 接管后只对账，不再次发送。
	CleanupStarted    bool         `json:"cleanup_started"`
	AttentionRequired bool         `json:"attention_required"`
	Proof             *RemoteProof `json:"proof,omitempty"`
	Lease             Lease        `json:"lease"`
	Revision          uint64       `json:"revision"`
	NextReconcileAt   time.Time    `json:"next_reconcile_at"`
}

// RemoteOperationID 返回稳定的每 Turn/代/操作标识。
func RemoteOperationID(turn Turn, kind RemoteKind) string {
	return "remote:" + stableKey(turn.ID, string(kind), strconv.FormatUint(turn.Session.Generation, 10))
}

// InterruptEventID 返回冻结轮次的确定性中断 ID，供历史对账和发送共同使用。
func (o RemoteOperation) InterruptEventID() string {
	return "sevt-ch" + stableKey(o.TurnID, "interrupt", strconv.FormatUint(o.Generation, 10))[:32]
}

// NewRemoteOperation 从冻结 Turn 生成远端操作。
func NewRemoteOperation(turn Turn, owner string, kind RemoteKind) RemoteOperation {
	op := RemoteOperation{State: "intent", RequestHash: digestJSON(struct {
		Session   TurnSession
		InputHash string
		EventID   string
	}{turn.Session, turn.storedInputHash(), turn.InputEventID}), ID: RemoteOperationID(turn, kind), Owner: owner, TurnID: turn.ID, BindingID: turn.BindingID, BindingVersion: turn.BindingVersion, SnapshotID: turn.Session.SnapshotID, ConversationID: turn.ConversationID, Generation: turn.Session.Generation, Kind: kind, SessionID: turn.Session.SessionID, InputEventID: turn.InputEventID, CreationKey: turn.Session.CreationKey, SubmittedAt: turn.Submit.SubmittedAt, Counted: true, Revision: 1}
	if turn.Snapshot != nil {
		op.Project = turn.Snapshot.Binding.Project
		op.ChannelID = turn.Snapshot.Binding.ChannelID
		op.BindingEpoch = turn.Snapshot.Binding.BindingEpoch
	}
	return op
}

// Resolve 校验证明并幂等释放；中断在途时相关输入终态仍不能排除迟到中断。
func (o RemoteOperation) Resolve(proof RemoteProof, now time.Time) (RemoteOperation, error) {
	if proof.Reference == "" {
		return o, ErrInvalid
	}
	if o.CleanupStarted && proof.Kind != RemoteGone && proof.Kind != RemoteInterrupted {
		return o, ErrRemoteBarrier
	}
	switch proof.Kind {
	case RemoteNotAccepted:
		if o.CleanupStarted {
			return o, ErrRemoteBarrier
		}
	case RemoteCreated:
		if o.Kind != RemoteCreate {
			return o, ErrInvalid
		}
	case RemoteTerminal:
		if o.Kind != RemoteInput || o.CleanupStarted {
			return o, ErrRemoteBarrier
		}
	case RemoteInterrupted:
		if o.Kind != RemoteInput || !o.CleanupStarted || proof.Reference != o.InterruptEventID() {
			return o, ErrRemoteBarrier
		}
	case RemoteGone:
	default:
		return o, ErrInvalid
	}
	proof.At = now
	o.State = "resolved"
	o.Proof = &proof
	o.Resolved = true
	o.Counted = false
	o.AttentionRequired = false
	o.NextReconcileAt = time.Time{}
	o.Lease.Owner = ""
	o.Lease.ExpiresAt = time.Time{}
	o.Revision++
	return o, nil
}

// Apply 先纳入本次清理意图再校验证明，防止同次terminal证明绕过新屏障。
// 返回错误时原记录不变；新登记 cleanup 的同次更新仍只接受权威 gone。
func (o RemoteOperation) Apply(update RemoteUpdate, now time.Time) (RemoteOperation, error) {
	if o.Resolved {
		return o, ErrLeaseLost
	}
	if update.Proof != nil && update.Proof.Kind == RemoteInterrupted && !o.CleanupStarted {
		return o, ErrRemoteBarrier
	}
	next := o
	if update.CleanupStarted {
		if o.CleanupStarted && update.Proof == nil {
			return o, ErrInterruptStarted
		}
		next.CleanupStarted = true
		next.State = "closing"
	}
	next.AttentionRequired = next.AttentionRequired || update.AttentionRequired
	if update.Proof != nil {
		resolved, err := next.Resolve(*update.Proof, now)
		if err != nil {
			return o, err
		}
		return resolved, nil
	}
	next.Revision++
	if !update.NextReconcileAt.IsZero() {
		next.NextReconcileAt = update.NextReconcileAt
		next.Lease.Owner = ""
		next.Lease.ExpiresAt = time.Time{}
	}
	return next, nil
}

// RemoteOperationQuery 使用稳定 ID 分页，支持终态 Turn 的引用恢复。
type RemoteOperationQuery struct {
	TurnID   string
	OpenOnly bool
	After    string
	Limit    int
}

// RemoteClaim 指定后台操作租约。只有本地终态或已移交操作可领取。
type RemoteClaim struct {
	// UserAuth 显式开启 User 操作恢复，默认只领取旧协议。
	UserAuth bool
	Owner    string
	TTL      time.Duration
}

// RemoteUpdate 更新后台收敛进度；清理意图必须在 HTTP 前持久化。
type RemoteUpdate struct {
	Proof             *RemoteProof
	CleanupStarted    bool
	AttentionRequired bool
	NextReconcileAt   time.Time
}

// RemoteOperationStore 提供独立于本地 Turn 的对账及持久 fence。
type RemoteOperationStore interface {
	// GetRemoteOperationContext 读取原 Turn、入站正文与冻结绑定；不授予执行租约。
	GetRemoteOperationContext(context.Context, string) (*TurnLease, error)
	ListRemoteOperations(context.Context, RemoteOperationQuery) ([]RemoteOperation, error)
	LeaseRemoteOperation(context.Context, RemoteClaim) (*RemoteOperation, error)
	UpdateRemoteOperation(context.Context, Lease, RemoteUpdate) (RemoteOperation, error)
}
