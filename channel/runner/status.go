// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"errors"
	"sort"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/secret"
)

// ConversationStatus 是可公开展示的会话绑定状态。
type ConversationStatus struct {
	Conversation core.RouteKey     `json:"conversation"`
	State        core.SessionState `json:"state"`
	Generation   uint64            `json:"generation"`
	SessionID    string            `json:"session_id,omitempty"`
}

// TurnStatus 是可公开展示的单轮状态，不包含消息正文或 Binding。
type TurnStatus struct {
	ID        string          `json:"id"`
	Status    core.TurnStatus `json:"status"`
	Phase     core.TurnPhase  `json:"phase,omitempty"`
	SessionID string          `json:"session_id,omitempty"`
	ErrorKind string          `json:"error_kind,omitempty"`
}

// DeliveryStatus 是可公开展示的投递状态。
type DeliveryStatus struct {
	ID     string              `json:"id"`
	Source core.DeliverySource `json:"source"`
	Seq    int                 `json:"seq"`
	State  core.DeliveryStatus `json:"state"`
	Error  string              `json:"error_kind,omitempty"`
}

// StatusSnapshot 是运维用的状态投影：队列概况加分页后的各实体视图。
type StatusSnapshot struct {
	Stats         core.QueueStats      `json:"stats"`
	Conversations []ConversationStatus `json:"conversations"`
	Turns         []TurnStatus         `json:"turns"`
	Deliveries    []DeliveryStatus     `json:"deliveries"`
}

// Status 只读打开既有数据库；embedded 下运行中的 writer 持锁时提示停机。
func Status(ctx context.Context, config Config) (StatusSnapshot, error) {
	return StatusWith(ctx, config, nil)
}

// StatusWith 允许测试注入 Secret resolver。
func StatusWith(ctx context.Context, config Config, resolver secret.Resolver) (StatusSnapshot, error) {
	if ctx == nil {
		return StatusSnapshot{}, errors.New("status context required")
	}
	if err := config.Validate(); err != nil {
		return StatusSnapshot{}, err
	}
	store, err := openStore(ctx, config, resolver, modeReadOnly)
	if err != nil {
		return StatusSnapshot{}, err
	}
	defer func() { _ = store.Close() }()
	return projectStatus(ctx, store)
}

func outcomeError(turn core.Turn) string {
	if turn.Outcome != nil {
		return turn.Outcome.ErrorKind
	}
	return ""
}

// projectStatus 通过 Stats 与分页列表（而不是全量快照）构造状态视图。
func projectStatus(ctx context.Context, store core.Store) (StatusSnapshot, error) {
	stats, err := store.Stats(ctx)
	if err != nil {
		return StatusSnapshot{}, errors.New("database stats unavailable")
	}
	conversations, err := store.ListConversations(ctx, core.ConversationQuery{Limit: core.MaxPageSize})
	if err != nil {
		return StatusSnapshot{}, err
	}
	turns, err := store.ListTurns(ctx, core.TurnQuery{Limit: core.MaxPageSize})
	if err != nil {
		return StatusSnapshot{}, err
	}
	deliveries, err := store.ListDeliveries(ctx, core.DeliveryQuery{Limit: core.MaxPageSize})
	if err != nil {
		return StatusSnapshot{}, err
	}
	snapshot := StatusSnapshot{Stats: stats,
		Conversations: make([]ConversationStatus, 0, len(conversations)),
		Turns:         make([]TurnStatus, 0, len(turns)),
		Deliveries:    make([]DeliveryStatus, 0, len(deliveries))}
	for _, conversation := range conversations {
		snapshot.Conversations = append(snapshot.Conversations, ConversationStatus{Conversation: conversation.ID, State: conversation.Session.State, Generation: conversation.Session.Generation, SessionID: conversation.Session.SessionID})
	}
	for _, turn := range turns {
		snapshot.Turns = append(snapshot.Turns, TurnStatus{ID: turn.ID, Status: turn.Status, Phase: turn.Phase, SessionID: turn.Session.SessionID, ErrorKind: outcomeError(turn)})
	}
	for _, delivery := range deliveries {
		snapshot.Deliveries = append(snapshot.Deliveries, DeliveryStatus{ID: delivery.ID, Source: delivery.Source, Seq: delivery.Seq, State: delivery.Status, Error: delivery.ErrorKind})
	}
	sort.Slice(snapshot.Conversations, func(i, j int) bool {
		return snapshot.Conversations[i].Conversation < snapshot.Conversations[j].Conversation
	})
	return snapshot, nil
}
