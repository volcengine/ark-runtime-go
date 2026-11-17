// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// StoreConfig 是契约套件对被测 Store 的要求：Clock 必须作为 Store 唯一的时间来源，
// MaxOpenTurns 与 MaxMessageBytes 为 0 时使用实现默认值。
type StoreConfig struct {
	Clock                func() time.Time
	MaxChannelOpenTurns  int
	MaxRejectedMessages  int
	RejectNoticeInterval time.Duration
	MaxOpenTurns         int
	MaxMessageBytes      int
	QueueTTL             time.Duration
}

// TestStore 是被测 Store：除 core.Store 外还要实现测试用的 Exporter。
type TestStore interface {
	core.Store
	core.Exporter
}

// StoreFactory 为每个子测试创建一个全新的空 Store；关闭由套件负责。
type StoreFactory func(t *testing.T, config StoreConfig) TestStore

// Clock 是可手动推进的测试时钟。
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// NewClock 返回从 start 开始的时钟。
func NewClock(start time.Time) *Clock { return &Clock{now: start} }

// Now 返回当前时刻。
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance 把时钟向前推进 d。
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// SampleBinding 是契约用例共用的 Binding。
func SampleBinding() core.Binding {
	return core.Binding{ID: "binding", AppID: "app", AgentID: "agent", AgentVersion: "1", VaultIDs: []string{"vault"}, MemoryIDs: []string{"memory"}}
}

// SampleVersion 是 SampleBinding 的配置版本。
func SampleVersion() core.BindingVersion { return core.NewBindingVersion(SampleBinding()) }

// SampleMessage 返回同一会话上以 id 区分的群聊消息。
func SampleMessage(id string) core.Message {
	return core.Message{BindingID: "binding", ProviderEventID: "event-" + id, MessageID: "message-" + id, TenantKey: "tenant", ChatID: "chat", ChatType: "group", SenderID: "user", Text: "text " + id}
}

// StopMessage 返回同一单聊会话里的 /stop 命令；与 P2PMessage 共用一个会话。
func StopMessage(id string) core.Message {
	m := P2PMessage(id)
	m.Text = "/stop"
	return m
}

// P2PMessage 返回单聊会话上以 id 区分的消息。
func P2PMessage(id string) core.Message {
	m := SampleMessage(id)
	m.ChatType, m.ChatID = "p2p", "p2p-chat"
	return m
}

// Claim 是契约用例默认的领取参数。
func Claim(owner string) core.ClaimOptions {
	return core.ClaimOptions{Owner: owner, TTL: time.Minute, MessageDeadline: time.Hour}
}

// storeCase 是一个契约用例；name 即子测试名。
type storeCase struct {
	name string
	run  func(*testing.T, opener)
}

// storeCases 返回完整通用契约，不含大规模用例。
func storeCases() []storeCase {
	return []storeCase{
		{"TurnStageClaim", turnStageClaim},
		{"TurnStageExpiredTail", turnStageExpiredTail},
		{"MessageReactions", reactionContract},
		{"MessageRecall", recallContract},
		{"UnsupportedMessageRepliesWithoutExecution", unsupportedMessageContract},
		{"DurableProgress", progressContract},
		{"StreamingText", textStreamContract},
		{"RuntimeTakeover", runtimeTakeover},
		{"AccountQuota", accountQuotaContract},
		{"AdmissionPermit", admissionContract},
		{"SessionHistory", sessionHistoryContract},
		{"QueueTailWithFullAccountQuota", queueTailFullQuota},
		{"QueueTailRecovery", queueTailRecoveryContract},
		{"QueueTailDeadline", queueTailDeadlineContract},
		{"ImmutableSessionSnapshot", snapshotContract},
		{"IngestDeduplicatesByMessageAndEvent", ingestDedupe},
		{"MessageMetadataUpgrade", messageMetadataUpgrade},
		{"IngestWritesVersionAndConversationOnce", ingestVersionAndConversation},
		{"IngestLimits", ingestLimits},
		{"ControlWithoutTargetRepliesInSameTransaction", controlNoTarget},
		{"ControlStopsEarliestOpenTurn", controlStops},
		{"ControlWakesWaitingTurn", controlWakesWaiting},
		{"LeasesOnlyConversationHeadWithFence", turnHeadAndFence},
		{"WaitReleasesLeaseAndBlocksOnlyItsConversation", turnWait},
		{"ReleaseKeepsProgress", turnRelease},
		{"UpdateFollowsTransitionRules", turnUpdate},
		{"SettleIsAtomicAndAdvancesHead", turnSettle},
		{"BindSessionGenerations", conversationBind},
		{"ResetReservesNextGeneration", conversationReset},
		{"InteractionLifecycle", interactionLifecycle},
		{"DeliveryRenewalFences", deliveryRenewal},
		{"DeliveryLeaseAndFence", deliveryLease},
		{"DeliveriesOfOneSourceAreSerial", deliverySerial},
		{"RetryDeliveryOnlyFailed", deliveryRetry},
		{"StatsAndPaging", adminStatsAndPaging},
		{"PruneRemovesSettledRecords", adminPrune},
		{"ExportIsIsolated", exportIsolated},
		{"CanceledContext", canceled},
	}
}

// payloadCaseNames 是加密载荷变体运行的子集：覆盖每类加密记录（入站消息、Turn、Delivery、
// Session 快照）的写入与读回，以及只在加密模式存在的清理和投递领取路径。
// 通用状态机断言由同一实现的明文完整契约承担。
var payloadCaseNames = []string{
	"MessageRecall", "MessageMetadataUpgrade", "IngestDeduplicatesByMessageAndEvent", "UnsupportedMessageRepliesWithoutExecution",
	"UpdateFollowsTransitionRules", "RuntimeTakeover", "ControlStopsEarliestOpenTurn", "ControlWakesWaitingTurn",
	"SettleIsAtomicAndAdvancesHead", "DeliveryLeaseAndFence", "DeliveryRenewalFences", "DeliveriesOfOneSourceAreSerial",
	"StreamingText", "DurableProgress", "ImmutableSessionSnapshot",
	"PruneRemovesSettledRecords", "ExportIsIsolated",
}

func storeOpener(factory StoreFactory) opener {
	return func(t *testing.T, config StoreConfig) (TestStore, *Clock) {
		t.Helper()
		clock := NewClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		if config.Clock == nil {
			config.Clock = clock.Now
		}
		s := factory(t, config)
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		return s, clock
	}
}

func runStoreCases(t *testing.T, factory StoreFactory, cases []storeCase) {
	open := storeOpener(factory)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.run(t, open) })
	}
}

// RunStore 对 factory 创建的 Store 执行完整通用契约。所有错误断言只依赖 core 中的哨兵错误。
// 每个独立实现运行一次；与已运行实现共享代码的变体改用 RunStorePayload。
func RunStore(t *testing.T, factory StoreFactory) {
	runStoreCases(t, factory, storeCases())
}

// RunStorePayload 对启用载荷加密的 Store 执行 payloadCaseNames 子集。
func RunStorePayload(t *testing.T, factory StoreFactory) {
	cases, err := selectStoreCases(payloadCaseNames)
	require.NoError(t, err)
	runStoreCases(t, factory, cases)
}

func selectStoreCases(names []string) ([]storeCase, error) {
	byName := make(map[string]storeCase)
	for _, c := range storeCases() {
		byName[c.name] = c
	}
	selected := make([]storeCase, 0, len(names))
	for _, name := range names {
		c, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("unknown store contract case %q", name)
		}
		selected = append(selected, c)
	}
	return selected, nil
}

func messageMetadataUpgrade(t *testing.T, open opener) {
	s, _ := open(t, StoreConfig{})
	message := SampleMessage("legacy-metadata")
	first := ingest(t, s, message)
	message.Metadata = &core.MessageMetadata{MessageType: "text", Mentions: []core.Mention{{OpenID: "ou_a", Name: "姓名"}}}
	duplicate, err := s.Ingest(t.Context(), SampleVersion(), message)
	require.NoError(t, err)
	require.True(t, duplicate.Duplicate)
	require.Equal(t, first.Message.ContentHash, duplicate.Message.ContentHash)
	message.MessageID, message.ProviderEventID = "new-metadata", "new-metadata-event"
	created := ingest(t, s, message)
	message.Metadata.Mentions[0].Name = "篡改姓名"
	duplicate, err = s.Ingest(t.Context(), SampleVersion(), message)
	require.NoError(t, err)
	require.True(t, duplicate.Duplicate)
	require.Equal(t, "姓名", duplicate.Message.Message.Metadata.Mentions[0].Name, "首次受理的来源事实不得被重推覆盖")
	message.Metadata = nil
	duplicate, err = s.Ingest(t.Context(), SampleVersion(), message)
	require.NoError(t, err)
	require.Equal(t, created.Message.ContentHash, duplicate.Message.ContentHash, "旧 worker 不携带 metadata 的重推仍须兼容")
}

type opener = func(*testing.T, StoreConfig) (TestStore, *Clock)

func ingest(t *testing.T, s core.Store, message core.Message) core.IngestResult {
	t.Helper()
	result, err := s.Ingest(context.Background(), SampleVersion(), message)
	require.NoError(t, err)
	return result
}

func lease(t *testing.T, s core.Store, owner string) *core.TurnLease {
	t.Helper()
	leased, err := s.LeaseTurn(context.Background(), Claim(owner))
	require.NoError(t, err)
	return leased
}

func answer(turn core.Turn, key, text string) core.Delivery {
	return core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: turn.ID}, 0, key, turn.BindingID, core.ReplyTarget{Mode: "reply", ChatID: "chat"}, core.TextBody(text))
}

func ingestDedupe(t *testing.T, open opener) {
	ctx := context.Background()
	s, clock := open(t, StoreConfig{})
	message := SampleMessage("one")
	first := ingest(t, s, message)
	require.False(t, first.Duplicate)
	require.NotNil(t, first.Turn)
	require.Equal(t, core.TurnQueued, first.Turn.Status)
	require.Equal(t, core.RouteFor(message), first.Turn.ConversationID)
	require.Equal(t, first.Message.ID, first.Turn.MessageID)
	require.Equal(t, core.InputEventIDFor(core.ContentHash(message)), first.Turn.InputEventID)
	require.Equal(t, SampleVersion().Version, first.Turn.BindingVersion)
	// 同一渠道事件重放：接收时间不同也必须去重。
	clock.Advance(time.Minute)
	message.ReceivedAt = clock.Now()
	again := ingest(t, s, message)
	require.True(t, again.Duplicate)
	require.Equal(t, first.Turn.ID, again.Turn.ID)
	// 同一消息以新事件 ID 重投递：按消息 ID 去重，新事件 ID 记为别名。
	message.ProviderEventID = "redelivered"
	same := ingest(t, s, message)
	require.True(t, same.Duplicate)
	require.Equal(t, first.Turn.ID, same.Turn.ID)
	// 同一消息 ID 对应不同内容是冲突；同一事件 ID 对应不同消息也是冲突。
	changed := message
	changed.Text = "different"
	_, err := s.Ingest(ctx, SampleVersion(), changed)
	require.ErrorIs(t, err, core.ErrConflict)
	reused := SampleMessage("two")
	reused.ProviderEventID = "redelivered"
	_, err = s.Ingest(ctx, SampleVersion(), reused)
	require.ErrorIs(t, err, core.ErrConflict)
	export, err := s.Export(ctx)
	require.NoError(t, err)
	require.Len(t, export.Messages, 1)
	require.Len(t, export.Turns, 1)
}

func ingestVersionAndConversation(t *testing.T, open opener) {
	ctx := context.Background()
	s, _ := open(t, StoreConfig{})
	_, err := s.Ingest(ctx, core.BindingVersion{BindingID: "binding", Version: "bv1:forged", Content: SampleBinding()}, SampleMessage("x"))
	require.ErrorIs(t, err, core.ErrInvalid, "a version must match its content")
	wrong := SampleMessage("x")
	wrong.BindingID = "other"
	_, err = s.Ingest(ctx, SampleVersion(), wrong)
	require.ErrorIs(t, err, core.ErrInvalid)
	ingest(t, s, SampleMessage("one"))
	ingest(t, s, SampleMessage("two"))
	changed := SampleBinding()
	changed.ShowProgress = true
	_, err = s.Ingest(ctx, core.NewBindingVersion(changed), SampleMessage("three"))
	require.NoError(t, err)
	versions, err := s.ListBindingVersions(ctx)
	require.NoError(t, err)
	require.Len(t, versions, 2, "each distinct configuration is stored once")
	conversation, err := s.GetConversation(ctx, core.RouteFor(SampleMessage("one")))
	require.NoError(t, err)
	require.Equal(t, core.SessionNone, conversation.Session.State)
	require.Equal(t, core.ConversationScope{TenantKey: "tenant", ChatType: "group", ChatID: "chat"}, conversation.Scope)
	missing, err := s.GetConversation(ctx, "route:missing")
	require.NoError(t, err)
	require.Nil(t, missing)
}

func ingestLimits(t *testing.T, open opener) {
	ctx := context.Background()
	s, _ := open(t, StoreConfig{MaxOpenTurns: 1, MaxMessageBytes: 8})
	first := SampleMessage("one")
	first.Text = "12345678"
	ingest(t, s, first)
	again := ingest(t, s, first)
	require.True(t, again.Duplicate, "duplicates are accepted even when the queue is full")
	tooLong := SampleMessage("long")
	tooLong.Text = "123456789"
	_, err := s.Ingest(ctx, SampleVersion(), tooLong)
	require.ErrorIs(t, err, core.ErrInvalid)
	second := SampleMessage("two")
	second.Text = "1"
	_, err = s.Ingest(ctx, SampleVersion(), second)
	require.ErrorIs(t, err, core.ErrQueueFull)
	stop := StopMessage("stop")
	_, err = s.Ingest(ctx, SampleVersion(), stop)
	require.NoError(t, err, "control commands bypass the turn capacity")
}

func controlNoTarget(t *testing.T, open opener) {
	ctx := context.Background()
	s, _ := open(t, StoreConfig{})
	result := ingest(t, s, StopMessage("stop"))
	require.Nil(t, result.Turn)
	require.NotNil(t, result.Control)
	require.Empty(t, result.Control.TargetTurnID)
	require.NotNil(t, result.Reply)
	require.Equal(t, core.ReplyNoActiveTurn, result.Reply.Body.Text)
	require.Equal(t, core.DeliverySource{Kind: core.SourceControl, ID: result.Control.ID}, result.Reply.Source)
	require.Equal(t, "reply", result.Reply.ReplyTarget.Mode)
	again := ingest(t, s, StopMessage("stop"))
	require.True(t, again.Duplicate)
	require.Equal(t, result.Control.ID, again.Control.ID)
	delivery, err := s.LeaseDelivery(ctx, "sender", time.Minute)
	require.NoError(t, err)
	require.Equal(t, result.Reply.ID, delivery.ID)
	none, err := s.LeaseTurn(ctx, Claim("w"))
	require.NoError(t, err)
	require.Nil(t, none, "a control command never enters the turn queue")
}

func controlStops(t *testing.T, open opener) {
	ctx := context.Background()
	s, clock := open(t, StoreConfig{})
	first := ingest(t, s, P2PMessage("one"))
	claimed := lease(t, s, "owner")
	require.Equal(t, first.Turn.ID, claimed.Turn.ID)
	command := ingest(t, s, StopMessage("stop"))
	later := ingest(t, s, P2PMessage("later"))
	require.Equal(t, first.Turn.ID, command.Control.TargetTurnID, "stop targets the earliest open turn before the command")
	require.Nil(t, command.Reply)
	require.False(t, command.Stopped.StopRequestedAt.IsZero())
	again := ingest(t, s, StopMessage("stop"))
	require.True(t, again.Duplicate)
	require.Equal(t, first.Turn.ID, again.Control.TargetTurnID, "an applied stop never moves to another turn")
	// 停止请求不需要租约，也不能让持有者的租约失效；之后的写入必须保留它。
	_, err := s.UpdateTurn(ctx, claimed.Turn.Lease, core.TurnUpdate{Phase: core.PhaseObserve, Session: core.TurnSession{Generation: 1, SessionID: "sesn-1", CreationKey: "k"}})
	require.NoError(t, err)
	_, err = s.RenewTurn(ctx, claimed.Turn.Lease, time.Minute)
	require.NoError(t, err)
	got, err := s.GetTurn(ctx, first.Turn.ID)
	require.NoError(t, err)
	require.False(t, got.StopRequestedAt.IsZero())
	require.Equal(t, claimed.Turn.Lease.Fence, got.Lease.Fence, "stop must not fence out the owner")
	clock.Advance(time.Second)
	require.NoError(t, s.SettleTurn(ctx, claimed.Turn.Lease, core.TurnOutcome{Status: core.TurnStopped}, nil))
	next, err := s.GetTurn(ctx, later.Turn.ID)
	require.NoError(t, err)
	require.True(t, next.StopRequestedAt.IsZero(), "turns after the command are never stopped")
	stopped, err := s.GetTurn(ctx, first.Turn.ID)
	require.NoError(t, err)
	require.Equal(t, core.TurnStopped, stopped.Status)
}

func controlWakesWaiting(t *testing.T, open opener) {
	ctx := context.Background()
	s, clock := open(t, StoreConfig{})
	waiting := ingest(t, s, P2PMessage("one"))
	other := SampleMessage("other")
	other.ChatID = "other-chat"
	otherTurn := ingest(t, s, other)
	claimed := lease(t, s, "owner")
	require.Equal(t, waiting.Turn.ID, claimed.Turn.ID)
	require.NoError(t, s.WaitTurn(ctx, claimed.Turn.Lease, core.TurnWait{Reason: "send_pending", Until: clock.Now().Add(time.Hour)}))
	command := ingest(t, s, StopMessage("stop"))
	require.Equal(t, waiting.Turn.ID, command.Control.TargetTurnID, "stop stays in its own conversation")
	seen := map[string]*core.TurnLease{}
	for range 2 {
		leased := lease(t, s, "owner")
		require.NotNil(t, leased)
		seen[leased.Turn.ID] = leased
	}
	require.Contains(t, seen, waiting.Turn.ID, "a stopped waiting turn is claimable at once")
	require.Contains(t, seen, otherTurn.Turn.ID)
	require.False(t, seen[waiting.Turn.ID].Turn.StopRequestedAt.IsZero())
	require.True(t, seen[otherTurn.Turn.ID].Turn.StopRequestedAt.IsZero())
}

func turnHeadAndFence(t *testing.T, open opener) {
	ctx := context.Background()
	s, clock := open(t, StoreConfig{})
	first := ingest(t, s, SampleMessage("one"))
	second := ingest(t, s, SampleMessage("two"))
	_, err := s.LeaseTurn(ctx, core.ClaimOptions{TTL: time.Minute, MessageDeadline: time.Hour})
	require.ErrorIs(t, err, core.ErrInvalid)
	claimed := lease(t, s, "worker-a")
	require.Equal(t, first.Turn.ID, claimed.Turn.ID)
	require.Equal(t, core.TurnRunning, claimed.Turn.Status)
	require.True(t, clock.Now().Equal(claimed.Turn.Schedule.StartedAt))
	require.True(t, clock.Now().Add(time.Hour).Equal(claimed.Turn.Schedule.DeadlineAt))
	require.Equal(t, first.Message.ID, claimed.Message.ID)
	require.Equal(t, "text one", claimed.Message.Message.Text)
	require.Equal(t, SampleBinding().AgentID, claimed.Binding.Content.AgentID)
	require.Equal(t, claimed.Turn.ConversationID, claimed.Conversation.ID)
	require.Nil(t, lease(t, s, "worker-b"), "the second turn of a conversation waits for the head")
	renewed, err := s.RenewTurn(ctx, claimed.Turn.Lease, 2*time.Minute)
	require.NoError(t, err)
	require.Equal(t, claimed.Turn.Lease.Fence, renewed.Fence)
	clock.Advance(3 * time.Minute)
	_, err = s.RenewTurn(ctx, renewed, time.Minute)
	require.ErrorIs(t, err, core.ErrLeaseLost, "an expired lease cannot be renewed")
	reclaimed := lease(t, s, "worker-b")
	require.Equal(t, first.Turn.ID, reclaimed.Turn.ID)
	require.Greater(t, reclaimed.Turn.Lease.Fence, claimed.Turn.Lease.Fence)
	require.True(t, claimed.Turn.Schedule.DeadlineAt.Equal(reclaimed.Turn.Schedule.DeadlineAt), "the deadline is fixed at the first claim")
	_, err = s.UpdateTurn(ctx, renewed, core.TurnUpdate{Phase: core.PhaseCreate})
	require.ErrorIs(t, err, core.ErrLeaseLost)
	require.ErrorIs(t, s.SettleTurn(ctx, renewed, core.TurnOutcome{Status: core.TurnDone}, nil), core.ErrLeaseLost)
	require.ErrorIs(t, s.SettleTurn(ctx, reclaimed.Turn.Lease, core.TurnOutcome{Status: core.TurnRunning}, nil), core.ErrInvalid)
	require.NoError(t, s.SettleTurn(ctx, reclaimed.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, nil))
	require.ErrorIs(t, s.SettleTurn(ctx, reclaimed.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, nil), core.ErrLeaseLost, "settle is final")
	next := lease(t, s, "worker-a")
	require.Equal(t, second.Turn.ID, next.Turn.ID, "settling advances the conversation head")
	missing, err := s.GetTurn(ctx, "missing")
	require.NoError(t, err)
	require.Nil(t, missing)
}

func turnWait(t *testing.T, open opener) {
	ctx := context.Background()
	s, clock := open(t, StoreConfig{})
	first := ingest(t, s, SampleMessage("first"))
	ingest(t, s, SampleMessage("second"))
	other := SampleMessage("other")
	other.ChatID = "another-chat"
	otherTurn := ingest(t, s, other)
	claimed, err := s.LeaseTurn(ctx, core.ClaimOptions{Owner: "owner", TTL: time.Hour, MessageDeadline: 2 * time.Hour})
	require.NoError(t, err)
	require.Equal(t, first.Turn.ID, claimed.Turn.ID)
	submitted := clock.Now()
	require.ErrorIs(t, s.WaitTurn(ctx, claimed.Turn.Lease, core.TurnWait{}), core.ErrInvalid)
	require.NoError(t, s.WaitTurn(ctx, claimed.Turn.Lease, core.TurnWait{Reason: "send_pending", Until: clock.Now().Add(time.Minute), Submit: core.Submission{SubmittedAt: submitted, WriteAttempts: 1}}))
	otherClaimed := lease(t, s, "other-owner")
	require.Equal(t, otherTurn.Turn.ID, otherClaimed.Turn.ID, "a waiting head blocks only its own conversation")
	require.Nil(t, lease(t, s, "early"), "a waiting turn is claimable only after Until")
	// 等待必须释放租约：不需等原租约（1 小时）过期，到点即可重新领取。
	clock.Advance(2 * time.Minute)
	reclaimed := lease(t, s, "new-owner")
	require.Equal(t, first.Turn.ID, reclaimed.Turn.ID)
	require.Equal(t, "send_pending", reclaimed.Turn.WaitReason)
	require.Equal(t, uint64(1), reclaimed.Turn.Submit.WriteAttempts)
	require.True(t, submitted.Equal(reclaimed.Turn.Submit.SubmittedAt))
	require.Greater(t, reclaimed.Turn.Lease.Fence, claimed.Turn.Lease.Fence)
	_, err = s.UpdateTurn(ctx, claimed.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate})
	require.ErrorIs(t, err, core.ErrLeaseLost, "a stale owner must not write after release")
}

func turnRelease(t *testing.T, open opener) {
	ctx := context.Background()
	s, _ := open(t, StoreConfig{})
	created := ingest(t, s, SampleMessage("one"))
	claimed, err := s.LeaseTurn(ctx, core.ClaimOptions{Owner: "owner", TTL: time.Hour, MessageDeadline: 2 * time.Hour})
	require.NoError(t, err)
	_, err = s.UpdateTurn(ctx, claimed.Turn.Lease, core.TurnUpdate{Phase: core.PhaseObserve, Session: core.TurnSession{Generation: 1, CreationKey: "key", SessionID: "sesn-1", Renewed: true}, Submit: core.Submission{WriteAttempts: 1}})
	require.NoError(t, err)
	require.NoError(t, s.ReleaseTurn(ctx, claimed.Turn.Lease))
	require.ErrorIs(t, s.ReleaseTurn(ctx, claimed.Turn.Lease), core.ErrLeaseLost)
	reclaimed := lease(t, s, "next")
	require.Equal(t, created.Turn.ID, reclaimed.Turn.ID, "a released turn is immediately claimable")
	require.Equal(t, core.PhaseObserve, reclaimed.Turn.Phase)
	require.Equal(t, core.TurnSession{Generation: 1, CreationKey: "key", SessionID: "sesn-1", Renewed: true}, reclaimed.Turn.Session)
	require.Equal(t, uint64(1), reclaimed.Turn.Submit.WriteAttempts)
	require.Greater(t, reclaimed.Turn.Lease.Fence, claimed.Turn.Lease.Fence)
}

func turnUpdate(t *testing.T, open opener) {
	ctx := context.Background()
	s, _ := open(t, StoreConfig{})
	created := ingest(t, s, SampleMessage("one"))
	claimed := lease(t, s, "owner")
	l := claimed.Turn.Lease
	_, err := s.UpdateTurn(ctx, l, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "k1"}})
	require.NoError(t, err)
	_, err = s.UpdateTurn(ctx, l, core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{SessionID: "sesn-1"}})
	require.NoError(t, err)
	_, err = s.UpdateTurn(ctx, l, core.TurnUpdate{Phase: core.PhaseCreate})
	require.ErrorIs(t, err, core.ErrInvalid, "phases never go back")
	_, err = s.UpdateTurn(ctx, l, core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{SessionID: "sesn-2"}})
	require.ErrorIs(t, err, core.ErrConflict)
	_, err = s.UpdateTurn(ctx, l, core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{CreationKey: "k2"}})
	require.ErrorIs(t, err, core.ErrConflict)
	blocks := []core.InputBlock{{Type: "image", FileID: "file-1"}}
	mounts := []core.MountedFile{{MessageID: "message", Key: "file-key", FileID: "file-1", SessionID: "sesn-1", ResourceID: "resource", ActualPath: "file:///mnt/session/uploads/private.png"}}
	_, err = s.UpdateTurn(ctx, l, core.TurnUpdate{Phase: core.PhaseSend, Input: core.FrozenInput{BlocksFrozen: true, Blocks: blocks, ContextFrozen: true, Context: "background", MountsFrozen: true, Mounts: mounts}})
	require.NoError(t, err)
	frozen, err := s.GetTurn(ctx, created.Turn.ID)
	require.NoError(t, err)
	require.Equal(t, mounts, frozen.Input.Mounts, "all stores restore the complete mount record")
	_, err = s.UpdateTurn(ctx, l, core.TurnUpdate{Phase: core.PhaseSend, Input: core.FrozenInput{BlocksFrozen: true, Blocks: []core.InputBlock{{Type: "image", FileID: "file-2"}}}})
	require.ErrorIs(t, err, core.ErrConflict, "frozen input is written once")
	_, err = s.UpdateTurn(ctx, l, core.TurnUpdate{Phase: core.PhaseCreate, Rebind: true, Session: core.TurnSession{Generation: 1, CreationKey: "k2"}})
	require.ErrorIs(t, err, core.ErrInvalid, "rebind must advance the generation")
	updated, err := s.UpdateTurn(ctx, l, core.TurnUpdate{Phase: core.PhaseCreate, Rebind: true, Session: core.TurnSession{Generation: 2, CreationKey: "k2", Renewed: true}})
	require.NoError(t, err)
	require.Equal(t, core.TurnSession{Generation: 2, CreationKey: "k2", Renewed: true}, updated.Session)
	got, err := s.GetTurn(ctx, created.Turn.ID)
	require.NoError(t, err)
	require.Equal(t, updated.Session, got.Session)
	require.Equal(t, blocks, got.Input.Blocks, "rebind reuses files uploaded independently of a session")
	require.Empty(t, got.Input.Context)
	require.True(t, got.Input.BlocksFrozen)
	require.False(t, got.Input.ContextFrozen)
	require.Empty(t, got.Input.Mounts)
	require.False(t, got.Input.MountsFrozen)
	require.Equal(t, created.Turn.InputEventID, got.InputEventID)
}

func turnSettle(t *testing.T, open opener) {
	ctx := context.Background()
	s, clock := open(t, StoreConfig{})
	ingest(t, s, SampleMessage("one"))
	claimed := lease(t, s, "worker")
	turn := claimed.Turn
	good := answer(turn, "answer", "answer")
	foreign := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: "turn:other"}, 1, "x", turn.BindingID, good.ReplyTarget, core.TextBody("x"))
	require.ErrorIs(t, s.SettleTurn(ctx, turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{good, foreign}), core.ErrConflict)
	duplicate := good
	require.ErrorIs(t, s.SettleTurn(ctx, turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{good, duplicate}), core.ErrConflict)
	export, err := s.Export(ctx)
	require.NoError(t, err)
	require.Equal(t, core.TurnRunning, export.Turns[0].Status, "a rejected settle rolls back entirely")
	require.Empty(t, export.Deliveries)
	require.NoError(t, s.SettleTurn(ctx, turn.Lease, core.TurnOutcome{Status: core.TurnFailed, ErrorKind: "agent_failed"}, []core.Delivery{good}))
	got, err := s.GetTurn(ctx, turn.ID)
	require.NoError(t, err)
	require.Equal(t, core.TurnFailed, got.Status)
	require.Equal(t, "agent_failed", got.Outcome.ErrorKind)
	require.True(t, clock.Now().Equal(got.Outcome.SettledAt))
	require.Empty(t, got.Lease.Owner)
	export, err = s.Export(ctx)
	require.NoError(t, err)
	require.Len(t, export.Deliveries, 1)
	require.Equal(t, core.DeliveryPending, export.Deliveries[0].Status)
	require.True(t, clock.Now().Equal(export.Deliveries[0].CreatedAt))
}

func conversationBind(t *testing.T, open opener) {
	ctx := context.Background()
	s, _ := open(t, StoreConfig{})
	ingest(t, s, SampleMessage("one"))
	claimed := lease(t, s, "owner")
	l := claimed.Turn.Lease
	binding := core.SessionBinding{State: core.SessionActive, Generation: 1, SessionID: "s1", CreationKey: "k1", LookupKey: "l1", BindingVersion: claimed.Turn.BindingVersion}
	got, err := s.BindSession(ctx, l, binding)
	require.NoError(t, err)
	require.Equal(t, binding, got.Session)
	_, err = s.BindSession(ctx, l, binding)
	require.NoError(t, err, "binding the same session is idempotent")
	other := binding
	other.SessionID = "s2"
	_, err = s.BindSession(ctx, l, other)
	require.ErrorIs(t, err, core.ErrConflict)
	foreign := binding
	foreign.Generation, foreign.BindingVersion = 2, "bv1:other"
	_, err = s.BindSession(ctx, l, foreign)
	require.ErrorIs(t, err, core.ErrInvalid, "a session is bound with the turn's frozen configuration")
	next := binding
	next.Generation, next.SessionID, next.CreationKey = 2, "s2", "k2"
	_, err = s.BindSession(ctx, l, next)
	require.NoError(t, err)
	_, err = s.BindSession(ctx, l, binding)
	require.ErrorIs(t, err, core.ErrConflict, "generations never go back")
	stored, err := s.GetConversation(ctx, claimed.Turn.ConversationID)
	require.NoError(t, err)
	require.Equal(t, next, stored.Session)
	require.NoError(t, s.SettleTurn(ctx, l, core.TurnOutcome{Status: core.TurnDone}, nil))
	_, err = s.BindSession(ctx, l, next)
	require.ErrorIs(t, err, core.ErrLeaseLost)
}

func conversationReset(t *testing.T, open opener) {
	ctx := context.Background()
	s, _ := open(t, StoreConfig{})
	none, err := s.ResetConversation(ctx, core.RouteFor(SampleMessage("one")))
	require.NoError(t, err)
	require.Nil(t, none, "nothing to reset")
	ingest(t, s, SampleMessage("one"))
	claimed := lease(t, s, "owner")
	l := claimed.Turn.Lease
	unbound, err := s.ResetConversation(ctx, claimed.Turn.ConversationID)
	require.NoError(t, err)
	require.Equal(t, core.SessionNone, unbound.Session.State, "an unbound conversation stays unbound")
	binding := core.SessionBinding{State: core.SessionActive, Generation: 1, SessionID: "s1", CreationKey: "k1", LookupKey: "l1", BindingVersion: claimed.Turn.BindingVersion}
	_, err = s.BindSession(ctx, l, binding)
	require.NoError(t, err)
	reset, err := s.ResetConversation(ctx, claimed.Turn.ConversationID)
	require.NoError(t, err)
	require.Equal(t, core.SessionBinding{State: core.SessionReserved, Generation: 2}, reset.Session)
	again, err := s.ResetConversation(ctx, claimed.Turn.ConversationID)
	require.NoError(t, err)
	require.Equal(t, reset.Session, again.Session, "reset is idempotent until the reserved generation is used")
	_, err = s.BindSession(ctx, l, binding)
	require.ErrorIs(t, err, core.ErrConflict, "an in-flight turn of the old generation cannot move the conversation back")
	next := binding
	next.Generation, next.SessionID, next.CreationKey = 2, "s2", "k2"
	_, err = s.BindSession(ctx, l, next)
	require.NoError(t, err, "the reserved generation can be filled once")
	other := next
	other.SessionID = "s3"
	_, err = s.BindSession(ctx, l, other)
	require.ErrorIs(t, err, core.ErrConflict)
}

func approval(turn core.Turn, toolUseID string, expires time.Time) core.Interaction {
	return core.Interaction{ID: core.InteractionID(turn.ID, toolUseID), TurnID: turn.ID, Kind: core.InteractionApproval, Status: core.InteractionPending, ApproverID: "user", ToolUseID: toolUseID, ToolName: "bash", ConfirmEventID: "sevt-" + toolUseID, BindingID: turn.BindingID, BindingEpoch: 0, AppID: "app", TenantKey: "tenant", ExpiresAt: expires}
}

func interactionLifecycle(t *testing.T, open opener) {
	ctx := context.Background()
	s, clock := open(t, StoreConfig{})
	ingest(t, s, SampleMessage("one"))
	claimed, err := s.LeaseTurn(ctx, core.ClaimOptions{Owner: "owner", TTL: time.Hour, MessageDeadline: 2 * time.Hour})
	require.NoError(t, err)
	turn := claimed.Turn
	expires := clock.Now().Add(time.Hour)
	first := approval(turn, "tool-1", expires)
	invalid := first
	invalid.ApproverID = ""
	_, err = s.PutInteraction(ctx, turn.Lease, invalid)
	require.ErrorIs(t, err, core.ErrInvalid)
	stored, err := s.PutInteraction(ctx, turn.Lease, first)
	require.NoError(t, err)
	require.False(t, stored.Prompted)
	prompted := first
	prompted.Prompted = true
	stored, err = s.PutInteraction(ctx, turn.Lease, prompted)
	require.NoError(t, err)
	require.True(t, stored.Prompted)
	stored, err = s.PutInteraction(ctx, turn.Lease, first)
	require.NoError(t, err)
	require.True(t, stored.Prompted, "re-registering never clears Prompted")
	changed := first
	changed.ToolName = "other"
	_, err = s.PutInteraction(ctx, turn.Lease, changed)
	require.ErrorIs(t, err, core.ErrConflict)
	_, err = s.DecideInteraction(ctx, "missing", core.InteractionDecision{Allow: true, DeciderID: "user"})
	require.ErrorIs(t, err, core.ErrNotFound)
	_, err = s.DecideInteraction(ctx, first.ID, core.InteractionDecision{Allow: true, DeciderID: "intruder"})
	require.ErrorIs(t, err, core.ErrApprovalForbidden)
	decided, err := s.DecideInteraction(ctx, first.ID, core.InteractionDecision{Allow: false, DeciderID: "user", DenyReason: "no"})
	require.NoError(t, err)
	require.Equal(t, core.InteractionDenied, decided.Status)
	require.Equal(t, "no", decided.DenyReason)
	again, err := s.DecideInteraction(ctx, first.ID, core.InteractionDecision{Allow: false, DeciderID: "user"})
	require.NoError(t, err)
	require.Equal(t, core.InteractionDenied, again.Status, "a repeated decision returns the stored result")
	_, err = s.DecideInteraction(ctx, first.ID, core.InteractionDecision{Allow: true, DeciderID: "user"})
	require.ErrorIs(t, err, core.ErrConflict, "a decision is made once")
	// 过期判定使用 Store 时钟，并且持久化。
	late := approval(turn, "tool-late", clock.Now().Add(time.Minute))
	_, err = s.PutInteraction(ctx, turn.Lease, late)
	require.NoError(t, err)
	clock.Advance(2 * time.Minute)
	_, err = s.DecideInteraction(ctx, late.ID, core.InteractionDecision{Allow: true, DeciderID: "user"})
	require.ErrorIs(t, err, core.ErrApprovalExpired)
	lateStored, err := s.GetInteraction(ctx, late.ID)
	require.NoError(t, err)
	require.Equal(t, core.InteractionExpired, lateStored.Status)
	// 结算时把剩余 pending 交互置为过期。
	pending := approval(turn, "tool-pending", expires)
	_, err = s.PutInteraction(ctx, turn.Lease, pending)
	require.NoError(t, err)
	require.NoError(t, s.SettleTurn(ctx, turn.Lease, core.TurnOutcome{Status: core.TurnDone}, nil))
	settled, err := s.GetInteraction(ctx, pending.ID)
	require.NoError(t, err)
	require.Equal(t, core.InteractionExpired, settled.Status)
	kept, err := s.GetInteraction(ctx, first.ID)
	require.NoError(t, err)
	require.Equal(t, core.InteractionDenied, kept.Status, "decided interactions keep their decision")
	_, err = s.DecideInteraction(ctx, pending.ID, core.InteractionDecision{Allow: true, DeciderID: "user"})
	require.ErrorIs(t, err, core.ErrApprovalExpired)
	_, err = s.PutInteraction(ctx, turn.Lease, approval(turn, "tool-after", expires))
	require.ErrorIs(t, err, core.ErrLeaseLost)
	missing, err := s.GetInteraction(ctx, "missing")
	require.NoError(t, err)
	require.Nil(t, missing)
}

func settleWith(t *testing.T, s core.Store, message core.Message, deliveries func(core.Turn) []core.Delivery) core.Turn {
	t.Helper()
	ingest(t, s, message)
	claimed := lease(t, s, "worker")
	require.NotNil(t, claimed)
	var out []core.Delivery
	if deliveries != nil {
		out = deliveries(claimed.Turn)
	}
	require.NoError(t, s.SettleTurn(context.Background(), claimed.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, out))
	return claimed.Turn
}

func deliveryLease(t *testing.T, open opener) {
	ctx := context.Background()
	s, clock := open(t, StoreConfig{})
	settleWith(t, s, SampleMessage("one"), func(turn core.Turn) []core.Delivery { return []core.Delivery{answer(turn, "answer", "answer")} })
	delivery, err := s.LeaseDelivery(ctx, "sender", time.Minute)
	require.NoError(t, err)
	require.Equal(t, uint64(1), delivery.Attempt)
	require.True(t, clock.Now().Equal(delivery.FirstAttemptAt))
	none, err := s.LeaseDelivery(ctx, "other", time.Minute)
	require.NoError(t, err)
	require.Nil(t, none, "a leased delivery is exclusive")
	stale := delivery.Lease
	firstAttempt := delivery.FirstAttemptAt
	clock.Advance(2 * time.Minute)
	delivery, err = s.LeaseDelivery(ctx, "new-sender", time.Minute)
	require.NoError(t, err)
	require.True(t, firstAttempt.Equal(delivery.FirstAttemptAt), "FirstAttemptAt is kept across attempts")
	require.Greater(t, delivery.Lease.Fence, stale.Fence)
	require.ErrorIs(t, s.CompleteDelivery(ctx, stale, core.DeliveryResult{Status: core.DeliveryDone}), core.ErrLeaseLost)
	require.ErrorIs(t, s.CompleteDelivery(ctx, delivery.Lease, core.DeliveryResult{Status: "bogus"}), core.ErrInvalid)
	require.NoError(t, s.CompleteDelivery(ctx, delivery.Lease, core.DeliveryResult{Status: core.DeliveryUncertain, NextAttemptAt: clock.Now().Add(time.Minute), ErrorKind: "retry"}))
	early, err := s.LeaseDelivery(ctx, "sender", time.Minute)
	require.NoError(t, err)
	require.Nil(t, early, "a delivery waits for NextAttemptAt")
	clock.Advance(2 * time.Minute)
	delivery, err = s.LeaseDelivery(ctx, "sender", time.Minute)
	require.NoError(t, err)
	require.Equal(t, core.DeliveryUncertain, delivery.Status)
	require.NoError(t, s.CompleteDelivery(ctx, delivery.Lease, core.DeliveryResult{Status: core.DeliveryDone, RemoteMessageID: "om_1"}))
	require.ErrorIs(t, s.CompleteDelivery(ctx, delivery.Lease, core.DeliveryResult{Status: core.DeliveryDone}), core.ErrLeaseLost, "a final delivery is closed")
	remaining, err := s.LeaseDelivery(ctx, "sender", time.Minute)
	require.NoError(t, err)
	require.Nil(t, remaining)
	export, err := s.Export(ctx)
	require.NoError(t, err)
	require.Equal(t, core.DeliveryDone, export.Deliveries[0].Status)
	require.Equal(t, "om_1", export.Deliveries[0].RemoteMessageID)
}

func deliverySerial(t *testing.T, open opener) {
	ctx := context.Background()
	s, clock := open(t, StoreConfig{})
	first := settleWith(t, s, SampleMessage("one"), func(turn core.Turn) []core.Delivery {
		target := core.ReplyTarget{Mode: "reply", ChatID: "chat"}
		source := core.DeliverySource{Kind: core.SourceTurn, ID: turn.ID}
		return []core.Delivery{
			core.NewDelivery(source, 2, "artifact:f2", turn.BindingID, target, core.ArtifactBody(core.Artifact{Kind: "document", FileID: "f2", FileName: "b.pdf"})),
			answer(turn, "answer", "answer"),
			core.NewDelivery(source, 1, "artifact:f1", turn.BindingID, target, core.ArtifactBody(core.Artifact{Kind: "image", FileID: "f1"})),
		}
	})
	other := SampleMessage("other")
	other.ChatID = "other-chat"
	second := settleWith(t, s, other, func(turn core.Turn) []core.Delivery { return []core.Delivery{answer(turn, "answer", "other")} })
	// 两个来源的队首可以并行领取；同一来源内只有队首可领取。
	a, err := s.LeaseDelivery(ctx, "s1", time.Minute)
	require.NoError(t, err)
	b, err := s.LeaseDelivery(ctx, "s2", time.Minute)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{first.ID, second.ID}, []string{a.Source.ID, b.Source.ID})
	heads := map[string]*core.Delivery{a.Source.ID: a, b.Source.ID: b}
	require.Equal(t, 0, heads[first.ID].Seq, "the text answer goes first")
	none, err := s.LeaseDelivery(ctx, "s3", time.Minute)
	require.NoError(t, err)
	require.Nil(t, none, "later deliveries of a source wait for its head")
	// 非终态结果不推进队首：重试仍然是同一条。
	require.NoError(t, s.CompleteDelivery(ctx, heads[first.ID].Lease, core.DeliveryResult{Status: core.DeliveryPending, NextAttemptAt: clock.Now().Add(time.Second)}))
	clock.Advance(2 * time.Second)
	retry, err := s.LeaseDelivery(ctx, "s1", time.Minute)
	require.NoError(t, err)
	require.Equal(t, heads[first.ID].ID, retry.ID)
	// 失败同样推进队首：一条投递失败不阻塞后续产物。
	require.NoError(t, s.CompleteDelivery(ctx, retry.Lease, core.DeliveryResult{Status: core.DeliveryFailed, ErrorKind: "x"}))
	art1, err := s.LeaseDelivery(ctx, "s1", time.Minute)
	require.NoError(t, err)
	require.Equal(t, 1, art1.Seq)
	require.Equal(t, core.BodyArtifact, art1.Body.Type)
	require.Equal(t, "f1", art1.Body.Artifact.FileID)
	require.NoError(t, s.CompleteDelivery(ctx, art1.Lease, core.DeliveryResult{Status: core.DeliveryDone, RemoteMessageID: "om"}))
	art2, err := s.LeaseDelivery(ctx, "s1", time.Minute)
	require.NoError(t, err)
	require.Equal(t, 2, art2.Seq)
	require.Equal(t, "b.pdf", art2.Body.Artifact.FileName)
}

func deliveryRetry(t *testing.T, open opener) {
	ctx := context.Background()
	s, clock := open(t, StoreConfig{})
	_, err := s.RetryDelivery(ctx, "missing")
	require.ErrorIs(t, err, core.ErrNotFound)
	turn := settleWith(t, s, SampleMessage("one"), func(turn core.Turn) []core.Delivery { return []core.Delivery{answer(turn, "answer", "answer")} })
	delivery, err := s.LeaseDelivery(ctx, "sender", time.Minute)
	require.NoError(t, err)
	_, err = s.RetryDelivery(ctx, delivery.ID)
	require.ErrorIs(t, err, core.ErrConflict, "only failed deliveries can be retried")
	require.NoError(t, s.CompleteDelivery(ctx, delivery.Lease, core.DeliveryResult{Status: core.DeliveryFailed, ErrorKind: "delivery_unconfirmed"}))
	clock.Advance(time.Minute)
	retried, err := s.RetryDelivery(ctx, delivery.ID)
	require.NoError(t, err)
	require.Equal(t, core.DeliveryPending, retried.Status)
	require.Zero(t, retried.Attempt)
	require.Empty(t, retried.ErrorKind)
	again, err := s.LeaseDelivery(ctx, "sender", time.Minute)
	require.NoError(t, err)
	require.Equal(t, delivery.ID, again.ID)
	require.Equal(t, uint64(1), again.Attempt)
	require.True(t, clock.Now().Equal(again.FirstAttemptAt), "a manual retry restarts the dedupe window")
	require.Equal(t, delivery.UUID, again.UUID, "the provider idempotency key never changes")
	got, err := s.GetTurn(ctx, turn.ID)
	require.NoError(t, err)
	require.Equal(t, core.TurnDone, got.Status, "retrying a delivery never reopens the turn")
}

func adminStatsAndPaging(t *testing.T, open opener) {
	ctx := context.Background()
	s, clock := open(t, StoreConfig{})
	empty, err := s.Stats(ctx)
	require.NoError(t, err)
	require.Equal(t, core.QueueStats{}, empty)
	created := clock.Now()
	ingest(t, s, SampleMessage("one"))
	clock.Advance(time.Second)
	ingest(t, s, SampleMessage("two"))
	other := SampleMessage("three")
	other.ChatID = "other"
	ingest(t, s, other)
	claimed := lease(t, s, "w")
	require.NoError(t, s.WaitTurn(ctx, claimed.Turn.Lease, core.TurnWait{Reason: "x", Until: clock.Now().Add(time.Hour)}))
	running := lease(t, s, "w")
	require.NotNil(t, running)
	stats, err := s.Stats(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.QueuedTurns)
	require.Equal(t, 1, stats.RunningTurns)
	require.Equal(t, 1, stats.WaitingTurns)
	require.True(t, created.Equal(stats.OldestOpenTurnAt))
	require.NoError(t, s.SettleTurn(ctx, running.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{answer(running.Turn, "answer", "a")}))
	stats, err = s.Stats(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.OpenDeliveries)
	require.Zero(t, stats.FailedDeliveries)
	page1, err := s.ListTurns(ctx, core.TurnQuery{Limit: 2})
	require.NoError(t, err)
	require.Len(t, page1, 2)
	page2, err := s.ListTurns(ctx, core.TurnQuery{Limit: 2, After: page1[1].ID})
	require.NoError(t, err)
	require.Len(t, page2, 1)
	require.Less(t, page1[1].ID, page2[0].ID, "pages are ordered and disjoint")
	open1, err := s.ListTurns(ctx, core.TurnQuery{OpenOnly: true})
	require.NoError(t, err)
	require.Len(t, open1, 2)
	byConversation, err := s.ListTurns(ctx, core.TurnQuery{ConversationID: core.RouteFor(other)})
	require.NoError(t, err)
	require.Len(t, byConversation, 1)
	_, err = s.ListTurns(ctx, core.TurnQuery{Limit: core.MaxPageSize + 1})
	require.ErrorIs(t, err, core.ErrInvalid)
	deliveries, err := s.ListDeliveries(ctx, core.DeliveryQuery{OpenOnly: true})
	require.NoError(t, err)
	require.Len(t, deliveries, 1)
	failed, err := s.ListDeliveries(ctx, core.DeliveryQuery{Status: core.DeliveryFailed})
	require.NoError(t, err)
	require.Empty(t, failed)
	conversations, err := s.ListConversations(ctx, core.ConversationQuery{Limit: 1})
	require.NoError(t, err)
	require.Len(t, conversations, 1)
	rest, err := s.ListConversations(ctx, core.ConversationQuery{After: string(conversations[0].ID)})
	require.NoError(t, err)
	require.Len(t, rest, 1)
}

func adminPrune(t *testing.T, open opener) {
	ctx := context.Background()
	s, clock := open(t, StoreConfig{})
	old := settleWith(t, s, SampleMessage("old"), func(turn core.Turn) []core.Delivery { return []core.Delivery{answer(turn, "answer", "a")} })
	delivery, err := s.LeaseDelivery(ctx, "sender", time.Minute)
	require.NoError(t, err)
	require.NoError(t, s.CompleteDelivery(ctx, delivery.Lease, core.DeliveryResult{Status: core.DeliveryDone, RemoteMessageID: "om"}))
	outbox := settleWith(t, s, SampleMessage("outbox"), func(turn core.Turn) []core.Delivery { return []core.Delivery{answer(turn, "answer", "b")} })
	idleStop := ingest(t, s, StopMessage("idle-stop"))
	reply, err := s.LeaseDelivery(ctx, "sender", time.Minute)
	require.NoError(t, err)
	if reply.ID != idleStop.Reply.ID {
		// 另一个来源暂缓后再领取控制回复，不依赖两个 eligible=0 记录的 opaque ID 排序。
		require.NoError(t, s.CompleteDelivery(ctx, reply.Lease, core.DeliveryResult{Status: core.DeliveryPending, NextAttemptAt: clock.Now().Add(time.Minute)}))
		reply, err = s.LeaseDelivery(ctx, "sender", time.Minute)
		require.NoError(t, err)
	}
	require.Equal(t, idleStop.Reply.ID, reply.ID)
	require.NoError(t, s.CompleteDelivery(ctx, reply.Lease, core.DeliveryResult{Status: core.DeliveryDone, RemoteMessageID: "om2"}))
	clock.Advance(time.Hour)
	recent := settleWith(t, s, SampleMessage("recent"), nil)
	removed, err := s.Prune(ctx, clock.Now().Add(-30*time.Minute))
	require.NoError(t, err)
	require.Equal(t, 1, removed.Turns, "only old turns whose deliveries are final are removed")
	require.Equal(t, 1, removed.Controls)
	require.Equal(t, 2, removed.Messages)
	require.Equal(t, 2, removed.Deliveries)
	gone, err := s.GetTurn(ctx, old.ID)
	require.NoError(t, err)
	require.Nil(t, gone)
	kept, err := s.GetTurn(ctx, outbox.ID)
	require.NoError(t, err)
	require.NotNil(t, kept, "a turn with an open delivery is kept")
	kept, err = s.GetTurn(ctx, recent.ID)
	require.NoError(t, err)
	require.NotNil(t, kept)
	versions, err := s.ListBindingVersions(ctx)
	require.NoError(t, err)
	require.Len(t, versions, 1, "a referenced version is kept")
	// 去重索引一起删除：同一消息再次到达会被当作新输入，这正是保留期必须长于渠道重投窗口的原因。
	again := ingest(t, s, SampleMessage("old"))
	require.False(t, again.Duplicate)
	require.NotEqual(t, old.ID, again.Turn.ID)
	none, err := s.Prune(ctx, clock.Now().Add(-30*time.Minute))
	require.NoError(t, err)
	require.Equal(t, core.PruneResult{}, none)
}

func exportIsolated(t *testing.T, open opener) {
	ctx := context.Background()
	s, _ := open(t, StoreConfig{})
	ingest(t, s, SampleMessage("one"))
	export, err := s.Export(ctx)
	require.NoError(t, err)
	export.BindingVersions[0].Content.VaultIDs[0] = "mutated"
	fresh, err := s.Export(ctx)
	require.NoError(t, err)
	require.Equal(t, "vault", fresh.BindingVersions[0].Content.VaultIDs[0], "exports must not alias store memory")
}

func canceled(t *testing.T, open opener) {
	s, _ := open(t, StoreConfig{})
	ingest(t, s, SampleMessage("one"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.Ingest(ctx, SampleVersion(), SampleMessage("two"))
	require.True(t, errors.Is(err, context.Canceled))
	_, err = s.LeaseTurn(ctx, Claim("owner"))
	require.ErrorIs(t, err, context.Canceled)
	_, err = s.GetTurn(ctx, "turn")
	require.ErrorIs(t, err, context.Canceled)
	_, err = s.GetConversation(ctx, "route")
	require.ErrorIs(t, err, context.Canceled)
	_, err = s.Stats(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func deliveryRenewal(t *testing.T, open opener) {
	s, clock := open(t, StoreConfig{})
	ctx := context.Background()
	ingest(t, s, SampleMessage("renew"))
	turn := lease(t, s, "turn")
	require.NoError(t, s.SettleTurn(ctx, turn.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{answer(turn.Turn, "answer", "ok")}))
	first, err := s.LeaseDelivery(ctx, "one", time.Second)
	require.NoError(t, err)
	require.NotNil(t, first)
	renewer, ok := s.(interface {
		RenewDelivery(context.Context, core.Lease, time.Duration) (core.Lease, error)
	})
	require.True(t, ok, "delivery lease must be verifiable before remote effects")
	_, err = renewer.RenewDelivery(ctx, first.Lease, 0)
	require.ErrorIs(t, err, core.ErrInvalid)
	clock.Advance(500 * time.Millisecond)
	renewed, err := renewer.RenewDelivery(ctx, first.Lease, time.Second)
	require.NoError(t, err)
	require.Equal(t, first.Lease.Fence, renewed.Fence)
	clock.Advance(600 * time.Millisecond)
	none, err := s.LeaseDelivery(ctx, "two", time.Second)
	require.NoError(t, err)
	require.Nil(t, none)
	clock.Advance(500 * time.Millisecond)
	second, err := s.LeaseDelivery(ctx, "two", time.Second)
	require.NoError(t, err)
	require.NotNil(t, second)
	_, err = renewer.RenewDelivery(ctx, first.Lease, time.Second)
	require.ErrorIs(t, err, core.ErrLeaseLost)
	require.NoError(t, s.CompleteDelivery(ctx, second.Lease, core.DeliveryResult{Status: core.DeliveryDone, RemoteMessageID: "done"}))
	_, err = renewer.RenewDelivery(ctx, second.Lease, time.Second)
	require.ErrorIs(t, err, core.ErrLeaseLost)
}

func queueTailFullQuota(t *testing.T, open opener) {
	s, clock := open(t, StoreConfig{QueueTTL: time.Minute})
	ctx := context.Background()
	binding := SampleBinding()
	binding.Owner = "account"
	version := core.NewBindingVersion(binding)
	_, err := s.Ingest(ctx, version, SampleMessage("head"))
	require.NoError(t, err)
	claim := Claim("worker")
	claim.AccountLimit = 1
	claim.TTL = time.Hour
	head, err := s.LeaseTurn(ctx, claim)
	require.NoError(t, err)
	require.NotNil(t, head)
	_, err = s.UpdateTurn(ctx, head.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "head"}, Submit: core.Submission{WriteAttempts: 1, SubmittedAt: clock.Now()}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteCreate, Limit: 1}})
	require.NoError(t, err)
	tail, err := s.Ingest(ctx, version, SampleMessage("tail"))
	require.NoError(t, err)
	clock.Advance(2 * time.Minute)
	expired, err := s.LeaseTurn(ctx, claim)
	require.NoError(t, err)
	require.NotNil(t, expired)
	require.Equal(t, tail.Turn.ID, expired.Turn.ID)
	require.True(t, expired.Turn.Schedule.StartedAt.IsZero())
	require.True(t, expired.Turn.Schedule.DeadlineAt.IsZero())
	fresh := SampleMessage("other")
	fresh.ChatID = "other"
	other, err := s.Ingest(ctx, version, fresh)
	require.NoError(t, err)
	available, err := s.LeaseTurn(ctx, claim)
	require.NoError(t, err)
	require.NotNil(t, available)
	require.Equal(t, other.Turn.ID, available.Turn.ID)
	intent := core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "other"}, Submit: core.Submission{WriteAttempts: 1, SubmittedAt: clock.Now()}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteCreate, Limit: 1}}
	_, err = s.UpdateTurn(ctx, available.Turn.Lease, intent)
	require.ErrorIs(t, err, core.ErrRemoteLimit)
	require.NoError(t, s.WaitTurn(ctx, head.Turn.Lease, core.TurnWait{Reason: "wait", Until: clock.Now().Add(time.Hour), Submit: core.Submission{WriteAttempts: 1, SubmittedAt: clock.Now()}}))
	_, err = s.UpdateTurn(ctx, available.Turn.Lease, intent)
	require.ErrorIs(t, err, core.ErrRemoteLimit, "unknown creation retains budget after waiting")
	require.NoError(t, s.SettleTurn(ctx, expired.Turn.Lease, core.TurnOutcome{Status: core.TurnFailed, ErrorKind: "queue_deadline"}, nil))
}
