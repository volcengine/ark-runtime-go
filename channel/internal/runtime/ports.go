// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"time"
)

// Store 契约错误：所有实现必须用 errors.Is 可识别的方式返回这些哨兵，Runtime 与
// conformance 套件只依赖它们，不依赖具体实现的错误类型。
var (
	// ErrLeaseLost 表示租约过期、所有者不符、fence 已失效或实体已结算。
	ErrLeaseLost = errors.New("lease lost")
	// ErrConflict 表示重复键对应不同内容，或只写一次的字段被重新赋值。
	ErrConflict = errors.New("conflicting record")
	// ErrInvalid 表示调用参数不满足 Store 契约。
	ErrInvalid = errors.New("invalid store input")
	// ErrQueueFull 表示未结算 Turn 已达到容量上限。
	ErrQueueFull = errors.New("turn queue full")
	// ErrNotFound 表示按 ID 操作的实体不存在。
	ErrNotFound = errors.New("record not found")
	// ErrApprovalExpired 表示审批对应的轮次已结束、调用已处理或已超时。
	ErrApprovalExpired = errors.New("approval expired")
	// ErrApprovalForbidden 表示做决定的用户不是该轮的审批人。
	ErrApprovalForbidden = errors.New("approval not permitted for this user")
)

// IngestResult 是一次入站的结果。user 消息对应 Turn；control 消息对应 Control，当时没有进行中
// 的轮次时 Reply 是同一事务写入的回复投递，命中轮次时 Stopped 是登记了停止请求后的目标 Turn。
type IngestResult struct {
	// Ignored 表示撤回来源不在已受理消息中，不创建任务或控制命令。
	Ignored bool
	// Rejected 表示消息与容量拒绝诊断已在同一事务持久化，宿主可安全 ACK。
	Rejected  bool
	Message   InboundMessage
	Duplicate bool
	Turn      *Turn
	Control   *ControlRequest
	Stopped   *Turn
	Reply     *Delivery
}

// IngestStore 持久化入站消息。
type IngestStore interface {
	// Ingest 在一个事务里完成：按 (binding, provider_message_id) 与 (binding, provider_event_id)
	// 去重；写入 BindingVersion（不存在时）；创建 Conversation（不存在时）；写入 InboundMessage；
	// user 消息生成 Turn（受容量限制），control 消息生成 ControlRequest 并同步生效。
	// 容量不足且拒绝预算启用时，同事务持久化拒绝去重事实和诊断来源，返回 Rejected=true、Turn=nil；
	// 拒绝预算耗尽返回 ErrQueueFull，所有写入回滚。
	//
	// 重复消息返回 Duplicate=true 与原有记录；同一键对应不同内容返回 ErrConflict。
	Ingest(context.Context, BindingVersion, Message) (IngestResult, error)
}

// TurnLease 是一次 Turn 领取的结果，携带执行所需的全部只读上下文。
type TurnLease struct {
	Turn         Turn
	Message      InboundMessage
	Binding      BindingVersion
	Conversation Conversation
}

// ClaimOptions 指定领取者与时限。MessageDeadline 为兼容命名，用于初始化无进展截止时间。
type ClaimOptions struct {
	// Stage 按持久阶段选择任务，空值兼容旧工作池。
	Stage TurnStage
	// UserAuth 显式允许领取本人授权协议任务，旧调用方默认关闭。
	UserAuth bool
	// AccountLimit 为旧调用方保留；领取仅控制观察，远端预算由 TurnUpdate.RemoteIntent 执行。
	AccountLimit    int
	Owner           string
	TTL             time.Duration
	MessageDeadline time.Duration
}

// Valid 校验领取参数。
func (o ClaimOptions) Valid() bool {
	return o.Stage.Valid() && o.AccountLimit >= 0 && o.Owner != "" && o.TTL > 0 && o.MessageDeadline > 0
}

// TurnStore 是单轮执行的状态机端口；所有写入都要求持有当前租约（StopRequest 由入站写入除外）。
type TurnStore interface {
	// LeaseTurn 领取一个可执行的队首 Turn，或已过排队时限且尚未执行的队尾 Turn。
	// 到期队尾只能由 Runtime 诊断结算，不能执行；普通领取必须到达可领取时间
	// （NextAttemptAt 与原租约到期中的较晚者；登记了停止且无有效租约时立即可领取）。
	// 按 (可领取时间, seq) 选择；没有可领取的 Turn 返回 nil。
	LeaseTurn(context.Context, ClaimOptions) (*TurnLease, error)
	// RenewTurn 使用当前 fence 延长租约。
	RenewTurn(context.Context, Lease, time.Duration) (Lease, error)
	// ReleaseTurn 不改变进度地交还租约，Turn 立即可被重新领取。
	ReleaseTurn(context.Context, Lease) error
	// UpdateTurn 按 Turn.Apply 的迁移规则推进阶段、写入 Session、提交记录或冻结输入。
	UpdateTurn(context.Context, Lease, TurnUpdate) (Turn, error)
	// WaitTurn 释放租约，到 Until 后重新可领取。
	WaitTurn(context.Context, Lease, TurnWait) error
	// SettleTurn 把 Turn 迁移到终态，并在同一事务内写入全部投递、把该 Turn 的 pending 交互
	// 置为 expired、把会话队首推进到下一个未结算 Turn。
	SettleTurn(context.Context, Lease, TurnOutcome, []Delivery) error
	// GetTurn 读取 Turn 的最新状态，不存在时返回 nil。
	GetTurn(context.Context, string) (*Turn, error)
}

// ConversationStore 管理会话与远程 Session 的绑定。
type ConversationStore interface {
	// GetConversation 读取会话，不存在时返回 nil。
	GetConversation(context.Context, RouteKey) (*Conversation, error)
	// BindSession 由持有 Turn 租约的 Worker 把该 Turn 所在会话绑定到已确认的 Session。
	// 迁移规则见 SessionBinding.CheckBind；BindingVersion 必须等于 Turn 冻结的版本。
	BindSession(context.Context, Lease, SessionBinding) (Conversation, error)
	// ResetConversation 让会话的下一条消息新建 Session：已绑定时预留下一代。会话不存在返回 nil。
	ResetConversation(context.Context, RouteKey) (*Conversation, error)
}

// InteractionStore 持久化待用户处理的交互。
type InteractionStore interface {
	// PutInteraction 由持有 Turn 租约的 Worker 登记交互（幂等）：不存在时插入；已存在时只允许把
	// Prompted 从 false 改为 true，其余字段不同返回 ErrConflict。返回存储中的最新记录。
	PutInteraction(context.Context, Lease, Interaction) (Interaction, error)
	// DecideInteraction 以 CAS 方式决定交互（见 Interaction.Decide），使用 Store 时钟判断过期。
	DecideInteraction(context.Context, string, InteractionDecision) (Interaction, error)
	// GetInteraction 读取交互，不存在时返回 nil。
	GetInteraction(context.Context, string) (*Interaction, error)
}

// DeliveryStore 是 Outbox 端口。
type DeliveryStore interface {
	// LeaseDelivery 只领取每个来源中 Seq 最小的未终态投递，且到达可领取时间。
	LeaseDelivery(context.Context, string, time.Duration) (*Delivery, error)
	// RenewDelivery 在外部投递前核验当前 fence 并延长租约。
	RenewDelivery(context.Context, Lease, time.Duration) (Lease, error)
	// CompleteDelivery 保存一次投递结果；终态时来源内的下一条投递成为队首。
	CompleteDelivery(context.Context, Lease, DeliveryResult) error
	// RetryDelivery 把 failed 投递恢复为 pending 并重置尝试预算；不会重新执行 Agent。
	// 不存在返回 ErrNotFound，非 failed 返回 ErrConflict。
	RetryDelivery(context.Context, string) (*Delivery, error)
}

// ProgressDeliveryAttemptStore 在调用不具备幂等保证的原生过程闭合接口前，原子登记发送意图。
// 实现须验证活租约；同一记录已登记时返回 ErrConflict，普通投递返回 ErrInvalid。
type ProgressDeliveryAttemptStore interface {
	MarkProgressDeliveryAttempt(context.Context, Lease) error
}

// QueueStats 是队列概况，用于指标与诊断。
type QueueStats struct {
	QueuedTurns  int `json:"queued_turns"`
	RunningTurns int `json:"running_turns"`
	WaitingTurns int `json:"waiting_turns"`
	// OldestOpenTurnAt 是最早未结算 Turn 的入队时间，没有时为零。
	OldestOpenTurnAt time.Time `json:"oldest_open_turn_at"`
	OpenDeliveries   int       `json:"open_deliveries"`
	FailedDeliveries int       `json:"failed_deliveries"`
}

// TurnQuery 是分页列出 Turn 的条件；结果按 ID（即入队顺序）升序，After 为上一页最后一个 ID。
type TurnQuery struct {
	ConversationID RouteKey
	OpenOnly       bool
	After          string
	Limit          int
}

// DeliveryQuery 是分页列出投递的条件；结果按 ID 升序。
type DeliveryQuery struct {
	Status   DeliveryStatus
	OpenOnly bool
	After    string
	Limit    int
}

// ConversationQuery 是分页列出会话的条件；结果按 ID 升序。
type ConversationQuery struct {
	After string
	Limit int
}

// MaxPageSize 是列表接口单页上限；Limit 为 0 时使用 100。
const MaxPageSize = 500

// PageLimit 规范化分页大小。
func PageLimit(limit int) (int, error) {
	switch {
	case limit == 0:
		return 100, nil
	case limit < 0 || limit > MaxPageSize:
		return 0, ErrInvalid
	}
	return limit, nil
}

// PruneResult 是一次清理删除的记录数。
type PruneResult struct {
	Turns           int `json:"turns"`
	Controls        int `json:"controls"`
	Messages        int `json:"messages"`
	Deliveries      int `json:"deliveries"`
	Interactions    int `json:"interactions"`
	BindingVersions int `json:"binding_versions"`
}

// AdminStore 是运维端口。
type AdminStore interface {
	Stats(context.Context) (QueueStats, error)
	ListTurns(context.Context, TurnQuery) ([]Turn, error)
	ListDeliveries(context.Context, DeliveryQuery) ([]Delivery, error)
	ListConversations(context.Context, ConversationQuery) ([]Conversation, error)
	// ListBindingVersions 返回全部仍被保留的配置版本，供启动时校验连接与持久数据一致。
	ListBindingVersions(context.Context) ([]BindingVersion, error)
	// Prune 删除 before 之前结算、且投递全部终态的 Turn 及其消息、交互与投递；删除 before 之前
	// 创建且投递全部终态的控制命令及其消息；删除不再被引用的配置版本。去重索引随消息一起删除，
	// 调用方须保证 before 早于渠道重投窗口。
	Prune(context.Context, time.Time) (PruneResult, error)
}

// Store 是全部持久化端口的组合，由同一个实现提供。
type Store interface {
	RemoteOperationStore
	IngestStore
	TurnStore
	ConversationStore
	InteractionStore
	DeliveryStore
	AdminStore
	SnapshotStore
	SessionHistoryStore
	BotVaultStore
	Close() error
}

// Export 是 Store 全部内容的独立副本，只用于测试。
type Export struct {
	BindingVersions []BindingVersion
	Conversations   []Conversation
	Messages        []InboundMessage
	Turns           []Turn
	Controls        []ControlRequest
	Interactions    []Interaction
	Deliveries      []Delivery
}

// Exporter 是测试辅助接口：Runtime 与 runner 不依赖它。
type Exporter interface {
	Export(context.Context) (Export, error)
}
