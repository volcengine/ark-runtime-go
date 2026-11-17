// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// stableKey 把若干字段拼接后取 sha256，作为确定性派生键。
func stableKey(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(h[:])
}

func digestJSON(value any) string {
	data, _ := json.Marshal(value)
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// ---------------------------------------------------------------------------
// BindingVersion

// BindingVersion 是一份按内容寻址的 Binding 配置。Turn 与 Conversation 只引用 Version，
// 冻结配置的一致性校验因此只比较版本号字符串。
type BindingVersion struct {
	StoredRecord  `json:"-"`
	frozenContent *frozenStoredJSON
	BindingID     string    `json:"binding_id"`
	Version       string    `json:"version"`
	Content       Binding   `json:"content"`
	CreatedAt     time.Time `json:"created_at"`
}

// NewBindingVersion 计算 Binding 的规范化内容哈希；相同内容总是得到相同版本号。
func NewBindingVersion(binding Binding) BindingVersion {
	content := binding.clone()
	return BindingVersion{BindingID: content.ID, Version: "bv1:" + digestJSON(content), Content: content}
}

// Valid 校验版本号与内容一致，防止存储层收到被篡改或不完整的版本。
func (v BindingVersion) Valid() bool {
	if v.IsOpaqueStored() {
		return false
	}
	validHash := v.Version == NewBindingVersion(v.Content).Version
	if v.frozenContent != nil {
		baseline, _ := json.Marshal(v.Content)
		validHash = v.Version == "bv1:"+v.frozenContent.digest && digestBytes(v.frozenContent.raw) == v.frozenContent.digest && bytes.Equal(baseline, v.frozenContent.baseline)
	}
	return v.BindingID != "" && v.BindingID == v.Content.ID && validHash
}

// ---------------------------------------------------------------------------
// Conversation

// ConversationScope 是会话所在的渠道范围，仅供运维阅读；路由键已包含这些字段。
type ConversationScope struct {
	TenantKey string `json:"tenant_key"`
	ChatType  string `json:"chat_type"`
	ChatID    string `json:"chat_id"`
	ThreadID  string `json:"thread_id,omitempty"`
}

// SessionState 是会话当前指向的远程 Session 状态。
type SessionState string

const (
	// SessionNone 表示从未绑定过 Session。
	SessionNone SessionState = "none"
	// SessionReserved 表示管理员重置后预留了下一代，尚未创建 Session。
	SessionReserved SessionState = "reserved"
	// SessionActive 表示已绑定一个已确认的 Session。
	SessionActive SessionState = "active"
)

// SessionBinding 是会话与远程 Session 的绑定关系；Generation 只增。
type SessionBinding struct {
	SnapshotID     string       `json:"snapshot_id,omitempty"`
	State          SessionState `json:"state"`
	Generation     uint64       `json:"generation"`
	SessionID      string       `json:"session_id,omitempty"`
	CreationKey    string       `json:"creation_key,omitempty"`
	LookupKey      string       `json:"lookup_key,omitempty"`
	BindingVersion string       `json:"binding_version,omitempty"`
}

// Conversation 是一条串行执行的会话（聚合根），ID 即 RouteKey。
type Conversation struct {
	StoredRecord `json:"-"`
	// GenerationHighWater 是已冻结创建请求的最大代数，与当前成功绑定指针分离。
	GenerationHighWater uint64            `json:"generation_high_water,omitempty"`
	ID                  RouteKey          `json:"id"`
	BindingID           string            `json:"binding_id"`
	Scope               ConversationScope `json:"scope"`
	Session             SessionBinding    `json:"session"`
	UpdatedAt           time.Time         `json:"updated_at"`
	// Revision 每次写入加一，供运维判断记录是否变化。
	Revision uint64 `json:"revision"`
}

// CheckBind 校验把会话绑定到 next 是否合法：只能绑定到已确认的 Session；代数不能回退；
// 同代只允许填充预留代或幂等重写。
func (s SessionBinding) CheckBind(next SessionBinding) error {
	if next.State != SessionActive || next.SessionID == "" || next.Generation == 0 || next.CreationKey == "" || next.LookupKey == "" || next.BindingVersion == "" {
		return ErrInvalid
	}
	switch {
	case next.Generation > s.Generation:
		return nil
	case next.Generation < s.Generation:
		return ErrConflict
	case s.State == SessionReserved:
		return nil
	case s.State == SessionActive && s == next:
		return nil
	}
	return ErrConflict
}

// Reset 返回管理员重置后的绑定：已绑定 Session 时预留下一代，其他状态保持不变。
func (s SessionBinding) Reset() SessionBinding {
	if s.State != SessionActive {
		return s
	}
	return SessionBinding{State: SessionReserved, Generation: s.Generation + 1}
}

// ---------------------------------------------------------------------------
// InboundMessage

// MessageKind 是入站消息的分类，在入站时确定。
type MessageKind string

const (
	// MessageUser 是交给 Agent 处理的普通消息，生成 Turn。
	MessageUser MessageKind = "user"
	// MessageControl 是停止命令或未支持消息的诊断，生成 ControlRequest。
	MessageControl MessageKind = "control"
)

// ClassifyMessage 将停止、撤回和未支持消息分类为不进入执行队列的控制请求。
func ClassifyMessage(message Message) MessageKind {
	if message.RecalledMessageID != "" || message.Unsupported {
		return MessageControl
	}
	stopScope := message.ChatType == "p2p" || message.ChatType == "group" && message.Mentioned
	if stopScope && strings.TrimSpace(message.Text) == "/stop" {
		return MessageControl
	}
	return MessageUser
}

// ContentHash 返回消息规范化内容的哈希，同一条消息重推保持不变。
// 接收时间、渠道事件 ID 与可选 Metadata 不参与身份；Metadata 以首次受理事实为准，
// 不能使滚动升级期间不认识新字段的旧 worker 与新 worker 计算出不同输入 ID。
func ContentHash(message Message) string {
	canonical := message
	canonical.Metadata = nil
	canonical.TrackReactions = false
	canonical.ReceivedAt = time.Time{}
	canonical.AdmitUntil = time.Time{}
	canonical.ProviderEventID = ""
	if len(canonical.Attachments) == 0 {
		canonical.Attachments = nil
	}
	return digestJSON(canonical)
}

// InboundMessage 是持久化的入站事实，写入后不可变。
type InboundMessage struct {
	StoredRecord `json:"-"`
	// BindingVersion 冻结消息受理时的精确配置来源，仅供持久归属与迁移核验。
	BindingVersion    string          `json:"binding_version,omitempty"`
	Payloads          *StoredPayloads `json:"_payloads,omitempty"`
	ID                string          `json:"id"`
	Seq               uint64          `json:"seq"`
	BindingID         string          `json:"binding_id"`
	ConversationID    RouteKey        `json:"conversation_id"`
	ProviderEventID   string          `json:"provider_event_id"`
	ProviderMessageID string          `json:"provider_message_id"`
	Kind              MessageKind     `json:"kind"`
	ContentHash       string          `json:"content_hash"`
	Message           Message         `json:"message"`
	ReceivedAt        time.Time       `json:"received_at"`
}

// NewInboundMessage 由适配器 DTO 构造入站事实；ID 与 Seq 由存储层分配。
func NewInboundMessage(message Message, now time.Time) InboundMessage {
	received := message.ReceivedAt
	if received.IsZero() {
		received = now
	}
	if message.Metadata != nil {
		metadata := *message.Metadata
		metadata.Mentions = append([]Mention(nil), metadata.Mentions...)
		message.Metadata = &metadata
	}
	return InboundMessage{BindingID: message.BindingID, ConversationID: RouteFor(message), ProviderEventID: message.ProviderEventID, ProviderMessageID: message.MessageID, Kind: ClassifyMessage(message), ContentHash: ContentHash(message), Message: message, ReceivedAt: received.UTC()}
}

// ValidateMessage 检查适配器产出的消息是否满足入站契约。
func ValidateMessage(binding Binding, message Message, maxTextBytes int) error {
	if binding.ID == "" || message.BindingID != binding.ID || message.ProviderEventID == "" || message.MessageID == "" || message.TenantKey == "" || message.ChatID == "" || message.ChatType == "" || len(message.Text) > maxTextBytes {
		return ErrInvalid
	}
	if message.Unsupported && (message.Text != "" || len(message.Attachments) != 0 || message.RecalledMessageID != "") {
		return ErrInvalid
	}
	return nil
}

// ---------------------------------------------------------------------------
// Turn

// TurnStatus 是单轮执行的状态。
type TurnStatus string

const (
	// TurnQueued 表示已入队、从未被领取。
	TurnQueued TurnStatus = "queued"
	// TurnRunning 表示被 Worker 持有（或持有者租约已过期待接管）。
	TurnRunning TurnStatus = "running"
	// TurnWaiting 表示已释放租约，等待到点重新领取（对账、限流、会话忙等）。
	TurnWaiting TurnStatus = "waiting"
	// TurnDone 表示正常结束。
	TurnDone TurnStatus = "done"
	// TurnFailed 表示失败结束。
	TurnFailed TurnStatus = "failed"
	// TurnStopped 表示按用户请求停止。
	TurnStopped TurnStatus = "stopped"
)

// Terminal 判断状态是否为终态。
func (s TurnStatus) Terminal() bool { return s == TurnDone || s == TurnFailed || s == TurnStopped }

// Open 判断状态是否为未结算状态。
func (s TurnStatus) Open() bool { return s == TurnQueued || s == TurnRunning || s == TurnWaiting }

// TurnPhase 是单轮执行推进到的阶段；只能前进，Rebind 时回到 create。
type TurnPhase string

const (
	// PhaseNone 表示尚未决定使用哪个 Session。
	PhaseNone TurnPhase = ""
	// PhaseCreate 表示已持久化创建意图，正在创建或找回 Session。
	PhaseCreate TurnPhase = "create"
	// PhaseSend 表示已确定 Session，正在提交输入。
	PhaseSend TurnPhase = "send"
	// PhaseObserve 表示输入已被受理，正在观察本轮结果。
	PhaseObserve TurnPhase = "observe"
)

func (p TurnPhase) rank() int {
	switch p {
	case PhaseNone:
		return 0
	case PhaseCreate:
		return 1
	case PhaseSend:
		return 2
	case PhaseObserve:
		return 3
	}
	return -1
}

// Schedule 是 Turn 的调度信息。StartedAt 只在首次领取时写入；DeadlineAt 随有效进展续期。
type Schedule struct {
	Attempt       uint64    `json:"attempt"`
	NextAttemptAt time.Time `json:"next_attempt_at"`
	StartedAt     time.Time `json:"started_at"`
	DeadlineAt    time.Time `json:"deadline_at"`
}

// TurnSession 是本轮使用的 Session 及其代数。Renewed 为 true 表示原 Session 已不可用、
// 本轮切换到了新会话，结算回复需附带提示。
type TurnSession struct {
	SnapshotID  string `json:"snapshot_id,omitempty"`
	Generation  uint64 `json:"generation"`
	CreationKey string `json:"creation_key,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	Renewed     bool   `json:"renewed,omitempty"`
}

// Submission 记录当前阶段的远程写：SubmittedAt 是最近一次写发出前的本地时间，WriteAttempts 是
// 当前阶段已发出的写次数。
type Submission struct {
	SubmittedAt   time.Time `json:"submitted_at"`
	WriteAttempts uint64    `json:"write_attempts"`
}

// FrozenInput 是首次提交前冻结的输入：附件块与背景各自只写一次，之后重试原样复用。
type FrozenInput struct {
	StoredRecord `json:"-"`
	// LogicalDigest 仅由Store保存已脱敏输入的内容hash，不能由输入DTO指定。
	LogicalDigest string       `json:"_logical_digest,omitempty"`
	BlocksFrozen  bool         `json:"blocks_frozen,omitempty"`
	Blocks        []InputBlock `json:"blocks,omitempty"`
	ContextFrozen bool         `json:"context_frozen,omitempty"`
	Context       string       `json:"context,omitempty"`
	// MountsFrozen 表示附件挂载已经完成（或已确定跳过），之后重试只复用 Mounts。
	MountsFrozen bool `json:"mounts_frozen,omitempty"`
	// Mounts 记录每个已上传附件在当前 Session 的挂载结果；Session 换代后整体失效并重新挂载。
	Mounts []MountedFile `json:"mounts,omitempty"`
	// payloadRefsReset 标记换代重建 Session，旧的 context/mounts 密文引用失效；
	// 不参与序列化，只在持有租约的同一次写入中由存储层读取。
	payloadRefsReset bool `json:"-"`
}

// PayloadRefsReset 报告换代是否要求存储层丢弃旧输入的密文引用。
func (f FrozenInput) PayloadRefsReset() bool { return f.payloadRefsReset }

// merge 把 next 中已冻结的部分合入 f；已冻结的部分内容不同返回 ErrConflict。
func (f FrozenInput) merge(next FrozenInput) (FrozenInput, error) {
	if next.BlocksFrozen || next.ContextFrozen || next.MountsFrozen {
		f.LogicalDigest = ""
	}
	if next.BlocksFrozen {
		if f.BlocksFrozen && !reflect.DeepEqual(normalizeBlocks(f.Blocks), normalizeBlocks(next.Blocks)) {
			return f, ErrConflict
		}
		f.BlocksFrozen, f.Blocks = true, normalizeBlocks(next.Blocks)
	}
	if next.ContextFrozen {
		if f.ContextFrozen && f.Context != next.Context {
			return f, ErrConflict
		}
		f.ContextFrozen, f.Context = true, next.Context
	}
	if next.MountsFrozen {
		if f.MountsFrozen && !reflect.DeepEqual(normalizeMounts(f.Mounts), normalizeMounts(next.Mounts)) {
			return f, ErrConflict
		}
		f.MountsFrozen, f.Mounts = true, normalizeMounts(next.Mounts)
	}
	return f, nil
}

func normalizeMounts(mounts []MountedFile) []MountedFile {
	if len(mounts) == 0 {
		return nil
	}
	return append([]MountedFile(nil), mounts...)
}

func normalizeBlocks(blocks []InputBlock) []InputBlock {
	if len(blocks) == 0 {
		return nil
	}
	return append([]InputBlock(nil), blocks...)
}

// TurnOutcome 是 Turn 的结算结果。
type TurnOutcome struct {
	Status    TurnStatus `json:"status"`
	ErrorKind string     `json:"error_kind,omitempty"`
	SettledAt time.Time  `json:"settled_at"`
}

// Turn 是一条用户消息对应的单轮执行（聚合根）。
type Turn struct {
	// Activity 持久化最后有效进展与去重高水位，重新领取不重置。
	Activity *TurnActivity `json:"activity,omitempty"`
	WorkKind string        `json:"_work_kind,omitempty"`
	// AuthWait 保存绝对授权截止时间，不能随着重启延长。
	AuthWait     *UserAuthWait `json:"auth_wait,omitempty"`
	StoredRecord `json:"-"`
	Payloads     *StoredPayloads `json:"_payloads,omitempty"`
	Snapshot     *TurnSnapshot   `json:"snapshot,omitempty"`
	ID           string          `json:"id"`
	// EnqueueOrder 是跨会话的全局入队顺序，由存储层在入站事务分配（SQL AUTO_INCREMENT 或
	// 单写者 MAX+1）；只用于领取排序，实体身份仍以 ID 为准。内存实现可留零值。
	EnqueueOrder   uint64    `json:"enqueue_order,omitempty"`
	Seq            uint64    `json:"seq"`
	ConversationID RouteKey  `json:"conversation_id"`
	MessageID      string    `json:"message_id"`
	BindingID      string    `json:"binding_id"`
	BindingVersion string    `json:"binding_version"`
	InputEventID   string    `json:"input_event_id"`
	CreatedAt      time.Time `json:"created_at"`

	Status   TurnStatus `json:"status"`
	Phase    TurnPhase  `json:"phase"`
	Schedule Schedule   `json:"schedule"`
	Lease    Lease      `json:"lease"`
	// QueueDeadlineAt 是排队时限：从消息 received_at 起算，到期时即使本轮仍在队尾，
	// 也必须以超时诊断结算（与首次领取后才起算的 Schedule.DeadlineAt 分开）。
	// 零值表示不限制排队时限。
	QueueDeadlineAt time.Time   `json:"queue_deadline_at,omitempty"`
	Session         TurnSession `json:"session"`
	Submit          Submission  `json:"submit"`
	Input           FrozenInput `json:"input"`
	// StopRequestedAt 非零表示用户要求停止；由入站事务写入，不需要租约。
	StopRequestedAt time.Time `json:"stop_requested_at"`
	// StopReplyTarget 避免停止反馈回复到已经撤回的消息；none 表示原话题已无可用目标。
	StopReplyTarget *ReplyTarget `json:"stop_reply_target,omitempty"`
	// WaitReason 是最近一次等待的原因，消息到期时用于选择回复文案。
	WaitReason string       `json:"wait_reason,omitempty"`
	Outcome    *TurnOutcome `json:"outcome,omitempty"`
	Revision   uint64       `json:"revision"`
	// Progress 是原生过程的非密恢复点；正文只驻留内存或加密 Delivery。
	Progress *ProgressCheckpoint `json:"progress,omitempty"`
}

// InputEventIDFor 返回发给 MA 的稳定 user.message ID。
func InputEventIDFor(contentHash string) string { return "input:" + contentHash }

// EligibleAt 返回 Turn 最早可被领取的时间：max(NextAttemptAt, 租约到期)。已登记停止且没有
// 有效租约时立即可领取。
func (t Turn) EligibleAt(now time.Time) time.Time {
	if !t.Status.Open() {
		return time.Time{}
	}
	next := t.Schedule.NextAttemptAt
	if !t.StopRequestedAt.IsZero() && !t.Lease.Live(now) {
		next = time.Time{}
	}
	if t.Lease.Owner != "" && t.Lease.ExpiresAt.After(next) {
		return t.Lease.ExpiresAt
	}
	return next
}

// Claim 返回被 owner 领取后的 Turn：fence 加一，首次领取写入 StartedAt 与 DeadlineAt。
func (t Turn) Claim(owner string, now time.Time, ttl, deadline time.Duration) Turn {
	t.Status = TurnRunning
	t.Lease = Lease{ID: t.ID, Owner: owner, Fence: t.Lease.Fence + 1, ExpiresAt: now.Add(ttl)}
	t.Schedule.Attempt++
	if t.Schedule.StartedAt.IsZero() {
		t.Schedule.StartedAt = now
		t.Schedule.DeadlineAt = now.Add(deadline)
		t.Activity = &TurnActivity{LastProgressAt: now.UTC(), Initialized: true}
	}
	t.Revision++
	return t
}

// Holds 判断 lease 是否是 Turn 当前有效的写入租约。
func (t Turn) Holds(lease Lease, now time.Time) bool {
	return t.Status == TurnRunning && lease.ID == t.ID && lease.Owner != "" && lease.Owner == t.Lease.Owner && lease.Fence == t.Lease.Fence && now.Before(t.Lease.ExpiresAt)
}

// TurnUpdate 是持有租约的 Worker 对 Turn 的一次推进。
//
// Session 中非空字段只能写一次（不同值返回 ErrConflict），Rebind 时整体覆盖并要求代数前进；
// Submission 每次覆盖；Input 中已冻结的部分只写一次。
type TurnUpdate struct {
	Activity      *TurnActivity
	IdleTimeout   time.Duration
	Progress      *ProgressCheckpoint
	RemoteIntent  *RemoteIntent
	RemoteProof   *RemoteProof
	RemoteKind    RemoteKind
	RemoteCleanup bool
	Phase         TurnPhase
	Session       TurnSession
	Submit        Submission
	Input         FrozenInput
	Rebind        bool
}

// Apply 按迁移规则把 update 应用到 Turn。
func (t Turn) Apply(update TurnUpdate) (Turn, error) {
	if update.RemoteIntent != nil {
		if t.WorkKind == WorkKindUserAuth && update.RemoteIntent.UserAuthKey == "" {
			return t, ErrUserAuthGate
		}
		if update.RemoteIntent.UserAuthKey != "" {
			t.WorkKind = WorkKindUserAuth
		}
	}
	if update.Phase.rank() < 0 {
		return t, ErrInvalid
	}
	if update.Rebind {
		if update.Phase != PhaseCreate || update.Session.Generation <= t.Session.Generation || update.Session.CreationKey == "" {
			return t, ErrInvalid
		}
		t.Session = update.Session
		// 换代重建 Session：已上传附件与 Session 无关，复用其 file_id；
		// 重新准备背景和挂载，并要求存储层丢弃旧的输入密文引用。
		t.Input = FrozenInput{BlocksFrozen: t.Input.BlocksFrozen, Blocks: normalizeBlocks(t.Input.Blocks), payloadRefsReset: true}
	} else {
		if update.Phase.rank() < t.Phase.rank() {
			return t, ErrInvalid
		}
		next := t.Session
		var err error
		if next.SnapshotID, err = writeOnce(next.SnapshotID, update.Session.SnapshotID); err != nil {
			return t, err
		}
		if next.Generation, err = writeOnceUint(next.Generation, update.Session.Generation); err != nil {
			return t, err
		}
		if next.CreationKey, err = writeOnce(next.CreationKey, update.Session.CreationKey); err != nil {
			return t, err
		}
		if next.SessionID, err = writeOnce(next.SessionID, update.Session.SessionID); err != nil {
			return t, err
		}
		next.Renewed = next.Renewed || update.Session.Renewed
		t.Session = next
	}
	input, err := t.Input.merge(update.Input)
	if err != nil {
		return t, err
	}
	t.Input = input
	if update.Progress != nil {
		next := *update.Progress
		if len(next.Handle) > 4096 || len(next.Cursor) > 16384 {
			return t, ErrInvalid
		}
		if old := t.Progress; old != nil {
			if next.Sequence < old.Sequence || old.Handle != "" && old.Handle != next.Handle || old.Failed && !next.Failed {
				return t, ErrConflict
			}
		}
		t.Progress = &next
	}
	if update.Activity != nil {
		next := cloneActivity(update.Activity)
		if next.LastProgressAt.IsZero() || update.IdleTimeout <= 0 || len(next.Streams) > maxActivityStreams || len(next.SeenEvents) > maxActivityEvents {
			return t, ErrInvalid
		}
		for _, offsets := range next.Streams {
			if len(offsets) > maxActivityBlocks {
				return t, ErrInvalid
			}
		}
		if next.SessionID != "" && next.SessionID != t.Session.SessionID || next.InputEventID != "" && next.InputEventID != t.InputEventID {
			return t, ErrConflict
		}
		if old := t.Activity; old != nil {
			if next.SessionID == old.SessionID && next.InputEventID == old.InputEventID {
				for id := range old.SeenEvents {
					if !next.SeenEvents[id] {
						return t, ErrConflict
					}
				}
				for id, offsets := range old.Streams {
					if fresh, ok := next.Streams[id]; ok {
						for i, offset := range offsets {
							if i >= len(fresh) || fresh[i] < offset {
								return t, ErrConflict
							}
						}
					}
				}
			}
			if next.LastProgressAt.Before(old.LastProgressAt) || old.Initialized && !next.Initialized {
				return t, ErrConflict
			}
		}
		t.Activity = &next
		t.Schedule.DeadlineAt = next.LastProgressAt.Add(update.IdleTimeout)
	}
	t.Phase = update.Phase
	t.Submit = Submission{WriteAttempts: update.Submit.WriteAttempts}
	if !update.Submit.SubmittedAt.IsZero() {
		t.Submit.SubmittedAt = update.Submit.SubmittedAt.UTC()
	}
	t.WaitReason = ""
	t.Revision++
	return t, nil
}

func writeOnce(current, next string) (string, error) {
	if next == "" {
		return current, nil
	}
	if current != "" && current != next {
		return current, ErrConflict
	}
	return next, nil
}

func writeOnceUint(current, next uint64) (uint64, error) {
	if next == 0 {
		return current, nil
	}
	if current != 0 && current != next {
		return current, ErrConflict
	}
	return next, nil
}

// TurnWait 让 Turn 释放租约并在 Until 之后重新领取；Submit 覆盖当前阶段的写记录。
type TurnWait struct {
	Reason string
	Until  time.Time
	Submit Submission
}

// Wait 按 wait 把 Turn 迁移到 waiting 并释放租约。
func (t Turn) Wait(wait TurnWait) (Turn, error) {
	if wait.Reason == "" {
		return t, ErrInvalid
	}
	t.Status = TurnWaiting
	t.WaitReason = wait.Reason
	t.Schedule.NextAttemptAt = wait.Until.UTC()
	t.Submit = Submission{WriteAttempts: wait.Submit.WriteAttempts}
	if !wait.Submit.SubmittedAt.IsZero() {
		t.Submit.SubmittedAt = wait.Submit.SubmittedAt.UTC()
	}
	t.Lease.Owner, t.Lease.ExpiresAt = "", time.Time{}
	t.Revision++
	return t, nil
}

// Release 不改变进度地交还租约。
func (t Turn) Release() Turn {
	t.Lease.Owner, t.Lease.ExpiresAt = "", time.Time{}
	t.Revision++
	return t
}

// Settle 把 Turn 迁移到终态。
func (t Turn) Settle(outcome TurnOutcome, now time.Time) (Turn, error) {
	if !outcome.Status.Terminal() {
		return t, ErrInvalid
	}
	outcome.SettledAt = now
	t.Status = outcome.Status
	t.Outcome = &outcome
	t.Lease.Owner, t.Lease.ExpiresAt = "", time.Time{}
	t.Revision++
	return t, nil
}

// RequestStop 登记停止请求；没有有效租约时清零可领取时间，让 Turn 立即被领取并收敛。
func (t Turn) RequestStop(now time.Time) Turn {
	if t.StopRequestedAt.IsZero() {
		t.StopRequestedAt = now
	}
	if !t.Lease.Live(now) {
		t.Schedule.NextAttemptAt = time.Time{}
	}
	t.Revision++
	return t
}

// ---------------------------------------------------------------------------
// ControlRequest

// ControlKind 是控制命令类型。
type ControlKind string

// ControlStop 是停止当前轮次的命令。
const ControlStop ControlKind = "stop"

// ControlUnsupportedMessage 是未支持消息的诊断，不停止或创建执行任务。
const ControlUnsupportedMessage ControlKind = "unsupported_message"

// ControlRequest 是一次控制命令，在入站事务内同步生效，不进入队列。
type ControlRequest struct {
	StoredRecord   `json:"-"`
	ID             string      `json:"id"`
	Seq            uint64      `json:"seq"`
	MessageID      string      `json:"message_id"`
	ConversationID RouteKey    `json:"conversation_id"`
	Kind           ControlKind `json:"kind"`
	// TargetTurnID 是生效时命中的 Turn，为空表示当时没有进行中的轮次。
	TargetTurnID string    `json:"target_turn_id,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

// ---------------------------------------------------------------------------
// Interaction

// InteractionKind 是待用户处理的交互类型。
type InteractionKind string

// InteractionApproval 是工具调用审批。
const InteractionApproval InteractionKind = "approval"

// InteractionStatus 是交互状态。
type InteractionStatus string

const (
	// InteractionPending 表示等待用户决定。
	InteractionPending InteractionStatus = "pending"
	// InteractionAllowed 表示用户允许。
	InteractionAllowed InteractionStatus = "allowed"
	// InteractionDenied 表示用户拒绝。
	InteractionDenied InteractionStatus = "denied"
	// InteractionExpired 表示轮次已结算或超时。
	InteractionExpired InteractionStatus = "expired"
)

// Interaction 是持久化的待用户处理交互（聚合根），重启后仍然有效。
type Interaction struct {
	StoredRecord    `json:"-"`
	Payloads        *StoredPayloads   `json:"_payloads,omitempty"`
	ID              string            `json:"id"`
	TurnID          string            `json:"turn_id"`
	Kind            InteractionKind   `json:"kind"`
	Status          InteractionStatus `json:"status"`
	ApproverID      string            `json:"approver_id"`
	ToolUseID       string            `json:"tool_use_id"`
	SessionThreadID string            `json:"session_thread_id,omitempty"`
	ToolName        string            `json:"tool_name,omitempty"`
	// InputPreview 是有限长度的参数摘要，按不可信内容处理。
	InputPreview string `json:"input_preview,omitempty"`
	// ConfirmEventID 是 user.tool_confirmation 的稳定事件 ID，重试复用。
	ConfirmEventID string `json:"confirm_event_id"`
	// BindingID / BindingEpoch / AppID / TenantKey 在登记时从 Turn 冻结，卡片决定时
	// 必须与当前凭证完全一致，防止换绑后的旧卡片继续审批。
	BindingID    string `json:"binding_id"`
	BindingEpoch uint64 `json:"binding_epoch,omitempty"`
	AppID        string `json:"app_id"`
	TenantKey    string `json:"tenant_key"`
	// ChatID 是审批卡片最初投递的聊天，空值仅兼容旧版本登记。
	ChatID string `json:"chat_id,omitempty"`
	// Prompted 为 true 表示审批卡片已成功展示，重启后不再重复展示。
	Prompted   bool      `json:"prompted,omitempty"`
	DenyReason string    `json:"deny_reason,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	ExpiresAt  time.Time `json:"expires_at"`
	DecidedAt  time.Time `json:"decided_at"`
}

// InteractionID 返回审批交互的稳定 ID。
func InteractionID(turnID, toolUseID string) string { return "ix:" + turnID + ":" + toolUseID }

// InteractionDecision 是用户对交互的决定；绑定坐标由宿主在回调验签后填入，
// 必须与 Interaction 登记时冻结的坐标一致。
type InteractionDecision struct {
	Allow        bool
	DeciderID    string
	DenyReason   string
	BindingID    string
	BindingEpoch uint64
	AppID        string
	TenantKey    string
	ChatID       string
}

// ValidNew 校验待登记的交互。
func (i Interaction) ValidNew() bool {
	if i.IsOpaqueStored() {
		return false
	}
	return i.TurnID != "" && i.ToolUseID != "" && i.ID == InteractionID(i.TurnID, i.ToolUseID) && i.Kind == InteractionApproval && i.Status == InteractionPending && i.ApproverID != "" && i.ConfirmEventID != "" && !i.ExpiresAt.IsZero() && i.BindingID != "" && i.AppID != "" && i.TenantKey != ""
}

// sameRequest 判断两次登记描述的是同一个待确认调用。
func (i Interaction) sameRequest(o Interaction) bool {
	return i.ID == o.ID && i.TurnID == o.TurnID && i.Kind == o.Kind && i.ApproverID == o.ApproverID && i.ToolUseID == o.ToolUseID && i.SessionThreadID == o.SessionThreadID && i.ToolName == o.ToolName && i.InputPreview == o.InputPreview && i.ConfirmEventID == o.ConfirmEventID && i.ExpiresAt.Equal(o.ExpiresAt) && i.BindingID == o.BindingID && i.BindingEpoch == o.BindingEpoch && i.AppID == o.AppID && i.TenantKey == o.TenantKey && (i.ChatID == "" || i.ChatID == o.ChatID)
}

// matchesBinding 校验卡片决定携带的绑定坐标与登记时一致；坐标全为空时视为内部/测试调用，
// 跳过该项校验（审批人校验仍然生效）。任一坐标非空则必须全部精确匹配。
func (i Interaction) matchesBinding(d InteractionDecision) bool {
	if i.ChatID != "" && d.ChatID != i.ChatID {
		return false
	}
	given := d.BindingID != "" || d.BindingEpoch != 0 || d.AppID != "" || d.TenantKey != ""
	if !given {
		return true
	}
	return d.BindingID == i.BindingID && d.BindingEpoch == i.BindingEpoch && d.AppID == i.AppID && d.TenantKey == i.TenantKey
}

// Register 把一次重复登记合并到已有交互：请求内容必须相同；只允许把 Prompted 从 false 改为 true。
// 返回合并后的记录以及是否需要写回。
func (i Interaction) Register(next Interaction) (Interaction, bool, error) {
	if !i.sameRequest(next) {
		return i, false, ErrConflict
	}
	if next.Prompted && !i.Prompted && i.Status == InteractionPending {
		i.Prompted = true
		return i, true, nil
	}
	return i, false, nil
}

// Expire 把 pending 交互置为过期；其他状态不变。
func (i Interaction) Expire(now time.Time) (Interaction, bool) {
	if i.Status != InteractionPending {
		return i, false
	}
	i.Status, i.DecidedAt = InteractionExpired, now
	return i, true
}

// Decide 按 decision 决定交互。pending 状态只能决定一次；重复相同的决定返回同一结果。
func (i Interaction) Decide(decision InteractionDecision, now time.Time) (Interaction, bool, error) {
	want := InteractionDenied
	if decision.Allow {
		want = InteractionAllowed
	}
	// 绑定坐标不匹配（如换绑后的旧卡片）：对任何状态都拒绝，且不改变交互。
	if !i.matchesBinding(decision) {
		return i, false, ErrApprovalForbidden
	}
	switch i.Status {
	case InteractionPending:
	case InteractionExpired:
		return i, false, ErrApprovalExpired
	default:
		if decision.DeciderID != i.ApproverID {
			return i, false, ErrApprovalForbidden
		}
		if i.Status != want {
			return i, false, ErrConflict
		}
		return i, false, nil
	}
	if decision.DeciderID == "" || decision.DeciderID != i.ApproverID {
		return i, false, ErrApprovalForbidden
	}
	if !now.Before(i.ExpiresAt) {
		i.Status = InteractionExpired
		return i, true, ErrApprovalExpired
	}
	i.Status, i.DecidedAt, i.DenyReason = want, now, decision.DenyReason
	return i, true, nil
}

// ---------------------------------------------------------------------------
// Delivery

// DeliverySourceKind 是投递来源类型。
type DeliverySourceKind string

const (
	// SourceTurn 表示投递来自 Turn 结算。
	SourceTurn DeliverySourceKind = "turn"
	// SourceControl 表示投递来自控制命令。
	SourceControl DeliverySourceKind = "control"
	// SourceProgress 表示过程闭合，与普通答案的来源队首分开。
	SourceProgress DeliverySourceKind = "progress"
	// SourceReaction 表示原消息的处理状态，与答案使用独立队首。
	SourceReaction DeliverySourceKind = "reaction"
)

// DeliverySource 标识投递所属的来源；同一来源内按 Seq 串行投递。
type DeliverySource struct {
	Kind DeliverySourceKind `json:"kind"`
	ID   string             `json:"id"`
}

// String 返回来源的稳定文本形式。
func (s DeliverySource) String() string { return string(s.Kind) + "|" + s.ID }

// Artifact 是本轮要交付的产物：工具 Files 引用或公开 Session Files 清单提供 FileID，
// Session 文件同时保留 Path 用于正文关联，并以 SessionID 绑定下载凭据。
// 只有 Path 的旧记录及 storage 路径继续通过可选 OutputFileResolver 下载。
type Artifact struct {
	// Kind 为 image / document / video。
	Kind     string `json:"kind"`
	FileID   string `json:"file_id,omitempty"`
	Path     string `json:"path,omitempty"`
	FileName string `json:"file_name,omitempty"`
	// SessionID 和 MimeType 来自公开 Session Files，下载时绑定会话并检查媒体类型。
	SessionID string `json:"session_id,omitempty"`
	MimeType  string `json:"mime_type,omitempty"`
}

// key 返回产物在本轮内的去重与投递幂等键。
func (a Artifact) key() string {
	if a.FileID != "" {
		return a.FileID
	}
	if a.Path != "" {
		return "path:" + a.Path
	}
	return a.FileID
}

// BodyType 是投递载荷类型。
type BodyType string

const (
	// BodyText 是文本消息。
	BodyText BodyType = "text"
	// BodyRichText 是包含已上传图片/视频引用的最终富文本，旧 Worker 不领取。
	BodyRichText BodyType = "rich_text"
	// BodyStreamText 是已确认最终答案的流式发送载荷。
	BodyStreamText BodyType = "stream_text"
	// BodyImageStream 隔离需要图片转换、原文降级及原子分段的新执行语义，旧 Worker 保留不领取。
	BodyImageStream BodyType = "image_stream_text"
	// BodyArtifact 是 Files 产物（图片或文件消息）。
	BodyArtifact BodyType = "artifact"
	// BodySessionFile 隔离必须用 Session 范围凭据下载的 Files 产物，旧 Worker 保留不领取。
	BodySessionFile BodyType = "session_file"
	// BodyOutputFile 是会话产出目录中的文件产物，投递时经 OutputFileResolver 现取下载地址。
	// 与 BodyArtifact 分开：未升级的 Worker 把它当未知载荷保留，滚动发布期间不会误投或误判失败。
	BodyOutputFile BodyType = "output_file"
	// BodyProgress 是原生过程闭合意图，不是用户最终答案。
	BodyProgress BodyType = "progress"
	// BodyReaction 根据关联 Turn 的最新状态收敛原消息上的表情。
	BodyReaction BodyType = "reaction"
)

// DeliveryBody 是带类型标签的投递载荷：Text 只在 BodyText 时有意义，Artifact 只在 BodyArtifact 时有意义。
type DeliveryBody struct {
	StoredRecord `json:"-"`
	Type         BodyType
	Text         string
	// ImageFallbackText 保存保留原始路径的完整代码引用视图，与正文一同加密。
	ImageFallbackText string
	Artifact          Artifact
	Progress          *ProgressDelivery
	Parts             []RichTextPart
	opaque            *opaqueStoredBody
}

type opaqueStoredBody struct {
	kind BodyType
	raw  json.RawMessage
}

// TextBody 构造文本载荷。
func TextBody(text string) DeliveryBody { return DeliveryBody{Type: BodyText, Text: text} }

// ArtifactBody 构造产物载荷。
func ArtifactBody(artifact Artifact) DeliveryBody {
	return DeliveryBody{Type: BodyArtifact, Artifact: artifact}
}

// OutputFileBody 构造会话产出文件载荷。
func OutputFileBody(artifact Artifact) DeliveryBody {
	return DeliveryBody{Type: BodyOutputFile, Artifact: artifact}
}

// IsArtifact 判断载荷是否为任一来源的产物。
func (b DeliveryBody) IsArtifact() bool {
	return b.Type == BodyArtifact || b.Type == BodySessionFile || b.Type == BodyOutputFile
}

// IsTextStream 判断是否需要可恢复的最终答案卡片处理器。
func (b DeliveryBody) IsTextStream() bool {
	return b.Type == BodyStreamText || b.Type == BodyImageStream
}

type wireTextBody struct {
	Type              BodyType `json:"type"`
	Text              string   `json:"text"`
	ImageFallbackText string   `json:"image_fallback_text,omitempty"`
}

type wireArtifactBody struct {
	Type     BodyType `json:"type"`
	Artifact Artifact `json:"artifact"`
}

type wireProgressBody struct {
	Type     BodyType          `json:"type"`
	Progress *ProgressDelivery `json:"progress"`
}

// ErrUnknownBody 表示投递载荷类型无法识别。
var ErrUnknownBody = errors.New("unknown delivery body type")

// MarshalJSON 按类型标签编码；未知类型报错。
func (b DeliveryBody) MarshalJSON() ([]byte, error) {
	if b.opaque != nil && b.opaque.kind == b.Type {
		return b.opaque.raw, nil
	}
	switch b.Type {
	case BodyRichText:
		return json.Marshal(wireRichTextBody{Type: b.Type, Parts: b.Parts})
	case BodyReaction:
		return json.Marshal(struct {
			Type BodyType `json:"type"`
		}{b.Type})
	case BodyText, BodyStreamText, BodyImageStream:
		return json.Marshal(wireTextBody{Type: b.Type, Text: b.Text, ImageFallbackText: b.ImageFallbackText})
	case BodyArtifact, BodySessionFile, BodyOutputFile:
		return json.Marshal(wireArtifactBody{Type: b.Type, Artifact: b.Artifact})
	case BodyProgress:
		return json.Marshal(wireProgressBody{Type: b.Type, Progress: b.Progress})
	}
	return nil, fmt.Errorf("%w: %q", ErrUnknownBody, b.Type)
}

// UnmarshalJSON 按类型标签解码；未知类型报错，不静默得到零值。
func (b *DeliveryBody) UnmarshalJSON(data []byte) error {
	var head struct {
		Type BodyType `json:"type"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return err
	}
	switch head.Type {
	case BodyRichText:
		var wire wireRichTextBody
		if err := json.Unmarshal(data, &wire); err != nil {
			return err
		}
		*b = RichTextBody(wire.Parts)
	case BodyReaction:
		*b = DeliveryBody{Type: BodyReaction}
	case BodyText, BodyStreamText, BodyImageStream:
		var wire wireTextBody
		if err := json.Unmarshal(data, &wire); err != nil {
			return err
		}
		*b = TextBody(wire.Text)
		b.Type = head.Type
		b.ImageFallbackText = wire.ImageFallbackText
	case BodyArtifact, BodySessionFile, BodyOutputFile:
		var wire wireArtifactBody
		if err := json.Unmarshal(data, &wire); err != nil {
			return err
		}
		*b = ArtifactBody(wire.Artifact)
		b.Type = head.Type
	case BodyProgress:
		var wire wireProgressBody
		if err := json.Unmarshal(data, &wire); err != nil {
			return err
		}
		if wire.Progress == nil {
			return ErrInvalid
		}
		*b = DeliveryBody{Type: BodyProgress, Progress: wire.Progress}
	default:
		return fmt.Errorf("%w: %q", ErrUnknownBody, head.Type)
	}
	return nil
}

// DeliveryStatus 是投递状态。
type DeliveryStatus string

const (
	// DeliveryPending 表示等待投递。
	DeliveryPending DeliveryStatus = "pending"
	// DeliveryUncertain 表示上次投递结果未知，等待按同一 UUID 重试。
	DeliveryUncertain DeliveryStatus = "uncertain"
	// DeliveryDone 表示投递已确认。
	DeliveryDone DeliveryStatus = "done"
	// DeliveryFailed 表示投递已停止重试。
	DeliveryFailed DeliveryStatus = "failed"
)

// Terminal 判断投递状态是否为终态。
func (s DeliveryStatus) Terminal() bool { return s == DeliveryDone || s == DeliveryFailed }

// Delivery 是待投递的 Outbox 条目（聚合根）。
type Delivery struct {
	StoredRecord `json:"-"`
	// WorkKind 仅保留已发表的表情 JSON 编码兼容；调度与检查点不依赖此字段。
	// Deprecated: 不用于 SQL 列、领取筛选或注册新协议。
	WorkKind string `json:"_work_kind,omitempty"`
	// ReactionState 保存最后一次表情同步的检查点，不包含用户正文。
	ReactionState *MessageReactionState `json:"reaction_state,omitempty"`
	// TextStream 保存最终结果的非密投递进度，不保存正文。
	TextStream        *TextStreamState `json:"text_stream,omitempty"`
	Payloads          *StoredPayloads  `json:"_payloads,omitempty"`
	Snapshot          *TurnSnapshot    `json:"snapshot,omitempty"`
	SessionSnapshotID string           `json:"session_snapshot_id,omitempty"`
	// OutputSessionID 是 output_file 产物所属的 MA Session，投递时据此签名产出目录路径。
	OutputSessionID string         `json:"output_session_id,omitempty"`
	ID              string         `json:"id"`
	Source          DeliverySource `json:"source"`
	// Seq 是同一来源内的投递顺序：0 是文本答案，1..n 是产物。存储层只领取每个来源中
	// Seq 最小的未终态投递。
	Seq             int            `json:"seq"`
	BindingID       string         `json:"binding_id"`
	ReplyTarget     ReplyTarget    `json:"reply_target"`
	Body            DeliveryBody   `json:"body"`
	UUID            string         `json:"uuid"`
	Status          DeliveryStatus `json:"status"`
	Attempt         uint64         `json:"attempt"`
	CreatedAt       time.Time      `json:"created_at"`
	FirstAttemptAt  time.Time      `json:"first_attempt_at"`
	NextAttemptAt   time.Time      `json:"next_attempt_at"`
	Lease           Lease          `json:"lease"`
	RemoteMessageID string         `json:"remote_message_id,omitempty"`
	ErrorKind       string         `json:"error_kind,omitempty"`
	// ProgressAttempted 是原生过程闭合请求的持久发送意图，与领取次数分开，避免未知请求被重放。
	ProgressAttempted bool `json:"progress_attempted,omitempty"`
	// ConversationID 与 InboxSeq 由 SQL Store 持久化时填充：InboxSeq 在会话内按提交顺序单调
	// 递增，作为跨来源全局领取顺序的稳定依据（不依赖随机生成的实体 ID）。
	ConversationID string `json:"conversation_id,omitempty"`
	InboxSeq       uint64 `json:"inbox_seq,omitempty"`
}

// NewDelivery 构造一条待投递记录；ID 与渠道幂等键 UUID 都由 (来源, key) 确定性派生。
func NewDelivery(source DeliverySource, seq int, key, bindingID string, target ReplyTarget, body DeliveryBody) Delivery {
	d := Delivery{ID: "dlv:" + stableKey(string(source.Kind), source.ID, key)[:40], Source: source, Seq: seq, BindingID: bindingID, ReplyTarget: target, Body: body, UUID: deliveryUUID(source.ID + ":" + key), Status: DeliveryPending}
	if body.IsTextStream() {
		d.TextStream = &TextStreamState{}
	}
	return d
}

func deliveryUUID(key string) string {
	h := stableKey(key)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// ValidNew 校验结算时写入的新投递。
func (d Delivery) ValidNew(source DeliverySource) bool {
	if d.IsOpaqueStored() || d.Body.IsOpaqueStored() {
		return false
	}
	if !d.Body.IsTextStream() && d.TextStream != nil {
		return false
	}
	if d.ID == "" || d.UUID == "" || d.BindingID == "" || d.Source != source || d.Seq < 0 || d.Status != DeliveryPending {
		return false
	}
	if source.Kind == SourceReaction && d.Body.Type != BodyReaction {
		return false
	}
	switch d.Body.Type {
	case BodyRichText:
		return source.Kind == SourceTurn && d.Seq == 0 && CanRichText(d.Body.Parts)
	case BodyReaction:
		return source.Kind == SourceReaction && d.Seq == 0 && d.ReplyTarget.MessageID != "" && d.ReplyTarget.ChatID != ""
	case BodyStreamText, BodyImageStream:
		if d.Body.Type == BodyImageStream && !CanStreamText(d.Body.ImageFallbackText) {
			return false
		}
		return source.Kind == SourceTurn && d.Seq == 0 && CanStreamText(d.Body.Text) && d.TextStream != nil && *d.TextStream == (TextStreamState{})
	case BodyText:
		return d.Body.Text != ""
	case BodyArtifact:
		return d.Body.Artifact.FileID != ""
	case BodySessionFile:
		return validFileID(d.Body.Artifact.FileID) && d.Body.Artifact.SessionID != ""
	case BodyOutputFile:
		_, ok := normalizeOutputPath(d.Body.Artifact.Path)
		return ok && d.Body.Artifact.FileID == "" && d.OutputSessionID != ""
	case BodyProgress:
		if source.Kind != SourceProgress || d.Body.Progress == nil || d.Body.Progress.Checkpoint.Handle == "" || d.Body.Progress.Update.TurnID != source.ID {
			return false
		}
		state := d.Body.Progress.Update.State
		return state == ProgressDone || state == ProgressFailed || state == ProgressStopped
	}
	return false
}

// EligibleAt 返回投递最早可被领取的时间：max(NextAttemptAt, 租约到期)。
func (d Delivery) EligibleAt() time.Time {
	if d.Status.Terminal() {
		return time.Time{}
	}
	if d.Lease.Owner != "" && d.Lease.ExpiresAt.After(d.NextAttemptAt) {
		return d.Lease.ExpiresAt
	}
	return d.NextAttemptAt
}

// Claim 返回被 owner 领取后的投递。
func (d Delivery) Claim(owner string, now time.Time, ttl time.Duration) Delivery {
	d.Lease = Lease{ID: d.ID, Owner: owner, Fence: d.Lease.Fence + 1, ExpiresAt: now.Add(ttl)}
	d.Attempt++
	if d.FirstAttemptAt.IsZero() {
		d.FirstAttemptAt = now
	}
	return d
}

// Holds 判断 lease 是否是投递当前有效的租约。
func (d Delivery) Holds(lease Lease, now time.Time) bool {
	return !d.Status.Terminal() && lease.ID == d.ID && lease.Owner != "" && lease.Owner == d.Lease.Owner && lease.Fence == d.Lease.Fence && now.Before(d.Lease.ExpiresAt)
}

// DeliveryResult 是一次投递尝试后的结果；非终态结果表示稍后按同一 UUID 重试。
type DeliveryResult struct {
	ReactionState   *MessageReactionState
	Status          DeliveryStatus
	RemoteMessageID string
	NextAttemptAt   time.Time
	ErrorKind       string
}

// Complete 把一次投递结果应用到投递并释放租约。
func (d Delivery) Complete(result DeliveryResult) (Delivery, error) {
	if result.ReactionState != nil {
		if d.Source.Kind != SourceReaction {
			return d, ErrInvalid
		}
		next := *result.ReactionState
		d.ReactionState = &next
	}
	switch result.Status {
	case DeliveryPending, DeliveryUncertain, DeliveryDone, DeliveryFailed:
	default:
		return d, ErrInvalid
	}
	d.Status, d.RemoteMessageID, d.NextAttemptAt, d.ErrorKind = result.Status, result.RemoteMessageID, result.NextAttemptAt.UTC(), result.ErrorKind
	if result.NextAttemptAt.IsZero() {
		d.NextAttemptAt = time.Time{}
	}
	d.Lease.Owner, d.Lease.ExpiresAt = "", time.Time{}
	return d, nil
}

// Retry 把 failed 投递恢复为 pending 并重置尝试预算；UUID 不变。
func (d Delivery) Retry() (Delivery, error) {
	if d.Status != DeliveryFailed || d.Source.Kind == SourceProgress {
		return d, ErrConflict
	}
	d.Status, d.Attempt, d.FirstAttemptAt, d.NextAttemptAt, d.ErrorKind = DeliveryPending, 0, time.Time{}, time.Time{}, ""
	if d.ReactionState != nil {
		state := *d.ReactionState
		state.Failures, state.CheckedAt = 0, time.Time{}
		d.ReactionState = &state
	}
	if d.TextStream != nil {
		state := *d.TextStream
		state.Failures = 0
		d.TextStream = &state
	}
	d.Lease.Owner, d.Lease.ExpiresAt = "", time.Time{}
	return d, nil
}

// ---------------------------------------------------------------------------
// 控制命令的同步回复

// ReplyNoActiveTurn 是 /stop 命中不到进行中轮次时的回复。
const ReplyNoActiveTurn = "当前没有进行中的任务。"

// ReplyUnsupportedMessage 引导用户改用可处理的消息形式。
const ReplyUnsupportedMessage = "暂不支持这种消息类型，请改用文字或文件发送。"

// NewControlReply 构造控制命令在入站事务内直接写入的回复投递。
func NewControlReply(control ControlRequest, message InboundMessage, binding Binding, text string) Delivery {
	return NewDelivery(DeliverySource{Kind: SourceControl, ID: control.ID}, 0, string(control.Kind), binding.ID, ReplyTargetFor(message.Message, binding), TextBody(text))
}
