// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"time"
)

// UserPrincipal 是授权、凭据和门闩共同使用的完整本人身份。
type UserPrincipal struct {
	Owner     string `json:"owner"`
	Project   string `json:"project"`
	ChannelID string `json:"channel_id"`
	Epoch     uint64 `json:"epoch"`
	AppID     string `json:"app_id"`
	TenantKey string `json:"tenant_key"`
	OpenID    string `json:"open_id"`
}

// Key 返回无分隔符歧义的稳定身份键。
func (p UserPrincipal) Key() string { return "user:" + digestJSON(p) }

// Valid 要求每个身份维度完整，禁止跨主体复用门闩。
func (p UserPrincipal) Valid() bool {
	return p.Owner != "" && p.Project != "" && p.ChannelID != "" && p.Epoch > 0 && p.AppID != "" && p.TenantKey != "" && p.OpenID != ""
}

// UserGrantQuery 查询凭据引用及必需权限。
type UserGrantQuery struct {
	Principal                     UserPrincipal
	VaultID, CredentialID, FlowID string
	RequiredScopes                []string
}

// UserGrant 只携带授权状态和引用，GrantVersion 是宿主稳定授权代次。
type UserGrant struct {
	ExpiresAt     time.Time `json:"expires_at,omitempty"`
	Status        string    `json:"status"`
	VaultID       string    `json:"vault_id,omitempty"`
	CredentialID  string    `json:"credential_id,omitempty"`
	FlowID        string    `json:"flow_id,omitempty"`
	GrantedScopes []string  `json:"granted_scopes,omitempty"`
	GrantVersion  string    `json:"grant_version,omitempty"`
}

// AuthRequest 描述一次授权尝试；Attempt 不是服务端强幂等键。
type AuthRequest struct {
	VaultID, ExistingCredentialID string
	Attempt                       uint64
	Lease                         Lease
	Revision                      uint64
	RequiredScopes                []string
}

// AuthFlow 仅用于内存交互；URL 不得直接进入持久记录。
type AuthFlow struct {
	ID, URL, UserCode string
	ExpiresAt         time.Time
}

// String 返回不含授权链接与用户码的安全视图。
func (f AuthFlow) String() string { return "AuthFlow{redacted}" }

// GoString 返回不含秘密的 Go 格式视图。
func (f AuthFlow) GoString() string { return f.String() }

// UserAuthPromptReader 只读恢复已持久 flow 的原提示；不得重新发起授权。
type UserAuthPromptReader interface {
	AuthPrompt(context.Context, AuthFlowRef) (AuthFlow, error)
}

// AuthFlowRef 使用完整主体与非密流程引用查询状态。
type AuthFlowRef struct {
	RequiredScopes  []string
	Lease           Lease
	Revision        uint64
	Principal       UserPrincipal
	VaultID, FlowID string
}

// AuthStatus 由 authorizer 验证本人后才允许返回 ready。
type AuthStatus struct {
	NextPollAt     time.Time
	Status, Reason string
	Grant          UserGrant
	ExpiresAt      time.Time
}

// UserCredentialReader 只读确认冻结个人凭据是否存在；false 必须是精确删除证据。
// 查询失败、归属不符或其他资源不存在均返回 error，不得解释为凭据删除。
type UserCredentialReader interface {
	UserCredentialExists(context.Context, UserGrantQuery) (bool, error)
}

// UserAuthorizer 将本人验证与授权实现隔离在宿主适配器。
type UserAuthorizer interface {
	Lookup(context.Context, UserGrantQuery) (UserGrant, error)
	Start(context.Context, UserPrincipal, AuthRequest) (AuthFlow, error)
	Status(context.Context, AuthFlowRef) (AuthStatus, error)
}

// UserCredentialSpec 冻结 Session 使用的凭据引用及授权代次。
type UserCredentialSpec struct {
	FlowID       string        `json:"flow_id,omitempty"`
	ExpiresAt    time.Time     `json:"expires_at,omitempty"`
	Principal    UserPrincipal `json:"principal"`
	VaultID      string        `json:"vault_id"`
	CredentialID string        `json:"credential_id"`
	GrantVersion string        `json:"grant_version"`
}

// UserAuthScopeBatchLimit 是单次授权请求允许的权限数量上限。
const UserAuthScopeBatchLimit = 200

// 授权门闩状态只允许 ready 发起新的 User 远端操作。
const (
	UserAuthDraining       = "draining"
	UserAuthPending        = "pending"
	UserAuthVerifying      = "verifying"
	UserAuthReady          = "ready"
	UserAuthInvalidSubject = "invalid_subject"
	UserAuthFailed         = "failed"
	UserAuthUncertain      = "uncertain"
	UserAuthAuthorize      = "authorize"
	UserAuthRefresh        = "refresh"
	WorkKindUserAuth       = "user_auth_v2"
)

// ErrUserAuthGate 表示本人授权门闩尚未放行。
var ErrUserAuthGate = errors.New("user authorization gate closed")

// ErrUserIdentityConflict 表示权威个人 Vault 或冻结主体归属不符；网络/存储错误不得使用它。
var ErrUserIdentityConflict = errors.New("user identity ownership conflict")

// ErrUserVaultDeleted 表示权威查询确认原个人 Vault 已不存在；临时依赖错误不得使用它。
var ErrUserVaultDeleted = errors.New("user authorization vault deleted")

// ErrUserAuthEncryption 表示授权提示缺少强制加密依赖。
var ErrUserAuthEncryption = errors.New("user authorization requires payload encryption")

// UserAuthSecret 保存宿主已经封装的密文；SDK core 不接收 token 或 device code 明文。
type UserAuthSecret struct {
	Version    uint32 `json:"version"`
	KeyID      string `json:"key_id"`
	Ciphertext []byte `json:"ciphertext"`
}

// UserAuthRecord 是 principal 唯一的授权门闩与恢复记录，读取它无需 KMS。
type UserAuthRecord struct {
	StoredRecord `json:"-"`
	// RequiredScopes 是本次 attempt 冻结的全集，后续绑定变更不扩大它。
	RequiredScopes []string `json:"required_scopes,omitempty"`
	// BatchScopes 是当前 flow 实际请求和验证的范围。
	BatchScopes []string `json:"batch_scopes,omitempty"`
	// VerifiedScopes 仅约束后批 token 必须继续覆盖的范围，不构成当前 grant。
	VerifiedScopes []string `json:"verified_scopes,omitempty"`
	// Batch 在同一 attempt 内递增，用于持久恢复点和独立提示身份。
	Batch     uint64        `json:"batch,omitempty"`
	WorkKind  string        `json:"_work_kind,omitempty"`
	Principal UserPrincipal `json:"principal"`
	VaultID   string        `json:"vault_id"`
	// CredentialID 是已确认创建的稳定目标，不表示本人/权限校验已经通过。
	CredentialID string          `json:"credential_id,omitempty"`
	State        string          `json:"state"`
	Operation    string          `json:"operation,omitempty"`
	FlowID       string          `json:"flow_id,omitempty"`
	Grant        UserGrant       `json:"grant"`
	Secret       *UserAuthSecret `json:"secret,omitempty"`
	ExpiresAt    time.Time       `json:"expires_at"`
	Lease        Lease           `json:"lease"`
	Revision     uint64          `json:"revision"`
	Attempt      uint64          `json:"attempt,omitempty"`
}

// UserAuthBegin 申请授权工作租约；开始重授必须先证明所有同主体远端操作已收敛。
type UserAuthBegin struct {
	// DeletedCredentialID 仅用于显式 /new 已确认删除的原凭据；必须同时携带 ExpectedRevision。
	// Store 在同主体远端操作排空后清除旧引用，保留 Vault；未知刷新和密文不能重置。
	DeletedCredentialID string
	// ExpectedRevision 可选观察版本；nil 保留无观察条件的既有调用。
	ExpectedRevision *uint64
	Principal        UserPrincipal
	VaultID, Owner   string
	TTL              time.Duration
}

// UserVaultReplacement 仅在显式新建且旧 Vault 已确认删除后迁移当前授权引用。
// 旧 SessionSnapshot 保留原引用，调用方必须先通过 BeginUserAuth 排空远端操作。
type UserVaultReplacement struct {
	PreviousVaultID string
	VaultID         string
}

// UserAuthUpdate 以租约和 revision CAS 保存非密进度与已加密秘密。
type UserAuthUpdate struct {
	// ReplaceVault 只接受持有授权工作租约的显式新建恢复，不宣告授权 ready。
	ReplaceVault *UserVaultReplacement
	// BeginBatch 仅允许以已验证的本批 grant 推进，普通 Apply 仍禁止 flow 变更。
	BeginBatch                  bool
	RequiredScopes, BatchScopes []string
	BatchGrant                  UserGrant
	// CredentialID 只允许首次确定本 flow 的目标或保持原 ID。
	CredentialID string
	// Release 只在有界远端调用结束后释放当前 gate lease，保留流程和秘密。
	Release       bool
	Principal     UserPrincipal
	Lease         Lease
	Revision      uint64
	State, FlowID string
	Grant         UserGrant
	Secret        *UserAuthSecret
	ExpiresAt     time.Time
	TTL           time.Duration
}

// UserAuthWait 与 Turn 释放租约、可选提示 Outbox 同事务保存；DeadlineAt 不得延长。
type UserAuthWait struct {
	Batch uint64 `json:"batch,omitempty"`
	// RegisterFlow 将本批 flow 登记与等待、加密提示合为一笔事务，不持久化此操作标记。
	RegisterFlow bool          `json:"-"`
	Lease        Lease         `json:"-"`
	Revision     uint64        `json:"-"`
	Attempt      uint64        `json:"attempt,omitempty"`
	PollAttempt  uint64        `json:"poll_attempt,omitempty"`
	Principal    UserPrincipal `json:"principal"`
	FlowID       string        `json:"flow_id"`
	DeadlineAt   time.Time     `json:"deadline_at"`
	Until        time.Time     `json:"until"`
	Prompt       *Delivery     `json:"-"`
}

// UserAuthFailure 将授权状态、失败 Turn 和回复作为一个事务提交。
// 仅 user_auth_timeout 允许同时保存迟到但已验证的 ready Grant，原输入仍不得提交。
type UserAuthFailure struct {
	Update     UserAuthUpdate
	Outcome    TurnOutcome
	Deliveries []Delivery
}

// UserAuthStore 是可选授权端口，未启用 User 的存储无需安装它的表。
type UserAuthStore interface {
	BeginUserAuth(context.Context, UserAuthBegin) (UserAuthRecord, error)
	BeginUserAuthRefresh(context.Context, UserAuthBegin) (UserAuthRecord, error)
	GetUserAuth(context.Context, UserPrincipal) (*UserAuthRecord, error)
	UpdateUserAuth(context.Context, UserAuthUpdate) (UserAuthRecord, error)
	WaitForUserAuth(context.Context, Lease, UserAuthWait) error
	FailUserAuth(context.Context, Lease, UserAuthFailure) error
}

// Authorizer 状态协议：ready 已验证本人；pending 可继续查询原 flow；
// denied/expired/failed 为明确失败；subject_mismatch 为明确本人不符；
// reauth_required 要求重新授权；unknown 代表远端结果不确定，禁止自动重放写入。
// Lookup 仅返回 ready/reauth_required/pending/subject_mismatch/unknown；网络错误通过 error 返回。
const (
	AuthReady           = "ready"
	AuthPending         = "pending"
	AuthDenied          = "denied"
	AuthExpired         = "expired"
	AuthSubjectMismatch = "subject_mismatch"
	AuthReauthRequired  = "reauth_required"
	AuthFailed          = "failed"
	AuthUnknown         = "unknown"
)
