// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"time"
)

// Bot Vault reservation 状态机（持久化，跨重启与多副本共享）：
//
//	IntentPending   POST 前写入的创建意图，持 fence/owner，尚未确认远端受理
//	IntentConfirmed 创建已确认（成功或明确失败），Vault 归属已确定
//
// 公开 Vault Create 没有幂等键：状态机用「先持久意图 + fence 续约 + 未知只查找」
// 协调并发创建，替代旧的「后到者删除自己 Vault」路径。
const (
	BotVaultIntentPending   = "pending"
	BotVaultIntentConfirmed = "confirmed"
)

// 跨 principal 同名凭据冲突的确定性哨兵。
var (
	// ErrVaultPrincipalMismatch 表示同名 Vault 已被另一个 principal 确认占用：
	// 不覆盖、不删除、不复用，调用方必须确定性失败。
	ErrVaultPrincipalMismatch = errors.New("bot vault owned by another principal")
	// ErrVaultNameConflict 表示同名意图已被其他 owner 以有效 fence 持有。
	ErrVaultNameConflict = errors.New("bot vault reservation held by another owner")
)

// BotVaultReservation 是按 (principal, vault name) 唯一的持久创建协调记录。
// 不保存任何 token 或 Secret 明文，只记录 Vault 引用与协调状态。
type BotVaultReservation struct {
	StoredRecord `json:"-"`
	// Principal 是 Vault 唯一归属主体（如 account|project|channel|epoch|tenant）。
	Principal string `json:"principal"`
	// Name 是该主体确定性生成的 Vault 名称（跨副本一致）。
	Name string `json:"name"`
	// State 为 IntentPending / IntentConfirmed。
	State string `json:"state"`
	// Fence 标识当前持有者：续租/绑定都必须匹配 Owner 与 Fence。
	Owner string `json:"owner"`
	Fence uint64 `json:"fence"`
	// LeaseExpiresAt 是 pending 意图 fence 的到期时间；confirmed 后不再依赖租约。
	LeaseExpiresAt time.Time `json:"lease_expires_at,omitempty"`
	// VaultID 是确认后远端真实 Vault 的 ID；pending 期间为空。
	VaultID string `json:"vault_id,omitempty"`
	// PostAttempted 标记是否已经发出过一次 POST：置位后若结果未知，只能按名查找，
	// 不允许再次 POST（防止超时后重建出第二个 Vault）。
	PostAttempted bool `json:"post_attempted,omitempty"`
	// CreatedAt / UpdatedAt 记录协调时间线。
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Key 是 reservation 的唯一键。
func (r BotVaultReservation) Key() string { return r.Principal + "\x00" + r.Name }

// Active 表示 fence 当前仍有效（未到期）。
func (r BotVaultReservation) Active(now time.Time) bool {
	return r.State == BotVaultIntentPending && r.LeaseExpiresAt.After(now)
}

// Holds 判断给定 owner/fence 是否持有该意图。
func (r BotVaultReservation) Holds(owner string, fence uint64) bool {
	return r.Owner == owner && r.Fence == fence
}

// BotVaultStore 是 per-principal Vault 创建协调端口；三 Store 实现同一语义。
// 记录不含密钥，只持久「谁在为哪个主体创建哪个名字的 Vault、远端结果如何」。
type BotVaultStore interface {
	// ReserveBotVault 在 POST /vaults 之前持久创建意图（pending + fence）。
	//   - 不存在记录：原子插入并返回新意图（fence=1）。
	//   - 已被同 owner/fence 持有：幂等返回（POST 重试路径）。
	//   - 已 confirmed：返回含 VaultID 的记录（由 winner 创建，调用方转查找）。
	//   - 被其他 owner 以有效 fence 持有：返回 ErrVaultNameConflict。
	//   - 被其他 owner 的过期 fence 持有：接管（fence+1，覆盖 owner/到期时间）。
	ReserveBotVault(ctx context.Context, principal, name, owner string, ttl time.Duration) (BotVaultReservation, error)
	// MarkBotVaultPosted 在实际发出 POST 前把意图标记为已尝试（不改变 fence/owner）；
	// 标记后若 POST 结果未知，后续协调只按名查找、不再 POST。fence 不符返回 ErrLeaseLost。
	MarkBotVaultPosted(ctx context.Context, principal, name, owner string, fence uint64) (BotVaultReservation, error)
	// RenewBotVaultFence 用当前 owner/fence 延长 pending 意图租约；fence 不符返回 ErrLeaseLost。
	RenewBotVaultFence(ctx context.Context, principal, name, owner string, fence uint64, ttl time.Duration) (BotVaultReservation, error)
	// ConfirmBotVault 把远端创建结果绑定到意图。
	//   - fence 必须匹配；已 confirmed 且 VaultID/Principal 一致时幂等返回。
	//   - 记录中的 principal 与请求不符返回 ErrVaultPrincipalMismatch。
	ConfirmBotVault(ctx context.Context, principal, name, owner string, fence uint64, vaultID string) (BotVaultReservation, error)
	// RejectBotVault 在远端明确拒绝创建（4xx 非 412 等确定未受理）时解除自己的 pending 意图，
	// 允许后续按同 key 重新 POST。未知受理（超时/5xx）禁止调用——意图保留，只能查找。
	// 只能解除自己 owner/fence 的 pending 记录；其他状态返回 ErrConflict/ErrLeaseLost。
	RejectBotVault(ctx context.Context, principal, name, owner string, fence uint64) error
	// GetBotVaultReservation 读取协调记录；不存在返回 nil（无错误）。
	GetBotVaultReservation(ctx context.Context, principal, name string) (*BotVaultReservation, error)
}
