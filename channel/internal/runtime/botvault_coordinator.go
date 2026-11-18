// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"
)

// botVaultOwnerID 生成本进程稳定的 reservation owner 标识：主机名 + 随机后缀。
// 同一进程内多次协调复用该 owner（使 POST 重试能命中同一条 pending 意图）；
// 不同进程 owner 不同，过期 fence 由 Store 判定接管。
func botVaultOwnerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "runner"
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "vault-owner:" + host
	}
	return "vault-owner:" + host + ":" + hex.EncodeToString(b[:])
}

// BotVaultCoordinator 用持久 reservation/fence 协调公开 Vault Create（无幂等键）。
//
// 生命周期（替代旧的「后到者删除自己 Vault」）：
//  1. POST 前 Reserve 持久 pending 意图（fence）；已被确认则直接拿 winner 的 VaultID。
//  2. 发 POST 前续约 fence 并标记 PostAttempted。
//  3. POST 成功：校验返回的 ID 和 metadata principal 后直接 Confirm。
//  4. POST 明确未受理（4xx）：Reject 解除自己的意图。
//  5. POST 结果未知（超时/5xx）：不解除、不重建，只按名查找，找到且归属正确则 Confirm；
//     找不到返回 ErrBotVaultCreateUnknown，由调用方稍后重跑本协调。
type BotVaultCoordinator struct {
	store       BotVaultStore
	provisioner BotVaultProvisioner
	owner       string
}

// NewBotVaultCoordinator 构造协调器；owner 为空时生成进程级稳定标识。
func NewBotVaultCoordinator(store BotVaultStore, provisioner BotVaultProvisioner, owner string) *BotVaultCoordinator {
	if owner == "" {
		owner = botVaultOwnerID()
	}
	return &BotVaultCoordinator{store: store, provisioner: provisioner, owner: owner}
}

// ErrBotVaultCreateUnknown 表示 POST 结果未知且暂未按名查到 Vault：调用方应稍后重试协调，
// 不能再次 POST（reservation 已标记 PostAttempted）。
var ErrBotVaultCreateUnknown = errors.New("bot vault create result unknown")

// botVaultRejectTimeout 限制已证明未发送/未受理后的持久意图清理，不继承调用方取消。
const botVaultRejectTimeout = 3 * time.Second

// botVaultMetadataPrincipal 是创建 Vault 时写入 metadata 的归属键。
const botVaultMetadataPrincipal = "ark_channel_principal"

// EnsureVault 保证返回归属于 principal 的同名 Vault ID；跨 principal 同名占用确定性失败。
//
// createSpec 只承载非密的名称/展示名；principal 与 owner 由协调器统一管理。
func (c *BotVaultCoordinator) EnsureVault(ctx context.Context, principal string, createSpec BotVaultCreate, ttl time.Duration) (VaultRef, error) {
	if c == nil || c.store == nil || c.provisioner == nil || principal == "" || createSpec.Name == "" || ttl <= 0 {
		return VaultRef{}, ErrInvalid
	}
	reservation, err := c.store.ReserveBotVault(ctx, principal, createSpec.Name, c.owner, ttl)
	if err != nil {
		return VaultRef{}, err
	}
	// Winner 已确认：读取后校验归属再返回（不重新创建）。
	if reservation.State == BotVaultIntentConfirmed && reservation.VaultID != "" {
		return c.verifyConfirmed(ctx, principal, reservation.VaultID, createSpec.Name)
	}
	// 被其他 owner 以有效 fence 持有的并发创建：不重建，转为只查找 winner 结果。
	if reservation.Owner != c.owner {
		return c.lookupWinner(ctx, principal, createSpec.Name)
	}

	// 未尝试过 POST 的意图可以直接创建；重启后的未知结果由 PostAttempted 分流查账。
	if !reservation.PostAttempted {
		// POST 前续约 fence，随后立即标记已尝试，缩小「已发 POST、未标记」的崩溃窗口。
		if _, err := c.store.RenewBotVaultFence(ctx, principal, createSpec.Name, c.owner, reservation.Fence, ttl); err != nil {
			return VaultRef{}, err
		}
		if _, err := c.store.MarkBotVaultPosted(ctx, principal, createSpec.Name, c.owner, reservation.Fence); err != nil {
			return VaultRef{}, err
		}
		spec := createSpec
		if spec.Metadata == nil {
			spec.Metadata = map[string]string{}
		} else {
			spec.Metadata = cloneStringMap(spec.Metadata)
		}
		spec.Metadata[botVaultMetadataPrincipal] = principal
		created, createErr := c.provisioner.CreateVault(ctx, spec)
		switch {
		case createErr == nil:
			return c.bindCreated(ctx, principal, spec.Name, created)
		case errors.Is(createErr, ErrVaultNotCreated):
			// 明确未受理：解除自己的意图，本次确定性失败；下次调用可重新 POST。
			rejectCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), botVaultRejectTimeout)
			rejectErr := c.store.RejectBotVault(rejectCtx, principal, spec.Name, c.owner, reservation.Fence)
			cancel()
			if rejectErr != nil {
				return VaultRef{}, rejectErr
			}
			return VaultRef{}, createErr
		default:
			// 未知受理：意图保留，只查找不重建。
			return c.lookupAfterUnknown(ctx, principal, spec.Name, reservation.Fence)
		}
	}
	// 之前已发过 POST（重试/崩溃恢复）：只查找，绝不再次 POST。
	return c.lookupAfterUnknown(ctx, principal, createSpec.Name, reservation.Fence)
}

// bindCreated 直接绑定创建响应中的 ID，避免创建成功后又依赖列表扫描。
func (c *BotVaultCoordinator) bindCreated(ctx context.Context, principal, name string, created VaultRef) (VaultRef, error) {
	reservation, err := c.store.GetBotVaultReservation(ctx, principal, name)
	if err != nil {
		return VaultRef{}, err
	}
	if reservation == nil {
		return VaultRef{}, ErrConflict
	}
	if created.ID == "" {
		return VaultRef{}, ErrBotVaultCreateUnknown
	}
	if err := c.assertOwnership(ctx, principal, created); err != nil {
		return VaultRef{}, err
	}
	confirmed, err := c.store.ConfirmBotVault(ctx, principal, name, c.owner, reservation.Fence, created.ID)
	if err != nil {
		return VaultRef{}, err
	}
	return VaultRef{ID: confirmed.VaultID, Name: name}, nil
}

// lookupAfterUnknown 在 POST 结果未知后只按名查找；找到则校验归属并 Confirm，找不到报未知。
func (c *BotVaultCoordinator) lookupAfterUnknown(ctx context.Context, principal, name string, fence uint64) (VaultRef, error) {
	ref, err := c.findUnknown(ctx, principal, name)
	if err != nil {
		return VaultRef{}, err
	}
	if ref.ID == "" {
		return VaultRef{}, ErrBotVaultCreateUnknown
	}
	if err := c.assertOwnership(ctx, principal, ref); err != nil {
		return VaultRef{}, err
	}
	if _, err := c.store.ConfirmBotVault(ctx, principal, name, c.owner, fence, ref.ID); err != nil {
		// fence 可能已被过期接管：若已被 winner 确认同 ID，按 winner 路径成功返回。
		if errors.Is(err, ErrLeaseLost) {
			return c.verifyConfirmed(ctx, principal, ref.ID, name)
		}
		return VaultRef{}, err
	}
	return VaultRef{ID: ref.ID, Name: name}, nil
}

// lookupWinner 处理并发下他人持有的在途意图：只读查找 winner 的已建 Vault；
// winner 尚未产出结果时返回未知，稍后重试，不触碰他人意图。
func (c *BotVaultCoordinator) lookupWinner(ctx context.Context, principal, name string) (VaultRef, error) {
	reservation, err := c.store.GetBotVaultReservation(ctx, principal, name)
	if err != nil {
		return VaultRef{}, err
	}
	if reservation != nil && reservation.State == BotVaultIntentConfirmed && reservation.VaultID != "" {
		return c.verifyConfirmed(ctx, principal, reservation.VaultID, name)
	}
	ref, err := c.findUnknown(ctx, principal, name)
	if err != nil {
		return VaultRef{}, err
	}
	if ref.ID == "" {
		return VaultRef{}, ErrBotVaultCreateUnknown
	}
	if err := c.assertOwnership(ctx, principal, ref); err != nil {
		return VaultRef{}, err
	}
	return VaultRef{}, ErrBotVaultCreateUnknown
}

func (c *BotVaultCoordinator) findUnknown(ctx context.Context, principal, name string) (VaultRef, error) {
	if finder, ok := c.provisioner.(VaultPrincipalFinder); ok {
		return finder.FindVaultByPrincipal(ctx, principal, name)
	}
	return c.provisioner.FindVaultByName(ctx, name)
}

// verifyConfirmed 在 winner 已确认后重新读取 Vault 并校验归属；Vault 已被删除则确定性失败。
func (c *BotVaultCoordinator) verifyConfirmed(ctx context.Context, principal, vaultID, name string) (VaultRef, error) {
	var ref VaultRef
	var err error
	// 已确认ID是身份引用；可变名称只用于创建前与unknown阶段的查账。
	if reader, ok := c.provisioner.(interface {
		GetVault(context.Context, string) (VaultRef, error)
	}); ok {
		ref, err = reader.GetVault(ctx, vaultID)
	} else {
		ref, err = c.provisioner.FindVaultByName(ctx, name)
	}
	if err != nil {
		return VaultRef{}, err
	}
	if ref.ID == "" || ref.ID != vaultID {
		// 冻结ID已不存在或远端返回其它ID：已确认Vault不可用，明确失败。
		return VaultRef{}, fmt.Errorf("%w: confirmed bot vault %s missing", ErrVaultPrincipalMismatch, vaultID)
	}
	if err := c.assertOwnership(ctx, principal, ref); err != nil {
		return VaultRef{}, err
	}
	return VaultRef{ID: vaultID, Name: name}, nil
}

// assertOwnership 校验按名查到的 Vault 属于当前 principal；跨 principal 同名凭据拒绝覆盖/复用。
func (c *BotVaultCoordinator) assertOwnership(ctx context.Context, principal string, ref VaultRef) error {
	owner := ref.Principal
	if owner == "" {
		return ErrVaultPrincipalMismatch
	}
	if owner != principal {
		return ErrVaultPrincipalMismatch
	}
	return nil
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
