// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// RunBotVaultStore 对三 Store 执行同一套 Bot Vault 持久 reservation 契约。
// 只依赖 core 的端口与哨兵错误，覆盖 S6 协调状态机在存储层的全部不变量。
func RunBotVaultStore(t *testing.T, factory StoreFactory) {
	openVault := func(t *testing.T) (TestStore, *Clock) {
		t.Helper()
		clock := NewClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		s := factory(t, StoreConfig{Clock: clock.Now})
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		return s, clock
	}
	t.Run("ReserveAndConfirm", func(t *testing.T) { botVaultReserveAndConfirm(t, openVault) })
	t.Run("ConcurrentOwnerRejectedAndTakeover", func(t *testing.T) { botVaultConcurrentOwner(t, openVault) })
	t.Run("UnknownResultOnlyReconciles", func(t *testing.T) { botVaultUnknownOnlyReconciles(t, openVault) })
	t.Run("RejectReleasesOwnPending", func(t *testing.T) { botVaultReject(t, openVault) })
	t.Run("PrincipalIsolation", func(t *testing.T) { botVaultPrincipalIsolation(t, openVault) })
	t.Run("FenceRenewal", func(t *testing.T) { botVaultFenceRenewal(t, openVault) })
}

type vaultOpener func(*testing.T) (TestStore, *Clock)

const botVaultTTL = time.Minute

func botVaultReserveAndConfirm(t *testing.T, open vaultOpener) {
	s, _ := open(t)
	ctx := context.Background()
	r, err := s.ReserveBotVault(ctx, "p1", "vault-a", "owner-1", botVaultTTL)
	require.NoError(t, err)
	require.Equal(t, core.BotVaultIntentPending, r.State)
	require.Equal(t, uint64(1), r.Fence)
	require.Equal(t, "owner-1", r.Owner)

	// POST 前标记已尝试。
	marked, err := s.MarkBotVaultPosted(ctx, "p1", "vault-a", "owner-1", r.Fence)
	require.NoError(t, err)
	require.True(t, marked.PostAttempted)

	confirmed, err := s.ConfirmBotVault(ctx, "p1", "vault-a", "owner-1", r.Fence, "vlt_1")
	require.NoError(t, err)
	require.Equal(t, core.BotVaultIntentConfirmed, confirmed.State)
	require.Equal(t, "vlt_1", confirmed.VaultID)

	// 再次确认同 Vault 幂等。
	again, err := s.ConfirmBotVault(ctx, "p1", "vault-a", "owner-1", r.Fence, "vlt_1")
	require.NoError(t, err)
	require.Equal(t, "vlt_1", again.VaultID)

	// 确认后 Reserve 直接返回 winner 记录（不重新 POST）。
	winner, err := s.ReserveBotVault(ctx, "p1", "vault-a", "owner-2", botVaultTTL)
	require.NoError(t, err)
	require.Equal(t, core.BotVaultIntentConfirmed, winner.State)
	require.Equal(t, "vlt_1", winner.VaultID)

	// 不同 Vault ID 确认同 key 是确定性冲突。
	_, err = s.ConfirmBotVault(ctx, "p1", "vault-a", "owner-1", r.Fence, "vlt_other")
	require.ErrorIs(t, err, core.ErrConflict)
}

func botVaultConcurrentOwner(t *testing.T, open vaultOpener) {
	s, clock := open(t)
	ctx := context.Background()
	first, err := s.ReserveBotVault(ctx, "p1", "vault-c", "owner-1", botVaultTTL)
	require.NoError(t, err)

	// fence 有效时另一 owner 不能抢占。
	_, err = s.ReserveBotVault(ctx, "p1", "vault-c", "owner-2", botVaultTTL)
	require.ErrorIs(t, err, core.ErrVaultNameConflict)

	// 同 owner 幂等返回同一意图。
	same, err := s.ReserveBotVault(ctx, "p1", "vault-c", "owner-1", botVaultTTL)
	require.NoError(t, err)
	require.Equal(t, first.Fence, same.Fence)

	// 旧 fence 不能再写。
	_, err = s.MarkBotVaultPosted(ctx, "p1", "vault-c", "owner-1", first.Fence+1)
	require.ErrorIs(t, err, core.ErrLeaseLost)

	// fence 到期后另一 owner 可接管，fence 推进。
	clock.Advance(botVaultTTL + time.Second)
	taken, err := s.ReserveBotVault(ctx, "p1", "vault-c", "owner-2", botVaultTTL)
	require.NoError(t, err)
	require.Equal(t, "owner-2", taken.Owner)
	require.Equal(t, first.Fence+1, taken.Fence)
	require.Empty(t, taken.VaultID)

	// 旧 owner 的过期 fence 无法确认被接管的意图。
	_, err = s.ConfirmBotVault(ctx, "p1", "vault-c", "owner-1", first.Fence, "vlt_stale")
	require.ErrorIs(t, err, core.ErrLeaseLost)
}

func botVaultUnknownOnlyReconciles(t *testing.T, open vaultOpener) {
	s, clock := open(t)
	ctx := context.Background()
	r, err := s.ReserveBotVault(ctx, "p1", "vault-u", "owner-1", botVaultTTL)
	require.NoError(t, err)
	// 标记已 POST 后即使 fence 到期，接管者拿到的意图也必须带着 PostAttempted 语义：
	// 这里验证存储层记录在 unknown 后不会被同 owner 重新 POST（协调器读 PostAttempted）。
	_, err = s.MarkBotVaultPosted(ctx, "p1", "vault-u", "owner-1", r.Fence)
	require.NoError(t, err)

	// fence 到期后接管：PostAttempted 必须重置为 false 还是保留？语义上接管意味着
	// 旧 owner 的 POST 结果未知，新 owner 仍只能先查找；存储接管后保持 PostAttempted=true，
	// 由协调器统一走「只查找」分支，避免第二个 POST 重建。
	clock.Advance(botVaultTTL + time.Second)
	taken, err := s.ReserveBotVault(ctx, "p1", "vault-u", "owner-2", botVaultTTL)
	require.NoError(t, err)
	require.True(t, taken.PostAttempted, "接管在途意图后仍只能查找，不能再 POST")

	// 未知结果期间不能 Reject（会解除意图导致重建）：接管者自己的 fence 可以解除，
	// 但旧 owner 的 fence 已失效。
	err = s.RejectBotVault(ctx, "p1", "vault-u", "owner-1", r.Fence)
	require.ErrorIs(t, err, core.ErrLeaseLost)
}

func botVaultReject(t *testing.T, open vaultOpener) {
	s, _ := open(t)
	ctx := context.Background()
	r, err := s.ReserveBotVault(ctx, "p1", "vault-r", "owner-1", botVaultTTL)
	require.NoError(t, err)

	// 别人的 fence 不能解除。
	err = s.RejectBotVault(ctx, "p1", "vault-r", "owner-2", r.Fence)
	require.ErrorIs(t, err, core.ErrLeaseLost)

	// 自己解除后可以重新 Reserve。
	require.NoError(t, s.RejectBotVault(ctx, "p1", "vault-r", "owner-1", r.Fence))
	got, err := s.GetBotVaultReservation(ctx, "p1", "vault-r")
	require.NoError(t, err)
	require.Nil(t, got)
	again, err := s.ReserveBotVault(ctx, "p1", "vault-r", "owner-1", botVaultTTL)
	require.NoError(t, err)
	require.Equal(t, uint64(1), again.Fence)

	// confirmed 记录不能被 Reject（不引入删除 winner Vault 的路径）。
	_, err = s.MarkBotVaultPosted(ctx, "p1", "vault-r", "owner-1", again.Fence)
	require.NoError(t, err)
	_, err = s.ConfirmBotVault(ctx, "p1", "vault-r", "owner-1", again.Fence, "vlt_9")
	require.NoError(t, err)
	err = s.RejectBotVault(ctx, "p1", "vault-r", "owner-1", again.Fence)
	require.ErrorIs(t, err, core.ErrConflict)
	still, err := s.GetBotVaultReservation(ctx, "p1", "vault-r")
	require.NoError(t, err)
	require.Equal(t, core.BotVaultIntentConfirmed, still.State)
}

func botVaultPrincipalIsolation(t *testing.T, open vaultOpener) {
	s, _ := open(t)
	ctx := context.Background()
	// 不同 principal 的同名 Vault 互不影响。
	r1, err := s.ReserveBotVault(ctx, "acct-a", "same-name", "owner-1", botVaultTTL)
	require.NoError(t, err)
	r2, err := s.ReserveBotVault(ctx, "acct-b", "same-name", "owner-1", botVaultTTL)
	require.NoError(t, err)
	require.NotEqual(t, r1.Key(), r2.Key())
	_, err = s.ConfirmBotVault(ctx, "acct-a", "same-name", "owner-1", r1.Fence, "vlt_a")
	require.NoError(t, err)
	_, err = s.ConfirmBotVault(ctx, "acct-b", "same-name", "owner-1", r2.Fence, "vlt_b")
	require.NoError(t, err)
	a, err := s.GetBotVaultReservation(ctx, "acct-a", "same-name")
	require.NoError(t, err)
	b, err := s.GetBotVaultReservation(ctx, "acct-b", "same-name")
	require.NoError(t, err)
	require.Equal(t, "vlt_a", a.VaultID)
	require.Equal(t, "vlt_b", b.VaultID)
}

func botVaultFenceRenewal(t *testing.T, open vaultOpener) {
	s, clock := open(t)
	ctx := context.Background()
	r, err := s.ReserveBotVault(ctx, "p1", "vault-f", "owner-1", botVaultTTL)
	require.NoError(t, err)
	clock.Advance(botVaultTTL - time.Second)
	renewed, err := s.RenewBotVaultFence(ctx, "p1", "vault-f", "owner-1", r.Fence, botVaultTTL)
	require.NoError(t, err)
	// 续约后原到期时刻仍未过期。
	clock.Advance(2 * time.Second)
	require.True(t, renewed.Active(clock.Now()))

	// 错误 fence / 不存在记录不能续约。
	_, err = s.RenewBotVaultFence(ctx, "p1", "vault-f", "owner-1", r.Fence+1, botVaultTTL)
	require.ErrorIs(t, err, core.ErrLeaseLost)
	_, err = s.RenewBotVaultFence(ctx, "p1", "missing", "owner-1", 1, botVaultTTL)
	require.ErrorIs(t, err, core.ErrLeaseLost)

	// 无效入参。
	_, err = s.ReserveBotVault(ctx, "", "n", "o", botVaultTTL)
	require.ErrorIs(t, err, core.ErrInvalid)
	_, err = s.ReserveBotVault(ctx, "p", "n", "o", 0)
	require.ErrorIs(t, err, core.ErrInvalid)
}
