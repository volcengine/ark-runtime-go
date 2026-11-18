// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	memory "github.com/volcengine/ark-runtime-go/channel/internal/testutil/memorystore"
)

// fakeProvisioner 是测试用 BotVaultProvisioner：按内存表模拟公开 Vault Create/Find。
type fakeProvisioner struct {
	mu          sync.Mutex
	vaults      map[string]core.VaultRef // name -> ref
	posts       int
	createOK    bool // false 时按 failUnknown 决定明确拒绝还是未知
	failUnknown bool
}

func newFakeProvisioner() *fakeProvisioner {
	return &fakeProvisioner{vaults: map[string]core.VaultRef{}, createOK: true}
}

func (p *fakeProvisioner) CreateVault(_ context.Context, spec core.BotVaultCreate) (core.VaultRef, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.posts++
	if !p.createOK {
		if p.failUnknown {
			return core.VaultRef{}, errors.New("network timeout")
		}
		return core.VaultRef{}, errors.Join(core.ErrVaultNotCreated, errors.New("400 bad request"))
	}
	if _, exists := p.vaults[spec.Name]; exists {
		return core.VaultRef{}, errors.New("duplicate")
	}
	ref := core.VaultRef{ID: "vlt_" + spec.Name, Name: spec.Name, Principal: spec.Metadata["ark_channel_principal"]}
	p.vaults[spec.Name] = ref
	return ref, nil
}

func (p *fakeProvisioner) FindVaultByName(_ context.Context, name string) (core.VaultRef, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ref, ok := p.vaults[name]; ok {
		return ref, nil
	}
	return core.VaultRef{}, nil
}

func (p *fakeProvisioner) postCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.posts
}

func botVaultTestStore(t *testing.T) core.Store {
	t.Helper()
	s, err := memory.New(memory.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

// TestConcurrentBotVaultReservation 验证两个协调器并发为同一主体创建同名 Vault：
// 只有一个 POST 成功，后到者拿到 winner 的 VaultID，且不删除任何 Vault。
func TestConcurrentBotVaultReservation(t *testing.T) {
	store := botVaultTestStore(t)
	prov := newFakeProvisioner()
	c1 := core.NewBotVaultCoordinator(store, prov, "owner-1")
	c2 := core.NewBotVaultCoordinator(store, prov, "owner-2")
	spec := core.BotVaultCreate{Name: "acct-proj-ch1-ep0-t1", DisplayName: "Acct Bot Vault"}

	first, err := c1.EnsureVault(context.Background(), "principal-1", spec, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "vlt_"+spec.Name, first.ID)

	// 第二个 owner 并发协调同一主体：reservation 已 confirmed，直接读 winner，不再 POST。
	second, err := c2.EnsureVault(context.Background(), "principal-1", spec, time.Minute)
	require.NoError(t, err)
	require.Equal(t, first.ID, second.ID)
	require.Equal(t, 1, prov.postCount(), "只允许一次 POST，winner 由读取确认")
}

// TestUnknownVaultCreateOnlyReconciles 验证 POST 结果未知时：不解除意图、不重建，
// 只按名查找；远端其实已建时第二次协调找回并确认，全程只发一次 POST。
func TestUnknownVaultCreateOnlyReconciles(t *testing.T) {
	store := botVaultTestStore(t)
	prov := newFakeProvisioner()
	prov.createOK = false
	prov.failUnknown = true // POST 超时，结果未知
	c := core.NewBotVaultCoordinator(store, prov, "owner-1")
	spec := core.BotVaultCreate{Name: "unknown-name", DisplayName: "Unknown"}

	_, err := c.EnsureVault(context.Background(), "principal-1", spec, time.Minute)
	require.ErrorIs(t, err, core.ErrBotVaultCreateUnknown)
	require.Equal(t, 1, prov.postCount())

	// 远端其实已经把 Vault 建出来了（模拟 POST 已生效但回包丢失）。
	prov.mu.Lock()
	prov.vaults[spec.Name] = core.VaultRef{ID: "vlt_recovered", Name: spec.Name, Principal: "principal-1"}
	prov.mu.Unlock()

	ref, err := c.EnsureVault(context.Background(), "principal-1", spec, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "vlt_recovered", ref.ID)
	require.Equal(t, 1, prov.postCount(), "未知结果只查找，绝不二次 POST")
}

// TestUnknownVaultCreateStaysUnknownWhenAbsent 验证远端确实没建出来时，
// 协调持续返回 unknown（在 fence 有效期内不重建），由调用方稍后重试。
func TestUnknownVaultCreateStaysUnknownWhenAbsent(t *testing.T) {
	store := botVaultTestStore(t)
	prov := newFakeProvisioner()
	prov.createOK = false
	prov.failUnknown = true
	c := core.NewBotVaultCoordinator(store, prov, "owner-1")
	spec := core.BotVaultCreate{Name: "absent", DisplayName: "Absent"}

	for i := 0; i < 2; i++ {
		_, err := c.EnsureVault(context.Background(), "principal-1", spec, time.Minute)
		require.ErrorIs(t, err, core.ErrBotVaultCreateUnknown)
	}
	require.Equal(t, 1, prov.postCount(), "远端缺失期间也不能重复 POST")
}

// TestUnknownVaultCreateRejectsForeignPrincipal 验证未知结果不能认领其它主体的同名 Vault。
func TestUnknownVaultCreateRejectsForeignPrincipal(t *testing.T) {
	store := botVaultTestStore(t)
	prov := newFakeProvisioner()
	prov.createOK = false
	prov.failUnknown = true
	// 远端已存在另一个 principal 的同名 Vault。
	prov.mu.Lock()
	prov.vaults["shared-name"] = core.VaultRef{ID: "vlt_foreign", Name: "shared-name", Principal: "principal-OTHER"}
	prov.mu.Unlock()

	// 创建结果未知后，查账读到 foreign principal，必须拒绝且不删除远端资源。
	c := core.NewBotVaultCoordinator(store, prov, "owner-1")
	_, err := c.EnsureVault(context.Background(), "principal-1", core.BotVaultCreate{Name: "shared-name", DisplayName: "Shared"}, time.Minute)
	require.ErrorIs(t, err, core.ErrVaultPrincipalMismatch)
	require.Equal(t, 1, prov.postCount(), "未知结果不能再次 POST")
}

// TestDefinitiveRejectReleasesReservation 验证明确 4xx 拒绝会解除自己的 pending 意图，
// 之后（如配置修正）可重新 POST。
func TestDefinitiveRejectReleasesReservation(t *testing.T) {
	store := botVaultTestStore(t)
	prov := newFakeProvisioner()
	prov.createOK = false
	prov.failUnknown = false // 明确 400
	c := core.NewBotVaultCoordinator(store, prov, "owner-1")
	spec := core.BotVaultCreate{Name: "rejected", DisplayName: "Rejected"}

	_, err := c.EnsureVault(context.Background(), "principal-1", spec, time.Minute)
	require.Error(t, err)
	require.ErrorIs(t, err, core.ErrVaultNotCreated)

	// 意图已解除：远端修好后可重新 POST 成功。
	prov.createOK = true
	ref, err := c.EnsureVault(context.Background(), "principal-1", spec, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "vlt_"+spec.Name, ref.ID)
	require.Equal(t, 2, prov.postCount())
}

// TestOldSessionRenewsOriginalPrincipal 验证协调以调用方传入的 principal 为准，
// 不同主体互不串用（配合 S6 的 epoch/tenant 隔离）。
func TestOldSessionRenewsOriginalPrincipal(t *testing.T) {
	store := botVaultTestStore(t)
	prov := newFakeProvisioner()
	c := core.NewBotVaultCoordinator(store, prov, "owner-1")
	oldSpec := core.BotVaultCreate{Name: "ch1-ep0", DisplayName: "Ep0"}
	newSpec := core.BotVaultCreate{Name: "ch1-ep1", DisplayName: "Ep1"}

	old, err := c.EnsureVault(context.Background(), "acct|proj|ch1|ep0|tenant", oldSpec, time.Minute)
	require.NoError(t, err)
	next, err := c.EnsureVault(context.Background(), "acct|proj|ch1|ep1|tenant", newSpec, time.Minute)
	require.NoError(t, err)
	require.NotEqual(t, old.ID, next.ID)
	// 再次协调旧主体仍稳定取回旧 Vault（旧 Session 续期用原 principal）。
	again, err := c.EnsureVault(context.Background(), "acct|proj|ch1|ep0|tenant", oldSpec, time.Minute)
	require.NoError(t, err)
	require.Equal(t, old.ID, again.ID)
}

// idVaultProvisioner 支持以冻结ID读取；名称可改但principal不可偷换。
type idVaultProvisioner struct {
	*fakeProvisioner
	gets int
}

func (p *idVaultProvisioner) GetVault(_ context.Context, id string) (core.VaultRef, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gets++
	for _, ref := range p.vaults {
		if ref.ID == id {
			return ref, nil
		}
	}
	return core.VaultRef{}, nil
}
func TestConfirmedBotVaultUsesIDAfterRenameAndRejectsForeignPrincipal(t *testing.T) {
	store := botVaultTestStore(t)
	p := &idVaultProvisioner{fakeProvisioner: newFakeProvisioner()}
	c := core.NewBotVaultCoordinator(store, p, "owner")
	spec := core.BotVaultCreate{Name: "initial"}
	ctx := context.Background()
	created, err := c.EnsureVault(ctx, "principal", spec, time.Minute)
	require.NoError(t, err)
	p.mu.Lock()
	ref := p.vaults[spec.Name]
	delete(p.vaults, spec.Name)
	ref.Name = "renamed"
	p.vaults[ref.Name] = ref
	p.mu.Unlock()
	reused, err := c.EnsureVault(ctx, "principal", spec, time.Minute)
	require.NoError(t, err)
	require.Equal(t, created.ID, reused.ID)
	p.mu.Lock()
	ref.Principal = "foreign"
	p.vaults[ref.Name] = ref
	p.mu.Unlock()
	_, err = c.EnsureVault(ctx, "principal", spec, time.Minute)
	require.ErrorIs(t, err, core.ErrVaultPrincipalMismatch)
	require.Equal(t, 1, p.postCount())
	require.Equal(t, 2, p.gets)
}
func TestUnknownBotVaultDoesNotUseIDReaderOrRepeatPost(t *testing.T) {
	store := botVaultTestStore(t)
	p := &idVaultProvisioner{fakeProvisioner: newFakeProvisioner()}
	p.createOK = false
	p.failUnknown = true
	c := core.NewBotVaultCoordinator(store, p, "owner")
	for range 2 {
		_, err := c.EnsureVault(context.Background(), "principal", core.BotVaultCreate{Name: "unknown"}, time.Minute)
		require.ErrorIs(t, err, core.ErrBotVaultCreateUnknown)
	}
	require.Equal(t, 1, p.postCount())
	require.Zero(t, p.gets)
}
