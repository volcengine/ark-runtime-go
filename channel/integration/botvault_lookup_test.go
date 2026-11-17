// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// unavailableVaultList 模拟账号 Vault 数量超过列表扫描上限，单点读取仍然可用。
type unavailableVaultList struct{ *idVaultProvisioner }

func (p *unavailableVaultList) FindVaultByName(context.Context, string) (core.VaultRef, error) {
	return core.VaultRef{}, errors.New("ma vault list too long")
}

func TestBotVaultCreateAndReuseDoNotNeedList(t *testing.T) {
	p := &unavailableVaultList{&idVaultProvisioner{fakeProvisioner: newFakeProvisioner()}}
	c := core.NewBotVaultCoordinator(botVaultTestStore(t), p, "owner")
	ctx := context.Background()
	spec := core.BotVaultCreate{Name: "stable-key", DisplayName: "Channel User"}
	created, err := c.EnsureVault(ctx, "principal", spec, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "vlt_stable-key", created.ID)
	reused, err := c.EnsureVault(ctx, "principal", spec, time.Minute)
	require.NoError(t, err)
	require.Equal(t, created.ID, reused.ID)
	require.Equal(t, 1, p.postCount())
	require.Equal(t, 1, p.gets)
}
