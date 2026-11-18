// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"time"
)

// userPrincipal 从冻结身份和消息中的平台主体构造隔离键，不根据显示名或 API key 推断。
func userPrincipal(b Binding, m Message) UserPrincipal {
	return UserPrincipal{Owner: b.Owner, Project: b.Project, ChannelID: b.ChannelID, Epoch: b.BindingEpoch, AppID: b.AppID, TenantKey: m.TenantKey, OpenID: m.SenderID}
}

func (r *Runtime) userVault(ctx context.Context, c Connection, p UserPrincipal) (string, error) {
	store, ok := r.store.(BotVaultStore)
	provisioner, pok := c.MAClient.(BotVaultProvisioner)
	if !ok || !pok {
		return "", ErrIdentityUnsupported
	}
	coordinator := NewBotVaultCoordinator(store, provisioner, "user-vault-"+r.instanceID)
	ref, err := coordinator.EnsureVault(ctx, p.Key(), BotVaultCreate{Name: "channel-user-" + digestJSON(p), DisplayName: userVaultDisplayName(p)}, time.Minute)
	return ref.ID, err
}

// userVaultDisplayName 使用冻结应用下的 OpenID 区分个人授权容器，独立于创建查账标识。
func userVaultDisplayName(principal UserPrincipal) string {
	return "Channel User · " + principal.OpenID
}
