// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"strings"
	"time"
)

// 执行身份。bot：Agent 以 Bot 应用身份调用飞书 OpenAPI；user：以触发者的用户授权调用。
const (
	IdentityNone = ""
	IdentityBot  = "bot"
	IdentityUser = "user"
)

// BotCredentialSpec 描述写入 Vault 的 Bot 执行凭据：一条 environment_variable 凭据，
// SecretName 固定（如 FEISHU_TENANT_ACCESS_TOKEN），AllowedHosts 限定出网目标。
type BotCredentialSpec struct {
	VaultID      string
	SecretName   string
	AllowedHosts []string
}

// VaultClient 是 MAClient 可选实现的公开 Vault 凭据端口（POST/GET /vaults/:id/credentials）。
// 实现只写入、不回读明文；返回的 credential ID 用于后续原位更新。
type VaultClient interface {
	// UpsertEnvCredential 按 SecretName 创建或原位更新 environment_variable 凭据，返回凭据 ID。
	UpsertEnvCredential(ctx context.Context, spec BotCredentialSpec, value string) (string, error)
}

// BotVaultProvisioner 是公开 Vault 容器端口（POST /vaults、GET /vaults）。
// 由 MAClient 可选实现；与 BotVaultStore 配合完成 per-principal 创建协调。
// 方法只在 coordinator 持持久 reservation 时调用，绝不删除任何 Vault。
type BotVaultProvisioner interface {
	// CreateVault 按确定名称创建 Vault 容器，返回远端 Vault ID 与名称。
	CreateVault(ctx context.Context, spec BotVaultCreate) (VaultRef, error)
	// FindVaultByName 按名称查找当前账号下的 Vault；未找到返回空 VaultID 与 nil 错误。
	FindVaultByName(ctx context.Context, name string) (VaultRef, error)
}

// VaultPrincipalFinder 按主体和创建标识精确查账；只用于未知创建结果的恢复。
// 旧 provider 可继续用 FindVaultByName，但不得把账号全量扫描当作精确查账。
type VaultPrincipalFinder interface {
	FindVaultByPrincipal(ctx context.Context, principal, name string) (VaultRef, error)
}

// BotVaultCreate 是创建 Bot Vault 容器的请求（非密，不含 token）。
type BotVaultCreate struct {
	Name        string
	DisplayName string
	// Metadata 写入创建请求，标记托管来源与 principal，供归属校验。
	Metadata map[string]string
}

// VaultRef 是一个远端 Vault 容器的引用。
type VaultRef struct {
	ID   string
	Name string
	// Principal 从 Vault metadata 回读的归属主体；空表示缺失/不可信。
	Principal string
}

// ErrVaultNotCreated 是 provisioner 的明确未受理信号（确定性 4xx）：
// coordinator 据此解除自己的 pending reservation。超时/5xx 属于未知，由 coordinator 自行区分。
var ErrVaultNotCreated = errors.New("vault create definitively rejected")

// BotTokenSource 是 Provider 可选实现的 Bot 执行令牌来源（如飞书 tenant_access_token）。
// 返回令牌与过期时间；令牌只在内存中传递给 VaultClient，不落盘、不进日志。
type BotTokenSource interface {
	BotToken(ctx context.Context) (string, time.Time, error)
}

// botIdentity 维护同一冻结主体和凭据的续期状态。
type botIdentity struct {
	refresh     chan struct{}
	expiresAt   time.Time
	lastErr     error
	attemptedAt time.Time
	refs        int
	lastUsed    time.Time
}

// errIdentityUnavailable 表示执行身份暂不可用（续期失败）；任务以 uncertain 重试。
var errIdentityUnavailable = errors.New("execution identity unavailable")

// botRefreshMargin 是令牌到期前提前续期的余量。
const botRefreshMargin = 10 * time.Minute

// ensureBotIdentity 保证 identity=bot 的连接在创建 Session 前，Vault 中的 Bot 凭据有效。
// 令牌距过期不足 botRefreshMargin 时刷新并原位更新凭据；失败返回 errIdentityUnavailable。
func (r *Runtime) ensureBotIdentity(ctx context.Context, connection Connection, chatType string) error {
	return r.refreshBotIdentity(ctx, connection, chatType, false)
}

// refreshBotIdentity 对后台失败做一个检查间隔的负缓存，前台任务重试不受影响。
func (r *Runtime) refreshBotIdentity(ctx context.Context, connection Connection, chatType string, background bool) error {
	binding := connection.Binding
	if effectiveIdentity(binding, chatType) == IdentityUser && connection.UserAuthorizer != nil {
		return nil
	}
	if err := validateIdentityForChat(binding, chatType); err != nil {
		return err
	}
	if effectiveIdentity(binding, chatType) != IdentityBot {
		return nil
	}
	state := r.botState(botIdentityKey(binding))
	if state == nil {
		return errIdentityUnavailable
	}
	defer r.releaseBotState(state)
	select {
	case state.refresh <- struct{}{}:
		defer func() { <-state.refresh }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	now := r.options.Clock()
	if background && state.lastErr != nil && now.Before(state.attemptedAt.Add(r.options.IdentityRenewalInterval)) {
		return state.lastErr
	}
	if !state.expiresAt.IsZero() && now.Add(botRefreshMargin).Before(state.expiresAt) {
		return nil
	}
	source, ok := providerPort[BotTokenSource](connection.Provider)
	vault, vok := connection.MAClient.(VaultClient)
	if !ok || !vok || binding.BotCredential.VaultID == "" {
		return errIdentityUnavailable
	}
	callCtx, cancel := context.WithTimeout(ctx, r.options.DeliveryTimeout*5)
	defer cancel()
	state.attemptedAt = now
	token, expiresAt, err := source.BotToken(callCtx)
	if isStoreFailure(err) {
		return err
	}
	if err != nil || token == "" {
		state.lastErr = errIdentityUnavailable
		r.report(ctx, Observation{Name: EventAuth, Status: "failed", ErrorKind: "bot_token", BindingID: binding.ID})
		return errIdentityUnavailable
	}
	if _, err := vault.UpsertEnvCredential(callCtx, binding.BotCredential, token); err != nil {
		if isStoreFailure(err) {
			return err
		}
		state.lastErr = errIdentityUnavailable
		r.report(ctx, Observation{Name: EventAuth, Status: "failed", ErrorKind: "vault_write", BindingID: binding.ID})
		return errIdentityUnavailable
	}
	state.expiresAt, state.lastErr = expiresAt, nil
	r.report(ctx, Observation{Name: EventAuth, Status: "refreshed", BindingID: binding.ID})
	return nil
}

// botIdentityCacheLimit 限制进程内元数据，不保留所有历史 epoch；在用状态不被淘汰。
const botIdentityCacheLimit = 1024

func botIdentityKey(binding Binding) string {
	channel := binding.ChannelID
	if channel == "" {
		channel = binding.ID
	}
	return digestJSON(struct {
		Owner, Project, Channel, AppID string
		Epoch                          uint64
		Credential                     BotCredentialSpec
	}{binding.Owner, binding.Project, channel, binding.AppID, binding.BindingEpoch, binding.BotCredential})
}

func (r *Runtime) botState(key string) *botIdentity {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.identities == nil {
		r.identities = make(map[string]*botIdentity)
	}
	state := r.identities[key]
	if state == nil {
		if len(r.identities) >= botIdentityCacheLimit {
			oldestKey := ""
			var oldest time.Time
			for candidate, cached := range r.identities {
				if cached.refs == 0 && (oldestKey == "" || cached.lastUsed.Before(oldest)) {
					oldestKey, oldest = candidate, cached.lastUsed
				}
			}
			if oldestKey == "" {
				return nil
			}
			delete(r.identities, oldestKey)
		}
		state = &botIdentity{refresh: make(chan struct{}, 1)}
		r.identities[key] = state
	}
	state.refs++
	state.lastUsed = r.options.Clock()
	return state
}

func (r *Runtime) releaseBotState(state *botIdentity) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state.refs--
	state.lastUsed = r.options.Clock()
}

// identityLoop 从持久引用恢复动态身份，静态连接继续按配置提前续期。
func (r *Runtime) identityLoop(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
		if r.options.Resolver != nil {
			if err := r.maintainBotIdentities(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				if !isUnsupportedWork(err) {
					return err
				}
				r.report(ctx, Observation{Name: EventWorkBlocked, Status: "blocked", ErrorKind: "unsupported_work"})
			}
		} else {
			for _, connection := range r.connections {
				chatType := "group"
				if connection.Binding.Identity.Group != IdentityBot {
					chatType = "p2p"
				}
				if err := r.refreshBotIdentity(ctx, connection, chatType, true); isStoreFailure(err) {
					return err
				}
			}
		}
		timer.Reset(r.options.IdentityRenewalInterval)
	}
}

// validateIdentityForChat 仅校验当前会话使用的身份，其他会话类型的未开放策略不影响 Bot 执行。
func validateIdentityForChat(binding Binding, chatType string) error {
	if err := binding.Identity.Valid(); err != nil {
		return ErrRuntimeConfig
	}
	binding.Identity = IdentityPolicy{P2P: effectiveIdentity(binding, chatType)}
	return validateIdentity(binding)
}

// validateIdentity 离线检查执行身份配置。user 身份依赖平台 env OAuth 公开接口
// （授权流创建与 authorization_status），现网未开放，启动时明确拒绝而不是静默降级为 bot。
func validateIdentity(binding Binding) error {
	policy := binding.Identity
	if err := policy.Valid(); err != nil {
		return ErrRuntimeConfig
	}
	usesBot := policy.Group == IdentityBot || policy.P2P == IdentityBot
	switch {
	case policy.Group == "" && policy.P2P == "":
		return nil
	case policy.P2P == IdentityUser:
		return ErrIdentityUnsupported
	case usesBot:
		spec := binding.BotCredential
		if strings.TrimSpace(spec.VaultID) == "" || strings.TrimSpace(spec.SecretName) == "" || len(spec.AllowedHosts) == 0 {
			return ErrRuntimeConfig
		}
		for _, id := range binding.VaultIDs {
			if id == spec.VaultID {
				return nil
			}
		}
		// Bot 凭据所在 Vault 必须挂到会话上，否则 Agent 拿不到凭据。
		return ErrRuntimeConfig
	}
	return nil
}

// ErrIdentityUnsupported 表示所选执行身份依赖的平台公开能力尚不可用。
var ErrIdentityUnsupported = errors.New("execution identity requires platform capability not yet available")
