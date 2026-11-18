// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package userauth 将飞书 Device 单次协议编排为加密持久、不可盲重放的本人授权。
package userauth

import (
	"context"
	"crypto/rand"
	"errors"
	"slices"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
	"github.com/volcengine/ark-runtime-go/channel/provider/feishu/oauth"
)

// Config 显式提供协议、公开凭据、持久 Store 与生产加密实现。
type Config struct {
	AppID        string
	OAuth        *oauth.Client
	Credentials  core.UserCredentialWriter
	Store        core.UserAuthStore
	Codec        codec.Codec
	Clock        func() time.Time
	AllowedHosts []string
}

// Authorizer 拥有 Device/token 生命周期；授权 lease 的最终状态由 Runtime 提交。
type Authorizer struct {
	config Config
	owner  string
}

// New 不访问网络，不读取本机用户凭据，不生成替代加密密钥。
func New(config Config) (*Authorizer, error) {
	if config.AppID == "" || config.OAuth == nil || config.Credentials == nil || config.Store == nil || config.Codec == nil {
		return nil, errConfiguration
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if len(config.AllowedHosts) == 0 {
		config.AllowedHosts = []string{"open.feishu.cn"}
	}
	for _, host := range config.AllowedHosts {
		if host != "open.feishu.cn" && host != "open.larksuite.com" {
			return nil, errConfiguration
		}
	}
	config.AllowedHosts = slices.Clone(config.AllowedHosts)
	return &Authorizer{config: config, owner: "device-refresh-" + rand.Text()}, nil
}

var errConfiguration = errors.New("device authorization configuration invalid")
var errState = errors.New("device authorization state unavailable")

// Start 只发起一次链接并先持久保存；响应未知允许 Runtime 失败后新 attempt。
func (a *Authorizer) Start(ctx context.Context, p core.UserPrincipal, request core.AuthRequest) (core.AuthFlow, error) {
	r, err := a.held(ctx, core.AuthFlowRef{Principal: p, VaultID: request.VaultID, Lease: request.Lease, Revision: request.Revision})
	if err != nil {
		return core.AuthFlow{}, err
	}
	if r.FlowID != "" || r.State != core.UserAuthVerifying || r.Operation != core.UserAuthAuthorize || r.Attempt != request.Attempt || stableID(r) != request.ExistingCredentialID {
		return core.AuthFlow{}, errState
	}
	if err = ctx.Err(); err != nil {
		return core.AuthFlow{}, errState
	}
	result, err := a.config.OAuth.Begin(ctx, request.RequiredScopes)
	if err != nil {
		return core.AuthFlow{}, err
	}
	deadline := result.ExpiresAt
	if !r.ExpiresAt.IsZero() && r.ExpiresAt.Before(deadline) {
		deadline = r.ExpiresAt
	}
	flow := core.AuthFlow{ID: "device-" + rand.Text(), URL: result.VerificationURL, UserCode: result.UserCode, ExpiresAt: deadline}
	r.FlowID = flow.ID
	secret := deviceSecret{Flow: flow, DeviceCode: result.DeviceCode, Interval: result.Interval, NextPollAt: a.config.Clock().Add(result.Interval), Phase: phasePending}
	if _, err = a.checkpoint(r, secret, core.UserAuthPending, ""); err != nil {
		return core.AuthFlow{}, err
	}
	return flow, nil
}

// AuthPrompt 从本次 flow 的密文恢复原提示，整个方法不发 HTTP 或更新 Store。
func (a *Authorizer) AuthPrompt(ctx context.Context, ref core.AuthFlowRef) (core.AuthFlow, error) {
	r, err := a.record(ctx, ref.Principal, ref.VaultID)
	if err != nil || r == nil || r.FlowID != ref.FlowID {
		return core.AuthFlow{}, errState
	}
	secret, err := a.decrypt(ctx, *r)
	if err != nil {
		return core.AuthFlow{}, err
	}
	return secret.Flow, nil
}

// Status 在 NextPollAt 到期时只查询一次；未完成的 HTTP intent 永远不自动重放。
func (a *Authorizer) Status(ctx context.Context, ref core.AuthFlowRef) (core.AuthStatus, error) {
	r, err := a.held(ctx, ref)
	if err != nil {
		return core.AuthStatus{}, err
	}
	secret, err := a.decrypt(ctx, r)
	if err != nil {
		return core.AuthStatus{}, err
	}
	if uncertainPhase(secret.Phase) {
		return core.AuthStatus{Status: core.AuthUnknown}, nil
	}
	if !a.config.Clock().Before(r.ExpiresAt) {
		return core.AuthStatus{Status: core.AuthExpired}, nil
	}
	if secret.Phase == phasePending {
		if a.config.Clock().Before(secret.NextPollAt) {
			return pending(secret), nil
		}
		if ctx.Err() != nil {
			return core.AuthStatus{}, errState
		}
		secret.Phase = phasePollIntent
		r, err = a.checkpoint(r, secret, core.UserAuthVerifying, "")
		if err != nil {
			return core.AuthStatus{}, err
		}
		result, pollErr := a.config.OAuth.Poll(ctx, secret.DeviceCode)
		if pollErr != nil {
			return a.exchangeError(r, secret, pollErr)
		}
		switch result.Status {
		case oauth.PollPending, oauth.PollSlowDown:
			if result.Status == oauth.PollSlowDown {
				secret.Interval += 5 * time.Second
			}
			secret.Phase = phasePending
			secret.NextPollAt = a.config.Clock().Add(secret.Interval)
			_, err = a.checkpoint(r, secret, core.UserAuthPending, "")
			return pending(secret), err
		case oauth.PollDenied:
			return core.AuthStatus{Status: core.AuthDenied}, nil
		case oauth.PollExpired:
			return core.AuthStatus{Status: core.AuthExpired}, nil
		case oauth.PollSucceeded:
			secret.Tokens = *result.Token
			secret.Phase = phaseTokens
			r, err = a.checkpoint(r, secret, core.UserAuthVerifying, "")
			if err != nil {
				return core.AuthStatus{}, err
			}
		default:
			return a.unknown(r, secret)
		}
	}
	return a.verifyAndSync(ctx, r, secret, ref.RequiredScopes)
}

func (a *Authorizer) verifyAndSync(ctx context.Context, r core.UserAuthRecord, secret deviceSecret, scopes []string) (core.AuthStatus, error) {
	var err error
	if !a.config.Clock().Before(secret.Tokens.ExpiresAt) {
		return core.AuthStatus{Status: core.AuthReauthRequired}, nil
	}
	if secret.Phase == phaseTokens {
		identity, identityErr := a.config.OAuth.GetUserIdentity(ctx, secret.Tokens.AccessToken)
		if identityErr != nil {
			return core.AuthStatus{}, identityErr
		}
		if identity.AppID != r.Principal.AppID || identity.OpenID != r.Principal.OpenID || identity.TenantKey != r.Principal.TenantKey {
			return core.AuthStatus{Status: core.AuthSubjectMismatch}, nil
		}
		if !hasScopes(secret.Tokens, scopes) {
			return core.AuthStatus{Status: core.AuthReauthRequired}, nil
		}
		secret.Phase = phaseSync
		r, err = a.checkpoint(r, secret, core.UserAuthVerifying, "")
		if err != nil {
			return core.AuthStatus{}, err
		}
	}
	if !hasScopes(secret.Tokens, scopes) {
		return core.AuthStatus{Status: core.AuthReauthRequired}, nil
	}
	if !a.config.Clock().Before(secret.Tokens.ExpiresAt) {
		return core.AuthStatus{Status: core.AuthReauthRequired}, nil
	}
	if secret.Phase == phaseSync {
		if ctx.Err() != nil {
			return core.AuthStatus{}, errState
		}
		secret.Phase = phaseSyncIntent
		r, err = a.checkpoint(r, secret, core.UserAuthVerifying, "")
		if err != nil {
			return core.AuthStatus{}, err
		}
		id, syncErr := a.config.Credentials.SyncUserCredential(ctx, core.UserCredentialTarget{Principal: r.Principal, VaultID: r.VaultID, CredentialID: stableID(r), AllowedHosts: a.config.AllowedHosts}, secret.Tokens.AccessToken)
		if syncErr != nil {
			var unsent core.RequestNotSent
			if errors.As(syncErr, &unsent) && unsent.RequestNotSent() {
				secret.Phase = phaseSync
				_, err = a.checkpoint(r, secret, core.UserAuthVerifying, "")
				if err != nil {
					return core.AuthStatus{}, err
				}
				return core.AuthStatus{}, syncErr
			}
			return a.unknown(r, secret)
		}
		if id == "" || stableID(r) != "" && id != stableID(r) {
			return a.unknown(r, secret)
		}
		secret.Phase = phaseSynced
		r, err = a.checkpoint(r, secret, core.UserAuthVerifying, id)
		if err != nil {
			return core.AuthStatus{}, err
		}
	}
	if secret.Phase != phaseSynced || stableID(r) == "" {
		return core.AuthStatus{}, errState
	}
	if !a.config.Clock().Before(secret.Tokens.ExpiresAt) {
		return core.AuthStatus{Status: core.AuthReauthRequired}, nil
	}
	version := r.FlowID
	if r.Operation == core.UserAuthRefresh {
		version = r.Grant.GrantVersion
	}
	return core.AuthStatus{Status: core.AuthReady, Grant: core.UserGrant{Status: core.AuthReady, VaultID: r.VaultID, CredentialID: stableID(r), FlowID: r.FlowID, GrantVersion: version, GrantedScopes: slices.Clone(secret.Tokens.GrantedScopes), ExpiresAt: secret.Tokens.ExpiresAt}}, nil
}
func (a *Authorizer) exchangeError(r core.UserAuthRecord, secret deviceSecret, err error) (core.AuthStatus, error) {
	var failure *oauth.Error
	if errors.As(err, &failure) && failure.Outcome == oauth.OutcomeRejected {
		if failure.Kind == oauth.ErrorReauthRequired {
			return core.AuthStatus{Status: core.AuthReauthRequired}, nil
		}
		if failure.Kind == oauth.ErrorPermission {
			return core.AuthStatus{Status: core.AuthDenied}, nil
		}
		// 明确拒绝不留下可能已交换成功的 intent，但不在同一调用重试。
		secret.Phase = phasePending
		secret.NextPollAt = a.config.Clock().Add(secret.Interval)
		if r.Operation == core.UserAuthRefresh {
			return core.AuthStatus{Status: core.AuthFailed}, nil
		}
		_, saveErr := a.checkpoint(r, secret, core.UserAuthPending, "")
		return pending(secret), saveErr
	}
	return a.unknown(r, secret)
}
func (a *Authorizer) unknown(r core.UserAuthRecord, secret deviceSecret) (core.AuthStatus, error) {
	secret.Phase = phaseUnknown
	_, err := a.checkpoint(r, secret, core.UserAuthVerifying, "")
	return core.AuthStatus{Status: core.AuthUnknown}, err
}
func pending(s deviceSecret) core.AuthStatus {
	return core.AuthStatus{Status: core.AuthPending, NextPollAt: s.NextPollAt, ExpiresAt: s.Flow.ExpiresAt}
}
func hasScopes(t oauth.Tokens, scopes []string) bool {
	if !t.ScopeKnown {
		return false
	}
	for _, scope := range scopes {
		if !slices.Contains(t.GrantedScopes, scope) {
			return false
		}
	}
	return true
}
func stableID(r core.UserAuthRecord) string {
	if r.CredentialID != "" {
		return r.CredentialID
	}
	return r.Grant.CredentialID
}
func (a *Authorizer) record(ctx context.Context, p core.UserPrincipal, vault string) (*core.UserAuthRecord, error) {
	if !p.Valid() || p.AppID != a.config.AppID || vault == "" {
		return nil, errState
	}
	r, err := a.config.Store.GetUserAuth(ctx, p)
	if err != nil {
		return nil, errState
	}
	if r != nil && (r.VaultID != vault || r.Principal != p) {
		return nil, errState
	}
	return r, nil
}
func (a *Authorizer) held(ctx context.Context, ref core.AuthFlowRef) (core.UserAuthRecord, error) {
	r, err := a.record(ctx, ref.Principal, ref.VaultID)
	if err != nil || r == nil || r.FlowID != ref.FlowID || r.Revision != ref.Revision || r.Lease != ref.Lease || !r.Lease.Live(a.config.Clock()) {
		return core.UserAuthRecord{}, errState
	}
	return *r, nil
}

// String 返回不含配置引用和密文的安全视图。
func (a Authorizer) String() string { return "DeviceUserAuthorizer{redacted}" }

// GoString 返回安全的 Go 格式视图。
func (a Authorizer) GoString() string { return a.String() }

// String 返回不含依赖内部状态的安全视图。
func (c Config) String() string { return "DeviceUserAuthConfig{redacted}" }

// GoString 返回安全的配置视图。
func (c Config) GoString() string { return c.String() }
