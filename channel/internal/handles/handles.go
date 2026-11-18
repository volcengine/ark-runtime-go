// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package handles 在公共门面与内部契约间传递不暴露记录的句柄。
package handles

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// Store 只公开生命周期，不提供内部记录或事务接口。
type Store struct{ value core.Store }

// Agent 保存 MA 领域端口。
type Agent struct{ value core.MAClient }

// Provider 保存渠道端口及可选用户授权。
type Provider struct {
	binding    *core.Binding
	bindingKey string
	authStore  *Store
	authAgent  *Agent
	value      core.Provider
	authorizer core.UserAuthorizer
}

// NewStore 包装内置 Store。
func NewStore(s core.Store) *Store { return &Store{value: s} }

// StoreValue 仅供 module 内装配。
func StoreValue(s *Store) core.Store {
	if s == nil {
		return nil
	}
	return s.value
}

// Close 关闭底层 Store；Runtime 不负责调用它。
func (s *Store) Close() error {
	if s == nil || s.value == nil {
		return nil
	}
	return s.value.Close()
}

// NewAgent 包装 MA Adapter。
func NewAgent(a core.MAClient) *Agent { return &Agent{value: a} }

// AgentValue 仅供 module 内装配。
func AgentValue(a *Agent) core.MAClient {
	if a == nil {
		return nil
	}
	return a.value
}

// NewProvider 包装具体渠道实现。
func NewProvider(p core.Provider, a core.UserAuthorizer) *Provider {
	return &Provider{value: p, authorizer: a}
}

// ProviderValue 仅供 module 内装配。
func ProviderValue(p *Provider) core.Provider {
	if p == nil {
		return nil
	}
	return p.value
}

// Authorizer 仅供 module 内装配。
func Authorizer(p *Provider) core.UserAuthorizer {
	if p == nil {
		return nil
	}
	return p.authorizer
}

// BindingKey 指纹只包含调用方的非密 Binding。
func BindingKey(binding any) (string, error) {
	raw, err := json.Marshal(binding)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// NewBoundProvider 保留具体渠道构造时的配置及可选授权依赖。
func NewBoundProvider(p core.Provider, a core.UserAuthorizer, b core.Binding, key string, store *Store, agent *Agent) *Provider {
	return &Provider{value: p, authorizer: a, binding: &b, bindingKey: key, authStore: store, authAgent: agent}
}

// BoundBinding 验证装配时的应用身份和授权依赖没有被替换。
func BoundBinding(p *Provider, key string, store *Store, agent *Agent) (core.Binding, bool, bool) {
	if p == nil || p.binding == nil {
		return core.Binding{}, false, true
	}
	valid := p.bindingKey == key && (p.authStore == nil || p.authStore == store) && (p.authAgent == nil || p.authAgent == agent)
	return *p.binding, true, valid
}

// UserAuthEncryptionReady 验证实际 Store 的加密能力，不暴露 Codec 或持久化记录。
func UserAuthEncryptionReady(store *Store) bool {
	p, ok := StoreValue(store).(interface{ UserAuthEncryptionReady() bool })
	return ok && p.UserAuthEncryptionReady()
}
