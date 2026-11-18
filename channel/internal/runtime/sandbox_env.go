// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"strings"
)

// 飞书沙箱 lark-cli 消费的身份环境变量前缀。这是 Channel 侧保留前缀：
// Hook extras、客户 Environment 配置都不得通过同名键影响身份语义。
const (
	// EnvLarkSuiteAppID 是进程内消费的 AppID；明文写入沙箱 config.env。
	EnvLarkSuiteAppID = "LARKSUITE_CLI_APP_ID"
	// EnvLarkSuiteStrictMode 固定 bot（群聊）或 user（单聊 p2p=user）。
	EnvLarkSuiteStrictMode = "LARKSUITE_CLI_STRICT_MODE"
	// EnvLarkSuiteDefaultAs 与 STRICT_MODE 配套，决定 --as 默认值。
	EnvLarkSuiteDefaultAs = "LARKSUITE_CLI_DEFAULT_AS"
	// EnvLarkSuiteBrand 固定 feishu。
	EnvLarkSuiteBrand = "LARKSUITE_CLI_BRAND"
)

// IsReservedSandboxEnv 判断键名是否属于 Channel 保留的沙箱环境变量。
func IsReservedSandboxEnv(key string) bool {
	return strings.HasPrefix(key, "LARKSUITE_CLI_")
}

// IdentityPolicy 是 Binding 的结构化执行身份：群聊恒为 bot，单聊由 P2P 决定。
// 零值表示不注入任何飞书身份环境；Group 非空时只允许 bot。
type IdentityPolicy struct {
	// Group 是群聊/话题的执行身份，非空时只允许 IdentityBot。
	Group string `json:"group,omitempty"`
	// P2P 是单聊执行身份：空（不注入）、bot 或 user（user 在 S9 前由上层启动校验拒绝）。
	P2P string `json:"p2p,omitempty"`
}

// Valid 校验身份策略取值。
func (p IdentityPolicy) Valid() error {
	switch p.Group {
	case "", IdentityBot:
	default:
		return ErrInvalid
	}
	switch p.P2P {
	case "", IdentityBot, IdentityUser:
	default:
		return ErrInvalid
	}
	return nil
}

// effectiveChat 返回指定会话类型的有效身份；chatType 取值 group/topic/p2p。
func (p IdentityPolicy) effectiveChat(chatType string) string {
	if chatType == "p2p" {
		return p.P2P
	}
	return p.Group
}

// SandboxEnvFor 按设计表产出注入沙箱 config.env 的 LARKSUITE_CLI_* 变量。
// identity 为空或该会话类型未配置身份时返回 nil（不注入）。
// 规则：
//   - 群聊/话题：STRICT_MODE=bot, DEFAULT_AS=bot；
//   - 单聊：按 identity.P2P（bot 或 user）。
func SandboxEnvFor(binding Binding, chatType string) map[string]string {
	identity := effectiveIdentity(binding, chatType)
	if identity == IdentityNone {
		return nil
	}
	env := map[string]string{
		EnvLarkSuiteStrictMode: identity,
		EnvLarkSuiteDefaultAs:  identity,
		EnvLarkSuiteBrand:      "feishu",
	}
	if binding.AppID != "" {
		env[EnvLarkSuiteAppID] = binding.AppID
	}
	return env
}

// effectiveIdentity 返回某会话类型最终的身份值。
func effectiveIdentity(binding Binding, chatType string) string {
	return binding.Identity.effectiveChat(chatType)
}

// EnvironmentConfig 是 EnvironmentReader 返回的基础环境配置；只含身份 env 合并所需字段，
// 不携带 networking/packages/setup_script/tos 等非 env 配置（这些保持原值透传）。
type EnvironmentConfig struct {
	// Type 是config.type（cloud 或 self_hosted）；self_hosted 与身份注入互斥，由上层显式拒绝。
	Type string
	// Env 是客户在 Environment 上配置的明文 env 副本。
	Env map[string]string
}

// EnvironmentReader 是 MAClient 可选实现的环境读取端口（GET /environments/:id）。
type EnvironmentReader interface {
	GetEnvironment(ctx context.Context, id string) (EnvironmentConfig, error)
}

// EnvironmentOverride 是冻结后的 environment_with_overrides 请求：Type 必须等于 base.Type，
// Env 是合并后（base env + LARKSUITE_CLI_*）的完整 env。
type EnvironmentOverride struct {
	Type string
	Env  map[string]string
}

// validateReservedExtras 校验 Hook 追加内容没有使用保留的 env 键（标签沿用 channel_ 前缀规则）。
func validateReservedExtras(extras SessionExtras) error {
	for key := range extras.Env {
		if IsReservedSandboxEnv(key) {
			return errors.New("reserved sandbox env key not allowed in hooks")
		}
	}
	return nil
}
