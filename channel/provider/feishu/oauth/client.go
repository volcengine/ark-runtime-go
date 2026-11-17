// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package oauth 实现飞书稳定版 Device OAuth 的单次 HTTP 协议调用。
package oauth

import (
	"net/http"
	"time"
)

// Config 显式注入应用、可信服务地址、HTTP client 和时钟。
type Config struct {
	AppID           string
	AppSecret       string
	AccountsBaseURL string
	OpenBaseURL     string
	HTTPClient      *http.Client
	Clock           func() time.Time
}

// Client 固定应用身份，不持有用户凭据或轮询状态。
type Client struct {
	appID, appSecret, accountsBaseURL, openBaseURL string
	httpClient                                     *http.Client
	clock                                          func() time.Time
}

// ErrorKind 表示可安全诊断的失败分类。
type ErrorKind string

const (
	// ErrorNetwork 表示传输失败。
	ErrorNetwork ErrorKind = "network"
	// ErrorUpstream 表示上游故障。
	ErrorUpstream ErrorKind = "upstream"
	// ErrorInvalidResponse 表示响应无法证明协议结果。
	ErrorInvalidResponse ErrorKind = "invalid_response"
	// ErrorReauthRequired 表示需要用户重新授权。
	ErrorReauthRequired ErrorKind = "reauth_required"
	// ErrorConfiguration 表示应用配置无效。
	ErrorConfiguration ErrorKind = "configuration"
	// ErrorPermission 表示权限不足。
	ErrorPermission ErrorKind = "permission"
	// ErrorRateLimit 表示限流。
	ErrorRateLimit ErrorKind = "rate_limit"
	// ErrorCancelled 表示上下文已取消。
	ErrorCancelled ErrorKind = "cancelled"
)

// Outcome 表示是否能确认本次请求被拒绝。
type Outcome string

const (
	// OutcomeUnknown 表示不能证明交换结果，调用方不得盲目重放。
	OutcomeUnknown Outcome = "unknown"
	// OutcomeRejected 表示上游明确拒绝。
	OutcomeRejected Outcome = "rejected"
)

// Error 只携带安全枚举与数字，不保留原响应、URL 或传输错误。
type Error struct {
	Kind       ErrorKind
	Outcome    Outcome
	HTTPStatus int
	Code       int
	cause      error
}

func (e *Error) Error() string { return safeErrorText(e) }

// Unwrap 仅暴露上下文取消原因。
func (e *Error) Unwrap() error { return e.cause }

// DeviceAuthorization 保存待用户确认的 Device 授权信息。
type DeviceAuthorization struct {
	DeviceCode, UserCode, VerificationURL string
	ExpiresAt                             time.Time
	Interval                              time.Duration
}

// PollStatus 表示单次查询的明确授权状态。
type PollStatus string

const (
	// PollPending 表示等待用户确认。
	PollPending PollStatus = "pending"
	// PollSlowDown 表示调用方应将轮询间隔增加五秒。
	PollSlowDown PollStatus = "slow_down"
	// PollSucceeded 表示授权成功。
	PollSucceeded PollStatus = "succeeded"
	// PollDenied 表示用户拒绝。
	PollDenied PollStatus = "denied"
	// PollExpired 表示授权码已过期。
	PollExpired PollStatus = "expired"
)

// PollResult 携带单次查询状态，仅成功时包含 Token。
type PollResult struct {
	Status PollStatus
	Token  *Tokens
}

// Tokens 区分实际授予 scope 与 refresh token 是否存在。
type Tokens struct {
	AccessToken, RefreshToken string
	ExpiresAt                 time.Time
	GrantedScopes             []string
	ScopeKnown                bool
}

// Identity 的 AppID 是冻结配置的绑定上下文，OpenID/TenantKey 由上游证明。
type Identity struct{ AppID, OpenID, TenantKey string }
