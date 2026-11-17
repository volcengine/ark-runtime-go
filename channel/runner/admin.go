// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/secret"
)

// openWriter 以运维写入方打开已存在的 Store；embedded 下 Runner 持锁时提示先停机。
func openWriter(ctx context.Context, config Config) (core.Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store, err := openStore(ctx, config, secret.EnvFileResolver{}, modeAdmin)
	if err != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return store, err
}

// ResetConversation 让会话的下一条消息新建 Session；不删除 MA 历史，不中断进行中的 Turn。
func ResetConversation(ctx context.Context, config Config, conversation string) (ConversationStatus, error) {
	if !strings.HasPrefix(conversation, "route:") {
		return ConversationStatus{}, errors.New("conversation key required")
	}
	store, err := openWriter(ctx, config)
	if err != nil {
		return ConversationStatus{}, err
	}
	defer func() { _ = store.Close() }()
	result, err := store.ResetConversation(ctx, core.RouteKey(conversation))
	switch {
	case err != nil:
		return ConversationStatus{}, adminOperationError(ctx, "reset failed")
	case result == nil:
		return ConversationStatus{}, errors.New("conversation has no session binding")
	}
	return ConversationStatus{Conversation: result.ID, State: result.Session.State, Generation: result.Session.Generation}, nil
}

// RetryDelivery 把一条失败投递重新排队；只补投回复，不重新执行 Agent。
func RetryDelivery(ctx context.Context, config Config, id string) (DeliveryStatus, error) {
	if id == "" {
		return DeliveryStatus{}, errors.New("delivery id required")
	}
	store, err := openWriter(ctx, config)
	if err != nil {
		return DeliveryStatus{}, err
	}
	defer func() { _ = store.Close() }()
	delivery, err := store.RetryDelivery(ctx, id)
	switch {
	case errors.Is(err, core.ErrNotFound):
		return DeliveryStatus{}, errors.New("delivery not found")
	case errors.Is(err, core.ErrConflict):
		return DeliveryStatus{}, errors.New("only failed deliveries can be retried")
	case err != nil:
		return DeliveryStatus{}, adminOperationError(ctx, "retry failed")
	}
	return DeliveryStatus{ID: delivery.ID, Source: delivery.Source, State: delivery.Status}, nil
}

// Prune 立即删除超过保留期的已结算记录。
func Prune(ctx context.Context, config Config) (core.PruneResult, error) {
	store, err := openWriter(ctx, config)
	if err != nil {
		return core.PruneResult{}, err
	}
	defer func() { _ = store.Close() }()
	removed, err := store.Prune(ctx, time.Now().Add(-config.Runtime.Retention))
	if err != nil {
		return core.PruneResult{}, adminOperationError(ctx, "prune failed")
	}
	return removed, nil
}

// adminOperationError 保留取消语义，其余Store错误仍保持脱敏。
func adminOperationError(ctx context.Context, message string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.New(message)
}

// Check 是一项诊断结果；Detail 不含 Secret 明文。
type Check struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
}

// DoctorOptions 控制诊断的外部探测；HTTPClient 为空时使用 10 秒超时的默认客户端。
type DoctorOptions struct {
	Resolver   secret.Resolver
	HTTPClient *http.Client
	// Online 为 true 时探测 MA 与飞书公开接口的鉴权连通性。
	Online bool
}

// Doctor 逐项检查配置、Secret 可解析性、数据库状态，以及（可选）MA 与飞书凭证连通性。
// 飞书凭证可换取 tenant token 只证明 App ID/Secret 可用，不证明消息收发权限已开通。
func Doctor(ctx context.Context, config Config, options DoctorOptions) []Check {
	if ctx.Err() != nil {
		return []Check{{Name: "context", Detail: "operation cancelled"}}
	}
	if options.Resolver == nil {
		options.Resolver = secret.EnvFileResolver{}
	}
	client := options.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	if err := config.Validate(); err != nil {
		return []Check{{Name: "config", Detail: err.Error()}}
	}
	checks := []Check{{Name: "config", OK: true, Detail: "offline validation passed"}}
	if config.UserAuth.enabled() {
		_, err := userAuthCodec(config, options.Resolver)
		detail := "AES-256 key resolved; owner/project are operator-configured, not inferred from api key"
		if err != nil {
			detail = err.Error()
		}
		checks = append(checks, Check{Name: "user_auth_encryption", OK: err == nil, Detail: detail})
	}
	_, statErr := os.Stat(config.DatabasePath)
	switch err := statErr; {
	case config.Store.Driver == "mysql":
		status, err := StatusWith(ctx, config, options.Resolver)
		if err != nil {
			checks = append(checks, Check{Name: "database", OK: false, Detail: err.Error()})
		} else {
			checks = append(checks, databaseChecks(status)...)
		}
	case errors.Is(err, os.ErrNotExist):
		checks = append(checks, Check{Name: "database", OK: false, Detail: "database does not exist; run channel-runner install first"})
	case err != nil:
		checks = append(checks, Check{Name: "database", OK: false, Detail: "database path unavailable"})
	default:
		status, err := StatusWith(ctx, config, options.Resolver)
		if err != nil {
			checks = append(checks, Check{Name: "database", OK: false, Detail: err.Error()})
		} else {
			checks = append(checks, databaseChecks(status)...)
		}
	}
	for _, connection := range config.Connections {
		if ctx.Err() != nil {
			return append(checks, Check{Name: "context", Detail: "operation cancelled"})
		}
		prefix := "connection " + connection.ID + ": "
		// 离线静态检查：bot 身份依赖沙箱注入 LARKSUITE_CLI_*，self_hosted 环境不受平台编排，
		// 必须在 doctor 阶段显式报出，避免上线后身份 env 静默失效。
		if connection.resolvedIdentity.Group == core.IdentityBot || connection.resolvedIdentity.P2P == core.IdentityBot || connection.resolvedIdentity.P2P == core.IdentityUser {
			checks = append(checks, Check{Name: prefix + "identity_environment", OK: true, Detail: "bot identity uses a platform-managed sandbox environment; self_hosted is rejected at runtime"})
			if options.Online && connection.EnvironmentID != "" {
				if maKey, mErr := options.Resolver.Resolve(connection.MA.APIKeyRef); mErr == nil && maKey != "" {
					checks = append(checks, probeEnvironment(ctx, client, connection, maKey, prefix))
				}
			}
		}
		appSecret, appErr := options.Resolver.Resolve(connection.Feishu.AppSecretRef)
		checks = append(checks, Check{Name: prefix + "feishu_app_secret", OK: appErr == nil && appSecret != "", Detail: resolveDetail(appErr, appSecret)})
		maKey, maErr := options.Resolver.Resolve(connection.MA.APIKeyRef)
		checks = append(checks, Check{Name: prefix + "ma_api_key", OK: maErr == nil && maKey != "", Detail: resolveDetail(maErr, maKey)})
		if !options.Online {
			continue
		}
		if maErr == nil && maKey != "" {
			checks = append(checks, probeMA(ctx, client, connection, maKey, prefix))
		}
		if appErr == nil && appSecret != "" && connection.resolvedIdentity.P2P != core.IdentityUser {
			checks = append(checks, probeFeishu(ctx, client, connection, appSecret, prefix))
		}
	}
	return checks
}

func databaseChecks(status StatusSnapshot) []Check {
	open := status.Stats.QueuedTurns + status.Stats.RunningTurns + status.Stats.WaitingTurns
	return []Check{
		{Name: "database", OK: true, Detail: "schema readable"},
		{Name: "open_turns", OK: true, Detail: strconv.Itoa(open)},
		{Name: "failed_deliveries", OK: status.Stats.FailedDeliveries == 0, Detail: strconv.Itoa(status.Stats.FailedDeliveries)},
	}
}

func resolveDetail(err error, value string) string {
	if err != nil || value == "" {
		return "reference cannot be resolved"
	}
	return "resolved"
}

// environmentSelfHosted 与 core 侧保持一致的自托管环境类型。
const environmentSelfHosted = "self_hosted"

// probeEnvironment 在 --online 时 GET /environments/:id：仅 config.type=cloud 支持
// bot 身份环境变量；self_hosted、缺失或未知类型都明确诊断，不猜测映射。
func probeEnvironment(ctx context.Context, client *http.Client, connection ConnectionConfig, key, prefix string) Check {
	base := strings.TrimRight(connection.MA.BaseURL, "/")
	if !strings.HasSuffix(base, "/api/v3") {
		base += "/api/v3"
	}
	target := base + "/environments/" + url.PathEscape(connection.EnvironmentID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return Check{Name: prefix + "identity_environment", OK: false, Detail: "request invalid"}
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return Check{Name: prefix + "identity_environment", OK: false, Detail: "ma unreachable"}
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return Check{Name: prefix + "identity_environment", OK: false, Detail: "environment not readable for this api key (status " + strconv.Itoa(resp.StatusCode) + ")"}
	}
	var env struct {
		Config struct {
			Type string `json:"type"`
		} `json:"config"`
	}
	if err := decodeJSON(resp, &env); err != nil {
		return Check{Name: prefix + "identity_environment", OK: false, Detail: "environment response invalid"}
	}
	// 现网运行类型取 config.type（顶层 type 是 environment/environment_with_overrides）。
	envType := env.Config.Type
	if envType == environmentSelfHosted {
		return Check{Name: prefix + "identity_environment", OK: false, Detail: "bot identity env cannot be applied to a self_hosted environment"}
	}
	if envType == "" {
		return Check{Name: prefix + "identity_environment", OK: false, Detail: "environment config type missing"}
	}
	if envType != "cloud" {
		return Check{Name: prefix + "identity_environment", OK: false, Detail: "environment config type unsupported"}
	}
	return Check{Name: prefix + "identity_environment", OK: true, Detail: "environment type " + envType + " supports bot identity env"}
}

func probeMA(ctx context.Context, client *http.Client, connection ConnectionConfig, key, prefix string) Check {
	base := strings.TrimRight(connection.MA.BaseURL, "/")
	if !strings.HasSuffix(base, "/api/v3") {
		base += "/api/v3"
	}
	target := base + "/agents/" + url.PathEscape(connection.AgentID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return Check{Name: prefix + "ma_agent", OK: false, Detail: "request invalid"}
	}
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := client.Do(req)
	if err != nil {
		return Check{Name: prefix + "ma_agent", OK: false, Detail: "ma unreachable"}
	}
	_ = resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return Check{Name: prefix + "ma_agent", OK: true, Detail: "api key can read the agent"}
	case http.StatusUnauthorized, http.StatusForbidden:
		return Check{Name: prefix + "ma_agent", OK: false, Detail: "api key rejected"}
	case http.StatusNotFound:
		return Check{Name: prefix + "ma_agent", OK: false, Detail: "agent not found for this api key"}
	}
	return Check{Name: prefix + "ma_agent", OK: false, Detail: "unexpected status " + strconv.Itoa(resp.StatusCode)}
}

func probeFeishu(ctx context.Context, client *http.Client, connection ConnectionConfig, appSecret, prefix string) Check {
	base := strings.TrimRight(connection.Feishu.OpenBaseURL, "/")
	if base == "" {
		base = "https://open.feishu.cn"
	}
	body := strings.NewReader(`{"app_id":` + quote(connection.Feishu.AppID) + `,"app_secret":` + quote(appSecret) + `}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/open-apis/auth/v3/tenant_access_token/internal", body)
	if err != nil {
		return Check{Name: prefix + "feishu_token", OK: false, Detail: "request invalid"}
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := client.Do(req)
	if err != nil {
		return Check{Name: prefix + "feishu_token", OK: false, Detail: "feishu unreachable"}
	}
	defer func() { _ = resp.Body.Close() }()
	var result struct {
		Code int `json:"code"`
	}
	if resp.StatusCode != http.StatusOK || decodeJSON(resp, &result) != nil || result.Code != 0 {
		return Check{Name: prefix + "feishu_token", OK: false, Detail: "app credentials rejected"}
	}
	return Check{Name: prefix + "feishu_token", OK: true, Detail: "app credentials valid (does not prove message permissions)"}
}
