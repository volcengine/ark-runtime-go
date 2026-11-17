// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package runner 装配配置式 Bot Runtime，并提供离线状态检查。
package runner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
	observability "github.com/volcengine/ark-runtime-go/channel/internal/telemetry"
	"github.com/volcengine/ark-runtime-go/channel/secret"
)

var (
	errIdentityUserUnsupported = errors.New("identity user requires explicit user_auth configuration and encrypted storage")
	errIdentityUnsupported     = errors.New("identity unsupported")
)

const maxConfigBytes = 256 * 1024

var agentVersionPattern = regexp.MustCompile(`^v?[1-9][0-9]*$`)

var tablePrefixPattern = regexp.MustCompile(`^[A-Za-z0-9_]{1,31}$`)

// Config 是已验证的非密运行配置；SecretRef 只保存引用。
type Config struct {
	UserAuth     UserAuthConfig
	payloadCodec codec.Codec
	DatabasePath string
	// WebhookAddr 是飞书事件回调监听地址（如 0.0.0.0:8080），任一连接使用 webhook 时必填。
	// 该端口需由部署方以 HTTPS 反向代理暴露给飞书。
	WebhookAddr   string
	Runtime       core.RuntimeOptions
	Store         StoreConfig
	Observability ObservabilityConfig
	Connections   []ConnectionConfig
}

// ObservabilityConfig 控制结构化日志与本地管理 HTTP。AdminAddr 为空时不启动管理端口；
// 非空时只允许回环地址，远程访问需部署方自行加认证代理。
type ObservabilityConfig struct {
	AdminAddr string `json:"admin_addr"`
	LogLevel  string `json:"log_level"`
	LogFormat string `json:"log_format"`
}

// StoreConfig 选择持久化实现并限制待处理任务与单条文本大小。
//
// Driver 为 embedded（默认，单实例 bbolt 文件，路径取 database_path）、sqlite（单机多进程共享
// 文件）或 mysql（Hosted 多实例）。sqlite 使用 database_path；mysql 的 DSN 含口令，必须通过
// DSNRef 以 SecretRef 提供。TablePrefix 允许多个部署共用一个库。
type StoreConfig struct {
	// PayloadNamespace 为通用持久化内容提供稳定隔离键，与用户身份无关。
	PayloadNamespace string `json:"payload_namespace,omitempty"`
	Driver           string `json:"driver"`
	DSNRef           string `json:"dsn_ref"`
	TablePrefix      string `json:"table_prefix"`
	MaxOpenTurns     int    `json:"max_open_turns"`
	MaxMessageBytes  int    `json:"max_message_bytes"`
}

// ConnectionConfig 显式绑定一个飞书 App 与一个 MA Agent。
type ConnectionConfig struct {
	UserAuth        UserAuthConnectionConfig `json:"user_auth,omitempty"`
	ID              string                   `json:"id"`
	Feishu          FeishuConfig             `json:"feishu"`
	MA              MAConfig                 `json:"ma"`
	AgentID         string                   `json:"agent_id"`
	AgentVersion    string                   `json:"agent_version"`
	EnvironmentID   string                   `json:"environment_id"`
	VaultIDs        []string                 `json:"vault_ids"`
	MemoryIDs       []string                 `json:"memory_ids"`
	GroupTrigger    string                   `json:"group_trigger"`
	GroupReplyMode  string                   `json:"group_reply_mode"`
	MaxTurnDuration time.Duration            `json:"-"`
	Identity        string                   `json:"identity,omitempty"`
	// IdentityV2 是结构化身份配置：{group,p2p}；设置后优先于旧单值 identity。
	IdentityV2   *IdentityConfig `json:"identity_v2,omitempty"`
	ResponseMode string          `json:"response_mode"`
	// ProgressProtocol 可选 native_cot；空值保留当前普通过程卡片。
	ProgressProtocol string `json:"progress_protocol,omitempty"`
	// ShowProgress 为 true 时以卡片展示工具调用过程（只含工具名与状态）。
	ShowProgress bool `json:"show_progress"`
	// Approvals 为 true 时，evaluated_permission=ask 的工具调用向触发者展示允许/拒绝卡片。
	// 飞书应用需订阅 card.action.trigger 回调（WebSocket 与 webhook 均可）。
	Approvals bool `json:"approvals"`
	// RecentContext 为 true 时，新 Session 首轮附带当前会话范围内的近期聊天背景。
	// 飞书应用需开通读取会话历史消息权限（im:message.group_msg / im:message.p2p_msg:readonly）。
	RecentContext bool `json:"recent_context"`
	// HookFailOpen 为 true 时，BeforeTrigger / EnrichInput 失败按放行 / 跳过处理；默认拒绝。
	HookFailOpen bool `json:"hook_fail_open"`
	// BotCredential 在 identity=bot 时必填：Runner 把飞书 tenant_access_token 写入该 Vault 的
	// environment_variable 凭据并定期续期；vault_id 必须同时列在 vault_ids 中。
	BotCredential BotCredentialConfig `json:"bot_credential"`
	// DeliverArtifacts 为 true 时，工具结果中以 Files 引用返回的图片/文档/视频在答案后发送。
	DeliverArtifacts bool `json:"deliver_artifacts"`

	// resolvedIdentity 是 normalize 后得到的结构化身份，不参与 JSON 序列化。
	resolvedIdentity core.IdentityPolicy `json:"-"`
}

// IdentityConfig 是结构化执行身份配置。Group 非空时只允许 bot；P2P 为空/bot/user。
type IdentityConfig struct {
	Group string `json:"group"`
	P2P   string `json:"p2p"`
}

// resolve 返回规范化后的身份策略与错误；兼容旧单值 identity。
func (c ConnectionConfig) identityPolicy() (core.IdentityPolicy, error) {
	if c.IdentityV2 != nil {
		policy := core.IdentityPolicy{Group: c.IdentityV2.Group, P2P: c.IdentityV2.P2P}
		if policy.Group != "" && policy.Group != core.IdentityBot {
			return core.IdentityPolicy{}, errors.New("identity_v2.group must be empty or bot")
		}
		if err := policy.Valid(); err != nil {
			return core.IdentityPolicy{}, err
		}
		return policy, nil
	}
	switch c.Identity {
	case "", "none":
		return core.IdentityPolicy{}, nil
	case "bot":
		return core.IdentityPolicy{Group: core.IdentityBot, P2P: core.IdentityBot}, nil
	case "user":
		return core.IdentityPolicy{P2P: core.IdentityUser}, nil
	default:
		return core.IdentityPolicy{}, errIdentityUnsupported
	}
}

// BotCredentialConfig 是 Bot 执行身份凭据的写入位置。
type BotCredentialConfig struct {
	VaultID      string   `json:"vault_id"`
	SecretName   string   `json:"secret_name"`
	AllowedHosts []string `json:"allowed_hosts"`
}

// FeishuConfig 保存 App ID 与 App Secret 引用。OpenBaseURL 为空时使用飞书公网地址。
//
// ReceiveMode 为 websocket（默认）或 webhook。webhook 需要 verification_token_ref 与
// encrypt_key_ref，强制加密与验签；回调路径为 /feishu/<connection id>/events，监听地址见
// 顶层 webhook_addr。
type FeishuConfig struct {
	AppID                string `json:"app_id"`
	AppSecretRef         string `json:"app_secret_ref"`
	OpenBaseURL          string `json:"open_base_url"`
	ReceiveMode          string `json:"receive_mode"`
	VerificationTokenRef string `json:"verification_token_ref"`
	EncryptKeyRef        string `json:"encrypt_key_ref"`
}

// MAConfig 保存公开 API 地址与密钥引用。
type MAConfig struct {
	// ModelHeaderRefs 只保存模型目标 Header 的 SecretRef，明文在提交 events 时读取。
	ModelHeaderRefs map[string]string `json:"model_header_refs,omitempty"`
	BaseURL         string            `json:"base_url"`
	APIKeyRef       string            `json:"api_key_ref"`
}

type runtimeWire struct {
	Workers             int    `json:"workers"`
	PollInterval        string `json:"poll_interval"`
	TurnTimeout         string `json:"turn_timeout"`
	LeaseTTL            string `json:"lease_ttl"`
	DeliveryTimeout     string `json:"delivery_timeout"`
	DeliveryMaxAttempts uint64 `json:"delivery_max_attempts"`
	IdleTimeout         string `json:"idle_timeout"`
	MessageDeadline     string `json:"message_deadline"`
	UnknownWriteGrace   string `json:"unknown_write_grace"`
	Retention           string `json:"retention"`
	PruneInterval       string `json:"prune_interval"`
	// Attachments 限制单条消息附件；字节数为整数。
	Attachments core.AttachmentLimits `json:"attachments"`
	// Context 限制近期上下文条数与字符数；max_age 为 Go duration 字符串，默认 24h。
	Context ContextConfig `json:"context"`
	// Artifacts 限制单轮产物投递数量与单个大小。
	Artifacts core.ArtifactLimits `json:"artifacts"`
}

// ContextConfig 是近期上下文的配置形态。
type ContextConfig struct {
	MaxMessages int    `json:"max_messages"`
	MaxChars    int    `json:"max_chars"`
	MaxAge      string `json:"max_age"`
}

type connectionWire struct {
	ConnectionConfig
	MaxTurnDuration string `json:"max_turn_duration"`
}

type configWire struct {
	UserAuth      UserAuthConfig      `json:"user_auth"`
	DatabasePath  string              `json:"database_path"`
	WebhookAddr   string              `json:"webhook_addr"`
	Runtime       runtimeWire         `json:"runtime"`
	Store         StoreConfig         `json:"store"`
	Observability ObservabilityConfig `json:"observability"`
	Connections   []connectionWire    `json:"connections"`
}

// Load 从有限大小的单个 JSON 对象加载配置；相对数据库路径锚定配置所在目录。
func Load(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, errors.New("config unavailable")
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, maxConfigBytes+1))
	if err != nil || len(data) > maxConfigBytes {
		return Config{}, errors.New("config unreadable or too large")
	}
	if len(bytes.TrimSpace(data)) == 0 || bytes.TrimSpace(data)[0] != '{' {
		return Config{}, errors.New("config must be one json object")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var wire configWire
	if err := decoder.Decode(&wire); err != nil {
		return Config{}, errors.New("config json invalid")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return Config{}, errors.New("config must be one json object")
	}
	pathValue := wire.DatabasePath
	if pathValue != "" && !filepath.IsAbs(pathValue) {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return Config{}, errors.New("config path invalid")
		}
		pathValue = filepath.Join(filepath.Dir(absolute), pathValue)
	}
	config := Config{UserAuth: wire.UserAuth, DatabasePath: pathValue, WebhookAddr: wire.WebhookAddr, Store: wire.Store, Observability: wire.Observability}
	if config.Runtime.PollInterval, err = parseDuration(wire.Runtime.PollInterval); err != nil {
		return Config{}, err
	}
	if config.Runtime.TurnTimeout, err = parseDuration(wire.Runtime.TurnTimeout); err != nil {
		return Config{}, err
	}
	if config.Runtime.LeaseTTL, err = parseDuration(wire.Runtime.LeaseTTL); err != nil {
		return Config{}, err
	}
	if config.Runtime.DeliveryTimeout, err = parseDuration(wire.Runtime.DeliveryTimeout); err != nil {
		return Config{}, err
	}
	if config.Runtime.IdleTimeout, err = parseDuration(wire.Runtime.IdleTimeout); err != nil {
		return Config{}, err
	}
	if config.Runtime.MessageDeadline, err = parseDuration(wire.Runtime.MessageDeadline); err != nil {
		return Config{}, err
	}
	if config.Runtime.UnknownWriteGrace, err = parseDuration(wire.Runtime.UnknownWriteGrace); err != nil {
		return Config{}, err
	}
	if config.Runtime.Retention, err = parseDuration(wire.Runtime.Retention); err != nil {
		return Config{}, err
	}
	if config.Runtime.PruneInterval, err = parseDuration(wire.Runtime.PruneInterval); err != nil {
		return Config{}, err
	}
	config.Runtime.Workers = wire.Runtime.Workers
	config.Runtime.Attachments = wire.Runtime.Attachments
	config.Runtime.Artifacts = wire.Runtime.Artifacts
	config.Runtime.Context = core.ContextLimits{MaxMessages: wire.Runtime.Context.MaxMessages, MaxChars: wire.Runtime.Context.MaxChars}
	if wire.Runtime.Context.MaxAge != "" {
		age, err := time.ParseDuration(wire.Runtime.Context.MaxAge)
		if err != nil {
			return Config{}, errors.New("runtime.context.max_age invalid")
		}
		config.Runtime.Context.MaxAge = age
	}
	config.Runtime.DeliveryMaxAttempts = wire.Runtime.DeliveryMaxAttempts
	for _, item := range wire.Connections {
		connection := item.ConnectionConfig
		connection.MaxTurnDuration, err = parseDuration(item.MaxTurnDuration)
		if err != nil {
			return Config{}, err
		}
		config.Connections = append(config.Connections, connection)
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func parseDuration(value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, errors.New("invalid duration")
	}
	return duration, nil
}

// Validate 离线检查配置并填充默认值，不读取 Secret 或数据库。
func (config *Config) Validate() error {
	if config == nil || config.Store.Driver != "mysql" && (config.DatabasePath == "" || strings.TrimSpace(config.DatabasePath) != config.DatabasePath) || len(config.Connections) == 0 || len(config.Connections) > 32 {
		return errors.New("invalid runner configuration")
	}
	if config.UserAuth.enabled() && config.Store.PayloadNamespace == "" {
		config.Store.PayloadNamespace = "runner-user-auth-v1"
	}
	if err := config.UserAuth.validate(); err != nil {
		return err
	}
	if config.Store.MaxOpenTurns < 0 || config.Store.MaxOpenTurns > 100000 || config.Store.MaxMessageBytes < 0 || config.Store.MaxMessageBytes > 1024*1024 {
		return errors.New("invalid store limits")
	}
	if config.Store.Driver == "" {
		config.Store.Driver = "embedded"
	}
	switch config.Store.Driver {
	case "embedded", "sqlite":
		if config.Store.DSNRef != "" {
			return errors.New("dsn_ref is only valid for the mysql driver")
		}
	case "mysql":
		if secret.Validate(config.Store.DSNRef) != nil {
			return errors.New("mysql store requires dsn_ref as a secret reference")
		}
	default:
		return errors.New("store driver unsupported")
	}
	if config.Store.TablePrefix != "" && (config.Store.Driver == "embedded" || !tablePrefixPattern.MatchString(config.Store.TablePrefix)) {
		return errors.New("table_prefix invalid")
	}
	if config.Store.MaxOpenTurns == 0 {
		config.Store.MaxOpenTurns = 1000
	}
	if config.Store.MaxMessageBytes == 0 {
		config.Store.MaxMessageBytes = 32768
	}
	if err := config.Observability.validate(); err != nil {
		return err
	}
	options, err := config.Runtime.Normalize()
	if err != nil {
		return errors.New("invalid runtime limits")
	}
	config.Runtime = options
	ids, apps := map[string]bool{}, map[string]bool{}
	webhooks := 0
	for i := range config.Connections {
		connection := &config.Connections[i]
		if !validID(connection.ID) || !validID(connection.Feishu.AppID) || !validID(connection.AgentID) || !validID(connection.EnvironmentID) || !agentVersionPattern.MatchString(connection.AgentVersion) || ids[connection.ID] || apps[connection.Feishu.AppID] {
			return fmt.Errorf("connection %d identity invalid or duplicate", i)
		}
		ids[connection.ID], apps[connection.Feishu.AppID] = true, true
		if err := validateBaseURL(connection.MA.BaseURL); err != nil {
			return fmt.Errorf("connection %d ma url invalid", i)
		}
		if connection.Feishu.ReceiveMode == "" {
			connection.Feishu.ReceiveMode = "websocket"
		}
		switch connection.Feishu.ReceiveMode {
		case "websocket":
			if connection.Feishu.VerificationTokenRef != "" || connection.Feishu.EncryptKeyRef != "" {
				return fmt.Errorf("connection %d webhook secrets set for websocket mode", i)
			}
		case "webhook":
			webhooks++
			if secret.Validate(connection.Feishu.VerificationTokenRef) != nil || secret.Validate(connection.Feishu.EncryptKeyRef) != nil {
				return fmt.Errorf("connection %d webhook secret reference invalid", i)
			}
		default:
			return fmt.Errorf("connection %d receive mode unsupported", i)
		}
		if connection.Feishu.OpenBaseURL != "" && validateOpenURL(connection.Feishu.OpenBaseURL) != nil {
			return fmt.Errorf("connection %d feishu url invalid", i)
		}
		if secret.Validate(connection.Feishu.AppSecretRef) != nil || secret.Validate(connection.MA.APIKeyRef) != nil {
			return fmt.Errorf("connection %d secret reference invalid", i)
		}
		if err := validateModelHeaderRefs(connection.MA.ModelHeaderRefs); err != nil {
			return fmt.Errorf("connection %q model header references invalid", connection.ID)
		}
		for _, id := range append(append([]string{}, connection.VaultIDs...), connection.MemoryIDs...) {
			if !validID(id) {
				return fmt.Errorf("connection %d resource id invalid", i)
			}
		}
		if connection.GroupTrigger == "" {
			connection.GroupTrigger = "mention"
		}
		if connection.GroupReplyMode == "" {
			connection.GroupReplyMode = "thread"
		}
		if connection.GroupTrigger != "mention" && connection.GroupTrigger != "all" || connection.GroupReplyMode != "thread" && connection.GroupReplyMode != "reply" && connection.GroupReplyMode != "chat" {
			return fmt.Errorf("connection %d group mode unsupported", i)
		}
		if connection.ResponseMode == "" {
			connection.ResponseMode = "non_stream"
		}
		policy, err := connection.identityPolicy()
		if err != nil {
			return fmt.Errorf("connection %q %w", connection.ID, err)
		}
		connection.resolvedIdentity = policy
		if policy.Group == core.IdentityBot || policy.P2P == core.IdentityBot {
			if connection.BotCredential.SecretName == "" {
				connection.BotCredential.SecretName = "FEISHU_TENANT_ACCESS_TOKEN"
			}
			if len(connection.BotCredential.AllowedHosts) == 0 {
				connection.BotCredential.AllowedHosts = []string{"open.feishu.cn"}
			}
			if !validID(connection.BotCredential.VaultID) || !contains(connection.VaultIDs, connection.BotCredential.VaultID) {
				return fmt.Errorf("connection %d bot_credential.vault_id must be listed in vault_ids", i)
			}
		}
		if policy.P2P == core.IdentityUser {
			if err := connection.validateUserAuth(config.UserAuth); err != nil {
				return fmt.Errorf("connection %d %w", i, err)
			}
		}
		if connection.ProgressProtocol != "" && connection.ProgressProtocol != "native_cot" ||
			connection.ResponseMode != "non_stream" && connection.ResponseMode != "process_stream" && connection.ResponseMode != "final_stream" ||
			connection.ResponseMode == "process_stream" && connection.ProgressProtocol != "native_cot" {
			return fmt.Errorf("connection %d capability unsupported", i)
		}
		if connection.MaxTurnDuration == 0 {
			connection.MaxTurnDuration = options.TurnTimeout
		}
		if connection.MaxTurnDuration < 0 || connection.MaxTurnDuration > 30*time.Minute || connection.MaxTurnDuration >= options.LeaseTTL {
			return fmt.Errorf("connection %d turn duration invalid", i)
		}
	}
	if webhooks > 0 {
		if _, port, err := net.SplitHostPort(config.WebhookAddr); err != nil || port == "" {
			return errors.New("webhook_addr required as host:port when a connection uses webhook")
		}
	} else if config.WebhookAddr != "" {
		return errors.New("webhook_addr set but no connection uses webhook")
	}
	return nil
}

func (o *ObservabilityConfig) validate() error {
	if o.LogLevel == "" {
		o.LogLevel = "info"
	}
	if o.LogFormat == "" {
		o.LogFormat = "json"
	}
	if o.LogLevel != "debug" && o.LogLevel != "info" && o.LogLevel != "warn" && o.LogLevel != "error" || o.LogFormat != "json" && o.LogFormat != "text" {
		return errors.New("invalid observability configuration")
	}
	if o.AdminAddr != "" && observability.ValidateLoopback(o.AdminAddr) != nil {
		return errors.New("admin_addr must bind a loopback interface")
	}
	return nil
}

func validID(value string) bool {
	if value == "" || len(value) > 256 || strings.TrimSpace(value) != value {
		return false
	}
	return !strings.ContainsAny(value, " \t\r\n\x00")
}

func validateOpenURL(value string) error {
	u, err := url.Parse(value)
	if err != nil || u == nil || u.Scheme != "https" && u.Scheme != "http" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" {
		return errors.New("invalid url")
	}
	return nil
}

func validateBaseURL(value string) error {
	u, err := url.Parse(value)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/api/v3" && u.Path != "/api/v3/") {
		return errors.New("invalid url")
	}
	return nil
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}
