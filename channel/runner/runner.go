// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	observability "github.com/volcengine/ark-runtime-go/channel/internal/telemetry"
	"github.com/volcengine/ark-runtime-go/channel/provider/feishu"
	"github.com/volcengine/ark-runtime-go/channel/provider/feishu/oauth"
	"github.com/volcengine/ark-runtime-go/channel/secret"
)

// Dependencies 为配置式装配提供业务 Hooks 与密钥解析器。
type Dependencies struct {
	Hooks    core.Hooks
	Resolver secret.Resolver
}

// dependencies 允许同包测试注入端口；零值使用实际 Store 与适配器。
type dependencies struct {
	// NewOAuth 允许测试注入协议客户端；同一客户端发现应用权限并执行授权。
	NewOAuth    func(oauth.Config) (*oauth.Client, error)
	Resolver    secret.Resolver
	NewMA       func(MAConfig, string) (core.MAClient, error)
	NewProvider func(core.Binding, FeishuConfig, secret.Resolver) (core.Provider, error)
	// LogOutput 是结构化日志输出；为空时写 stderr。
	LogOutput io.Writer
	// Metrics 允许调用方共享指标聚合器；为空时新建。
	Metrics *observability.Metrics
	// Hooks 是嵌入方注入的受控扩展点（官方可执行程序不注入）。
	Hooks core.Hooks
}

// NewLogger 按配置构造 slog.Logger。
func NewLogger(config ObservabilityConfig, output io.Writer) *slog.Logger {
	if output == nil {
		output = os.Stderr
	}
	level := map[string]slog.Level{"debug": slog.LevelDebug, "info": slog.LevelInfo, "warn": slog.LevelWarn, "error": slog.LevelError}[config.LogLevel]
	options := &slog.HandlerOptions{Level: level}
	if config.LogFormat == "text" {
		return slog.New(slog.NewTextHandler(output, options))
	}
	return slog.New(slog.NewJSONHandler(output, options))
}

// Run 以真实适配器运行配置，并在所有 goroutine 退出后关闭 Store。
func Run(ctx context.Context, config Config) error {
	return runWith(ctx, config, dependencies{})
}

// RunWith 使用显式依赖装配 Runtime；错误不包含 SecretRef 或明文。
func RunWith(ctx context.Context, config Config, deps Dependencies) error {
	return runWith(ctx, config, dependencies{Hooks: deps.Hooks, Resolver: deps.Resolver})
}

func runWith(ctx context.Context, config Config, deps dependencies) (runErr error) {
	if ctx == nil {
		return errors.New("run context required")
	}
	if err := config.Validate(); err != nil {
		return err
	}
	if deps.Resolver == nil {
		deps.Resolver = secret.EnvFileResolver{}
	}
	crypt, err := userAuthCodec(config, deps.Resolver)
	if err != nil {
		return err
	}
	config.payloadCodec = crypt
	store, err := openStore(ctx, config, deps.Resolver, modeRun)
	if err != nil {
		return err
	}
	defer func() {
		if err := store.Close(); err != nil && runErr == nil {
			runErr = errors.New("database close failed")
		}
	}()
	if deps.NewMA == nil {
		deps.NewMA = func(cfg MAConfig, key string) (core.MAClient, error) {
			return newRunnerMA(cfg, key, deps.Resolver)
		}
	}
	if deps.NewProvider == nil {
		cotLimiter := &feishu.COTLimiter{}
		cardKitLimiter := &feishu.CardKitLimiter{}
		deps.NewProvider = func(binding core.Binding, cfg FeishuConfig, resolver secret.Resolver) (core.Provider, error) {
			return feishu.NewProvider(feishu.ProviderConfig{Binding: binding, AppSecretRef: cfg.AppSecretRef, SecretResolver: resolver, OpenBaseURL: cfg.OpenBaseURL,
				COTLimiter: cotLimiter, CardKitLimiter: cardKitLimiter,
				ReceiveMode: cfg.ReceiveMode, VerificationTokenRef: cfg.VerificationTokenRef, EncryptKeyRef: cfg.EncryptKeyRef})
		}
	}
	connections := make([]core.Connection, 0, len(config.Connections))
	webhooks := http.NewServeMux()
	for _, item := range config.Connections {
		key, err := deps.Resolver.Resolve(item.MA.APIKeyRef)
		if err != nil || key == "" {
			return errors.New("ma api key unavailable")
		}
		client, err := deps.NewMA(item.MA, key)
		if err != nil || client == nil {
			return errors.New("ma client unavailable")
		}
		binding := core.Binding{
			ID: item.ID, AppID: item.Feishu.AppID,
			AgentID: item.AgentID, AgentVersion: item.AgentVersion,
			EnvironmentID: item.EnvironmentID,
			VaultIDs:      append([]string(nil), item.VaultIDs...),
			MemoryIDs:     append([]string(nil), item.MemoryIDs...),
			GroupTrigger:  item.GroupTrigger, GroupReplyMode: item.GroupReplyMode,
			MaxTurnDuration:   item.MaxTurnDuration,
			ShowProgress:      item.ShowProgress,
			ProgressProtocol:  item.ProgressProtocol,
			StreamProgress:    item.ResponseMode == "process_stream",
			StreamFinalAnswer: item.ResponseMode == "final_stream",
			Approvals:         item.Approvals,
			RecentContext:     item.RecentContext,
			HookFailOpen:      item.HookFailOpen,
			Identity:          item.resolvedIdentity,
			DeliverArtifacts:  item.DeliverArtifacts,
			BotCredential:     core.BotCredentialSpec{VaultID: item.BotCredential.VaultID, SecretName: item.BotCredential.SecretName, AllowedHosts: append([]string(nil), item.BotCredential.AllowedHosts...)},
		}
		authorizer, err := newUserAuthorizer(ctx, config, &item, store, client, deps.Resolver, deps.NewOAuth)
		if err != nil {
			return err
		}
		configureUserBinding(&binding, config, item)
		provider, err := deps.NewProvider(binding, item.Feishu, deps.Resolver)
		if err != nil || provider == nil {
			return errors.New("feishu provider unavailable")
		}
		if item.Feishu.ReceiveMode == "webhook" {
			receiver, ok := provider.(interface{ WebhookHandler() http.Handler })
			if !ok {
				return errors.New("provider does not support webhook")
			}
			webhooks.Handle("/feishu/"+url.PathEscape(item.ID)+"/events", receiver.WebhookHandler())
		}
		connections = append(connections, core.Connection{Binding: binding, MAClient: client, Provider: provider, UserAuthorizer: authorizer})
	}
	logger := NewLogger(config.Observability, deps.LogOutput)
	metrics := deps.Metrics
	if metrics == nil {
		metrics = observability.NewMetrics()
	}
	options := config.Runtime
	options.Observer = observability.Fanout{observability.NewLogger(logger), metrics}
	options.Hooks = deps.Hooks
	runtime, err := core.NewRuntime(store, connections, options)
	if err != nil {
		return errors.New("runtime unavailable")
	}
	components := []runnerComponent{{name: "runtime", run: runtime.Run}}
	var admin *observability.AdminServer
	if config.Observability.AdminAddr != "" {
		admin = &observability.AdminServer{Addr: config.Observability.AdminAddr, Metrics: metrics, Status: func(ctx context.Context) (any, error) {
			return projectStatus(ctx, store)
		}}
		components = append(components, runnerComponent{name: "admin", run: admin.Serve})
	}
	if config.WebhookAddr != "" {
		listener, err := net.Listen("tcp", config.WebhookAddr)
		if err != nil {
			return errors.New("webhook listen failed")
		}
		defer func() { _ = listener.Close() }()
		server := &http.Server{Handler: webhooks, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second}
		components = append(components, runnerComponent{name: "webhook", run: func(ctx context.Context) error {
			return serveWebhook(ctx, server, listener)
		}})
		logger.Info("feishu webhook listening", slog.String("addr", listener.Addr().String()))
	}
	components = append(components, runnerComponent{name: "queue metrics", run: func(ctx context.Context) error {
		reportQueueDepth(ctx, store, metrics, config.Runtime.PollInterval)
		return nil
	}})
	logger.Info("channel runner started", slog.Int("connections", len(connections)), slog.Bool("admin", admin != nil))
	if admin != nil {
		admin.SetReady(true)
	}
	err = runComponents(ctx, components)
	logger.Info("channel runner stopped")
	return err
}

// runnerComponent 的错误仅按组件名称输出，防止下游错误携带凭证或正文。
type runnerComponent struct {
	name string
	run  func(context.Context) error
}

// runComponents 在任一组件意外结束时取消其余组件，并等待全部退出。
func runComponents(parent context.Context, components []runnerComponent) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	type result struct {
		name string
		err  error
	}
	done := make(chan result, len(components))
	for _, component := range components {
		go func() { done <- result{name: component.name, err: component.run(ctx)} }()
	}
	remaining := len(components)
	var failure error
	select {
	case <-parent.Done():
	case stopped := <-done:
		remaining--
		if parent.Err() == nil {
			failure = errors.New(stopped.name + " stopped")
		}
	}
	cancel()
	for ; remaining > 0; remaining-- {
		stopped := <-done
		if failure == nil && parent.Err() == nil && stopped.err != nil {
			failure = errors.New(stopped.name + " stopped with error")
		}
	}
	return failure
}

func serveWebhook(ctx context.Context, server *http.Server, listener net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if server.Shutdown(shutdownCtx) != nil {
			_ = server.Close()
		}
	}()
	err := server.Serve(listener)
	cancel()
	<-shutdownDone
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// reportQueueDepth 周期性通过 Stats 写入队列 gauge，不读取历史全量数据。
func reportQueueDepth(ctx context.Context, store core.Store, metrics *observability.Metrics, interval time.Duration) {
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval * 10)
	defer ticker.Stop()
	for {
		stats, err := store.Stats(ctx)
		if err == nil {
			metrics.SetGauge("queue_queued_turns", float64(stats.QueuedTurns))
			metrics.SetGauge("queue_running_turns", float64(stats.RunningTurns))
			metrics.SetGauge("queue_waiting_turns", float64(stats.WaitingTurns))
			metrics.SetGauge("outbox_open_deliveries", float64(stats.OpenDeliveries))
			age := 0.0
			if !stats.OldestOpenTurnAt.IsZero() {
				age = time.Since(stats.OldestOpenTurnAt).Seconds()
			}
			metrics.SetGauge("queue_oldest_age_seconds", age)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
