// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
// Package environments 提供 self-hosted environment worker 的 SDK 风格组合层。
package environments

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/internal/selfhostedlog"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/environment"
	selfhosted "github.com/volcengine/ark-runtime-go/arkruntime/selfhosted"
	"github.com/volcengine/ark-runtime-go/arkruntime/selfhosted/envinit"
	"github.com/volcengine/ark-runtime-go/arkruntime/tools/agenttoolset"
)

// EnvironmentWorkerOptions 配置 EnvironmentWorker。
type EnvironmentWorkerOptions struct {
	// EnvironmentID 是 worker 持续 poll work 的 self-hosted environment，Run 必填。
	EnvironmentID string
	// WorkerID 是上报给控制面的 worker 标识，空值时自动生成。
	WorkerID string
	// Workdir 是每个 session 工作目录的根目录，空值时使用进程当前目录。
	Workdir string
	// UnrestrictedPaths 控制文件工具是否允许访问 Workdir 之外的路径。
	UnrestrictedPaths bool
	// ToolContext 提供默认工具集使用的高级执行环境配置。
	ToolContext *agenttoolset.AgentToolContext
	// Tools 是所有 session 复用的固定工具集合；设置 ToolsFunc 时忽略此字段。
	Tools *agenttoolset.Set
	// ToolsFunc 为每个 session 创建绑定到其工作目录的工具集合。
	ToolsFunc func(env *agenttoolset.AgentToolContext) (*agenttoolset.Set, error)
	// MaxIdle 是 session 在 end_turn idle 后继续等待事件的时间，nil 使用默认值。
	MaxIdle *time.Duration
	// ToolTimeout 限制单次工具执行时间，非正值使用 SessionToolRunner 默认值。
	ToolTimeout time.Duration
	// CustomTools 是按名称注册的自定义工具。
	CustomTools map[string]agenttoolset.Tool
	// Logger 接收 worker 生命周期日志，nil 使用 log.Default。
	Logger *log.Logger
}

// EnvironmentWorker 组合 work poll、session 初始化、tool event loop 和 heartbeat。
type EnvironmentWorker struct {
	api  selfhosted.API
	opts EnvironmentWorkerOptions
}

// NewEnvironmentWorker 创建绑定到控制面 API 的 environment worker。
func NewEnvironmentWorker(api selfhosted.API, opts EnvironmentWorkerOptions) *EnvironmentWorker {
	if opts.Workdir == "" {
		cwd, err := os.Getwd()
		if err == nil {
			opts.Workdir = cwd
		} else {
			opts.Workdir = "."
		}
	}
	if opts.WorkerID == "" {
		opts.WorkerID = defaultWorkerID()
	}
	return &EnvironmentWorker{api: api, opts: opts}
}

// NewEnvironmentWorkerForClient 创建绑定到 arkruntime.Client 的 environment worker。
func NewEnvironmentWorkerForClient(client *arkruntime.Client, opts EnvironmentWorkerOptions) *EnvironmentWorker {
	return NewEnvironmentWorker(selfhosted.NewClientAPI(client), opts)
}

// Run 持续 poll environment work queue 并处理 session work。
func (w *EnvironmentWorker) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	environmentID := w.opts.EnvironmentID
	if environmentID == "" {
		return errors.New("environments: EnvironmentID is required")
	}
	if w.api == nil {
		return errors.New("environments: API is required")
	}
	logger := w.logger().With("component", "environment-worker", "environment_id", environmentID)

	poller := NewWorkPoller(ctx, w.api, WorkPollerOptions{
		EnvironmentID: environmentID,
		WorkerID:      w.opts.WorkerID,
		AutoStop:      environment.NewOptBool(false),
		Logger:        w.opts.Logger,
	})
	defer func() { _ = poller.Close() }()

	for poller.Next() {
		work := poller.Current()
		if work == nil {
			continue
		}
		w.runClaimedWork(ctx, *work, logger)
	}
	if err := poller.Err(); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}

func (w *EnvironmentWorker) runClaimedWork(ctx context.Context, item selfhosted.WorkItem, logger *selfhostedlog.Logger) {
	work, err := claimedWorkFromItem(item, w.opts.EnvironmentID)
	if err != nil {
		logger.Warn("handle work failed", "work_id", item.ID, "err", err)
		return
	}
	if err := w.handleItem(ctx, work, false); err != nil &&
		!isBenignWorkerExit(err) {
		logger.Warn("handle work failed", "work_id", work.ID, "session_id", work.SessionID, "err", err)
	}
}

// HandleItemOptions 指定一个已经 claim 的 work item。
type HandleItemOptions struct {
	WorkID            string
	EnvironmentID     string
	SessionID         string
	LatestHeartbeatAt string
}

// HandleItem 处理单个已 claim 的 session work，适合作为 sandbox/container 入口。
func (w *EnvironmentWorker) HandleItem(ctx context.Context, opts HandleItemOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	work, err := claimedWorkFromOptions(opts)
	if err != nil {
		return err
	}
	if err := w.handleItem(ctx, work, true); err != nil &&
		!isBenignWorkerExit(err) {
		return err
	}
	return nil
}

func (w *EnvironmentWorker) handleItem(ctx context.Context, work claimedWork, useWorkdirAsSession bool) (err error) {
	if w.api == nil {
		return errors.New("environments: API is required")
	}
	workdir, err := w.workdirFor(work.SessionID, useWorkdirAsSession)
	if err != nil {
		return err
	}
	api := w.api
	logger := w.logger().With("component", "environment-worker", "work_id", work.ID, "session_id", work.SessionID, "workdir", workdir)

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var heartbeatCause atomic.Value
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		w.heartbeatLoop(workCtx, work, api, cancel, func(cause heartbeatStopCause) {
			heartbeatCause.Store(string(cause))
		})
	}()

	defer func() {
		cancel()
		<-heartbeatDone
		cause := loadHeartbeatStopCause(&heartbeatCause)
		if shouldStopItem(cause) {
			_ = w.stopItem(api, work)
		} else {
			logger.Info("skip stop work after heartbeat ownership became uncertain", "cause", cause)
		}
		if cause == heartbeatStopCauseStopRequested && errors.Is(err, context.Canceled) {
			err = nil
		}
	}()

	session, err := api.GetSession(workCtx, selfhosted.GetSessionRequest{SessionID: work.SessionID})
	if err != nil {
		return fmt.Errorf("get session: %w", err)
	}
	if session == nil {
		err = errors.New("session response is empty")
		return err
	}
	if session.ID == "" {
		session.ID = work.SessionID
	}
	toolEnv := w.toolContext(workdir)
	initOpts := toolEnv.InitOptions()
	initOpts.Workdir = workdir
	initOpts.Logger = w.opts.Logger
	if err := envinit.New(api, initOpts).Setup(workCtx, session); err != nil {
		return fmt.Errorf("setup session environment: %w", err)
	}
	tools, owned, err := w.toolsFor(toolEnv)
	if err != nil {
		return fmt.Errorf("create toolset: %w", err)
	}
	if owned {
		defer func() { _ = tools.Close() }()
	}
	store, err := selfhosted.NewFileToolResultStore(workdir)
	if err != nil {
		return fmt.Errorf("create tool result store: %w", err)
	}
	runner := selfhosted.NewSessionToolRunner(workCtx, api, work.SessionID, selfhosted.SessionToolRunnerOptions{
		WorkID:      work.ID,
		Tools:       tools,
		CustomTools: w.opts.CustomTools,
		ResultStore: store,
		MaxIdle:     w.opts.MaxIdle,
		ToolTimeout: w.effectiveToolTimeout(),
		Logger:      w.opts.Logger,
	})
	defer func() { _ = runner.Close() }()
	for runner.Next() {
		result := runner.Current()
		logger.Info("tool event handled", "tool_use_id", result.ToolUseID, "tool", result.Name, "custom", result.Custom, "posted", result.Posted)
	}
	if err := runner.Err(); err != nil &&
		!errors.Is(err, selfhosted.ErrIdleTimeout) &&
		!errors.Is(err, selfhosted.ErrSessionTerminated) &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return runner.Err()
}

func (w *EnvironmentWorker) toolsFor(env *agenttoolset.AgentToolContext) (*agenttoolset.Set, bool, error) {
	if w.opts.ToolsFunc != nil {
		tools, err := w.opts.ToolsFunc(env)
		if err != nil {
			return nil, false, err
		}
		if tools == nil {
			return nil, false, errors.New("tools func returned nil")
		}
		return tools, true, nil
	}
	if w.opts.Tools != nil {
		return w.opts.Tools, false, nil
	}
	tools, err := agenttoolset.AgentToolset20260401(env)
	if err != nil {
		return nil, false, err
	}
	return tools, true, nil
}

func (w *EnvironmentWorker) toolContext(workdir string) *agenttoolset.AgentToolContext {
	base := agenttoolset.AgentToolContext{}
	if w.opts.ToolContext != nil {
		base = *w.opts.ToolContext
	}
	base.Workdir = workdir
	if w.opts.UnrestrictedPaths {
		base.UnrestrictedPaths = true
	}
	if timeout := w.effectiveToolTimeout(); timeout > 0 {
		base.ToolTimeout = timeout
	}
	if base.Env != nil {
		env := make(map[string]string, len(base.Env))
		for k, v := range base.Env {
			env[k] = v
		}
		base.Env = env
	}
	return &base
}

func (w *EnvironmentWorker) effectiveToolTimeout() time.Duration {
	if w.opts.ToolTimeout > 0 {
		return w.opts.ToolTimeout
	}
	if w.opts.ToolContext != nil && w.opts.ToolContext.ToolTimeout > 0 {
		return w.opts.ToolContext.ToolTimeout
	}
	return 0
}

func (w *EnvironmentWorker) workdirFor(sessionID string, useWorkdirAsSession bool) (string, error) {
	root := w.opts.Workdir
	if root == "" {
		root = "."
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if useWorkdirAsSession {
		return absRoot, nil
	}
	return sessionWorkdir(absRoot, sessionID)
}

func (w *EnvironmentWorker) stopItem(api selfhosted.API, work claimedWork) error {
	stopCtx, stopCancel := context.WithTimeout(context.Background(), stopTimeout)
	defer stopCancel()
	req := selfhosted.StopWorkRequest{
		EnvironmentID: work.EnvironmentID,
		WorkID:        work.ID,
		Force:         environment.NewOptBool(true),
	}
	if err := api.StopWork(stopCtx, req); err != nil {
		if selfhosted.IsStatus(err, 409) || selfhosted.IsStatus(err, 412) {
			w.logger().Info("stop work already resolved", "work_id", work.ID, "err", err)
			return nil
		}
		w.logger().Warn("stop work failed", "work_id", work.ID, "err", err)
		return err
	}
	return nil
}

func (w *EnvironmentWorker) logger() *selfhostedlog.Logger {
	return selfhostedlog.New(w.opts.Logger)
}

func loadHeartbeatStopCause(value *atomic.Value) heartbeatStopCause {
	raw := value.Load()
	if raw == nil {
		return ""
	}
	cause, _ := raw.(string)
	return heartbeatStopCause(cause)
}

func shouldStopItem(cause heartbeatStopCause) bool {
	switch cause {
	case heartbeatStopCauseLeaseLost,
		heartbeatStopCauseLeaseNotExtended,
		heartbeatStopCauseHeartbeatLost,
		heartbeatStopCausePermanentFailure:
		return false
	default:
		return true
	}
}

func isBenignWorkerExit(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, selfhosted.ErrIdleTimeout) ||
		errors.Is(err, selfhosted.ErrSessionTerminated)
}

type claimedWork struct {
	ID                string
	EnvironmentID     string
	SessionID         string
	LatestHeartbeatAt string
}

func claimedWorkFromItem(item selfhosted.WorkItem, fallbackEnvironmentID string) (claimedWork, error) {
	work := claimedWork{
		ID:                item.ID,
		EnvironmentID:     firstNonEmpty(item.EnvironmentID, fallbackEnvironmentID),
		SessionID:         item.SessionIDValue(),
		LatestHeartbeatAt: item.LatestHeartbeatValue(),
	}
	return validateClaimedWork(work)
}

func claimedWorkFromOptions(opts HandleItemOptions) (claimedWork, error) {
	workID := firstNonEmpty(opts.WorkID, os.Getenv("MA_WORK_ID"))
	environmentID := firstNonEmpty(opts.EnvironmentID, os.Getenv("MA_ENVIRONMENT_ID"))
	sessionID := firstNonEmpty(opts.SessionID, os.Getenv("MA_SESSION_ID"))
	latestHeartbeatAt := firstNonEmpty(opts.LatestHeartbeatAt, os.Getenv("MA_LATEST_HEARTBEAT_AT"))
	return validateClaimedWork(claimedWork{
		ID:                workID,
		EnvironmentID:     environmentID,
		SessionID:         sessionID,
		LatestHeartbeatAt: latestHeartbeatAt,
	})
}

func validateClaimedWork(work claimedWork) (claimedWork, error) {
	if work.ID == "" {
		return claimedWork{}, errors.New("environments: work id is required")
	}
	if work.EnvironmentID == "" {
		return claimedWork{}, errors.New("environments: environment id is required")
	}
	if work.SessionID == "" {
		return claimedWork{}, errors.New("environments: session id is required")
	}
	return work, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
