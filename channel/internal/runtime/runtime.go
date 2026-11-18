// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrUnsupportedContract 表示当前公开平台契约无法证明安全完成。
	ErrUnsupportedContract = errors.New("unsupported contract")
	// ErrRuntimeConfig 表示 Runtime 的显式依赖或时限无效。
	ErrRuntimeConfig = errors.New("invalid runtime configuration")
)

// Connection 把非密 Binding 与对应 MA、渠道端口显式绑定。
type Connection struct {
	// UserAuthorizer 是可选的本人授权端口。
	UserAuthorizer UserAuthorizer
	// PrepareSession 仅在首次冻结新建请求前解析实际版本和凭证资源，不用于旧 Session。
	PrepareSession func(context.Context, Binding) (Binding, error)
	// Release 归还解析器缓存租约；静态连接留空。
	Release  func()
	Binding  Binding
	MAClient MAClient
	Provider Provider
	// Outputs 是可选的会话产出文件端口；提供时收集并投递写入产出目录的文件（托管部署）。
	Outputs OutputFileResolver
}

// RuntimeOptions 指定有限并发、执行时限与投递预算。
//
// IdleTimeout 限定连续没有有效进展的时间，默认一小时；MessageDeadline 是兼容别名。
// UnknownWriteGrace 是远程写结果未知后、确认未生效前的等待时间，宽限期内
// 只做只读对账。
type RuntimeOptions struct {
	Resolver ConnectionResolver
	// IdentityRenewalInterval 是冻结 Bot 身份的后台检查间隔，默认一分钟。
	IdentityRenewalInterval time.Duration
	// IdentityRenewalConcurrency 是后台身份准备的并发上限，默认 4，最大 32。
	IdentityRenewalConcurrency int
	// CreateConcurrency、SubmitConcurrency、ObserveConcurrency 分别限制本进程三个执行池。
	// 任一非零启用分池模式，禁止与旧 Workers/AccountLimit/CreationLimit 混用。
	CreateConcurrency  int
	SubmitConcurrency  int
	ObserveConcurrency int
	// AccountLimit 仅供旧统一工作池使用；分池模式由 MA 管理账号配额。
	AccountLimit int
	// CreationLimit 限制账号未知创建数，默认跟随 AccountLimit。
	CreationLimit       int
	Workers             int
	PollInterval        time.Duration
	TurnTimeout         time.Duration
	LeaseTTL            time.Duration
	DeliveryTimeout     time.Duration
	DeliveryMaxAttempts uint64
	IdleTimeout         time.Duration
	// MessageDeadline 已改为无进展窗口；新调用方使用 IdleTimeout。
	MessageDeadline   time.Duration
	UnknownWriteGrace time.Duration
	Clock             func() time.Time
	// Observer 接收结构化观测记录；为空时丢弃。
	Observer Observer
	// Retention 是已结算记录的保留时长，到期由后台清理删除；去重索引随之删除，因此必须长于
	// 渠道的事件重投窗口。默认 7 天，最小 24 小时。
	Retention time.Duration
	// PruneInterval 是后台清理的执行间隔，默认 1 小时。
	PruneInterval time.Duration
	// ProgressInterval 是同一轮过程卡片两次更新的最小间隔，默认 2 秒（飞书单消息更新有频控）。
	ProgressInterval time.Duration
	// UserAuthPollInterval 是等待用户授权时首次复查的间隔，之后逐次翻倍并带抖动，上限 15 秒。
	// 默认 2 秒；不得超过 15 秒。
	UserAuthPollInterval time.Duration
	// Attachments 限制单条消息的附件数量与大小。
	Attachments AttachmentLimits
	// Context 限制新 Session 首轮注入的近期聊天背景。
	Context ContextLimits
	// Artifacts 限制单轮投递的产物数量与大小。
	Artifacts ArtifactLimits
	// Hooks 是受控扩展点；HookTimeout 默认 3 秒，HookConcurrency 默认 16。
	Hooks           Hooks
	HookTimeout     time.Duration
	HookConcurrency int
}

// Normalize 离线填充默认值并验证 Runtime 的有限执行边界。
func (options RuntimeOptions) Normalize() (RuntimeOptions, error) {
	var err error
	if err := options.normalizePools(); err != nil {
		return RuntimeOptions{}, err
	}
	if options.AccountLimit < 0 || options.CreationLimit < 0 {
		return RuntimeOptions{}, ErrRuntimeConfig
	}
	if options.CreationLimit == 0 {
		options.CreationLimit = options.AccountLimit
	}
	if options.IdentityRenewalInterval < 0 || options.IdentityRenewalInterval > 10*time.Minute || options.IdentityRenewalConcurrency < 0 || options.IdentityRenewalConcurrency > 32 {
		return RuntimeOptions{}, ErrRuntimeConfig
	}
	if options.IdentityRenewalInterval == 0 {
		options.IdentityRenewalInterval = time.Minute
	}
	if options.IdentityRenewalConcurrency == 0 {
		options.IdentityRenewalConcurrency = 4
	}
	if options.Workers < 0 || options.PollInterval < 0 || options.TurnTimeout < 0 || options.LeaseTTL < 0 || options.DeliveryTimeout < 0 || options.MessageDeadline < 0 || options.IdleTimeout < 0 || options.UnknownWriteGrace < 0 {
		return RuntimeOptions{}, ErrRuntimeConfig
	}
	if options.Workers == 0 && !options.staged() {
		options.Workers = 4
	}
	if options.PollInterval == 0 {
		options.PollInterval = 500 * time.Millisecond
	}
	if options.TurnTimeout == 0 {
		options.TurnTimeout = 5 * time.Minute
	}
	if options.LeaseTTL == 0 {
		options.LeaseTTL = 6 * time.Minute
		if options.TurnTimeout+time.Minute > options.LeaseTTL {
			options.LeaseTTL = options.TurnTimeout + time.Minute
		}
	}
	if options.DeliveryTimeout == 0 {
		options.DeliveryTimeout = 30 * time.Second
	}
	if options.DeliveryMaxAttempts == 0 {
		options.DeliveryMaxAttempts = 5
	}
	if options.UnknownWriteGrace == 0 {
		options.UnknownWriteGrace = 30 * time.Second
	}
	if options.IdleTimeout != 0 && options.MessageDeadline != 0 && options.IdleTimeout != options.MessageDeadline {
		return RuntimeOptions{}, ErrRuntimeConfig
	}
	if options.IdleTimeout == 0 {
		options.IdleTimeout = options.MessageDeadline
	}
	if options.IdleTimeout == 0 {
		options.IdleTimeout = time.Hour
	}
	options.MessageDeadline = options.IdleTimeout
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.Observer == nil {
		options.Observer = noopObserver{}
	}
	if options.Retention == 0 {
		options.Retention = 7 * 24 * time.Hour
	}
	if options.PruneInterval == 0 {
		options.PruneInterval = time.Hour
	}
	if options.Context, err = options.Context.normalize(); err != nil {
		return RuntimeOptions{}, err
	}
	if options.Attachments, err = options.Attachments.normalize(); err != nil {
		return RuntimeOptions{}, err
	}
	if options.Artifacts, err = options.Artifacts.normalize(); err != nil {
		return RuntimeOptions{}, err
	}
	if options.HookTimeout == 0 {
		options.HookTimeout = defaultHookTimeout
	}
	if options.HookConcurrency == 0 {
		options.HookConcurrency = 16
	}
	if options.HookTimeout < 0 || options.HookTimeout > time.Minute || options.HookConcurrency < 0 || options.HookConcurrency > 1024 {
		return RuntimeOptions{}, ErrRuntimeConfig
	}
	if options.ProgressInterval == 0 {
		options.ProgressInterval = 2 * time.Second
	}
	if options.UserAuthPollInterval == 0 {
		options.UserAuthPollInterval = 2 * time.Second
	}
	if options.UserAuthPollInterval < 0 || options.UserAuthPollInterval > maxUserAuthPollDelay {
		return RuntimeOptions{}, ErrRuntimeConfig
	}
	if options.ProgressInterval < 0 || options.ProgressInterval > time.Minute {
		return RuntimeOptions{}, ErrRuntimeConfig
	}
	if options.Retention < 24*time.Hour || options.Retention > 365*24*time.Hour || options.PruneInterval < 0 || options.PruneInterval > 24*time.Hour {
		return RuntimeOptions{}, ErrRuntimeConfig
	}
	if options.Workers > 32 || options.PollInterval > time.Minute || options.TurnTimeout > 30*time.Minute || options.LeaseTTL > time.Hour || options.LeaseTTL <= options.TurnTimeout || options.DeliveryTimeout >= options.LeaseTTL || options.DeliveryMaxAttempts > 100 ||
		options.UnknownWriteGrace > 10*time.Minute || options.MessageDeadline < options.TurnTimeout || options.MessageDeadline > 2*time.Hour {
		return RuntimeOptions{}, ErrRuntimeConfig
	}
	return options, nil
}

// Runtime 运行持久 Turn 队列与独立 Outbox，不拥有 Store 生命周期。
type Runtime struct {
	instanceID  string
	store       Store
	connections map[string]Connection
	versions    map[string]BindingVersion
	options     RuntimeOptions
	mu          sync.Mutex
	running     atomic.Bool
	runCtx      atomic.Pointer[context.Context]
	progressMu  sync.Mutex
	progress    map[string]*progressState
	hookSlots   chan struct{}
	submitSlots chan struct{}
	identities  map[string]*botIdentity
	// prompted 是进程内已展示审批卡片的缓存（turn → tool_use），只用于减少存储读写。
	prompted map[string]map[string]bool
}

// NewRuntime 验证连接，并确认持久数据引用的配置都能由已注册连接处理。
func NewRuntime(store Store, connections []Connection, options RuntimeOptions) (*Runtime, error) {
	if store == nil || len(connections) == 0 && options.Resolver == nil {
		return nil, ErrRuntimeConfig
	}
	var err error
	options, err = options.Normalize()
	if err != nil {
		return nil, err
	}
	var identity [16]byte
	if _, err := io.ReadFull(rand.Reader, identity[:]); err != nil {
		return nil, fmt.Errorf("generate runtime identity: %w", err)
	}
	r := &Runtime{instanceID: fmt.Sprintf("%x", identity), store: store, connections: make(map[string]Connection), versions: make(map[string]BindingVersion), options: options, progress: make(map[string]*progressState), hookSlots: make(chan struct{}, options.HookConcurrency), identities: make(map[string]*botIdentity)}
	if options.staged() {
		r.submitSlots = make(chan struct{}, options.SubmitConcurrency)
	}
	apps := make(map[string]bool)
	for _, connection := range connections {
		binding := connection.Binding
		if (binding.ProgressMode != "" && binding.ProgressMode != "status_only" && binding.ProgressMode != "tools") || binding.ID == "" || binding.AppID == "" || binding.AgentID == "" || binding.AgentVersion == "" || connection.MAClient == nil || connection.Provider == nil || apps[binding.AppID] || (binding.GroupReplyMode != "" && binding.GroupReplyMode != "chat" && binding.GroupReplyMode != "reply" && binding.GroupReplyMode != "thread") {
			return nil, ErrRuntimeConfig
		}
		if _, exists := r.connections[binding.ID]; exists {
			return nil, ErrRuntimeConfig
		}
		apps[binding.AppID] = true
		validation := binding
		if _, ok := store.(UserAuthStore); ok && connection.UserAuthorizer != nil && binding.Identity.P2P == IdentityUser {
			validation.Identity.P2P = ""
		}
		if err := validateIdentity(validation); err != nil {
			return nil, fmt.Errorf("binding %s identity: %w", binding.ID, err)
		}
		r.connections[binding.ID] = connection
		r.versions[binding.ID] = NewBindingVersion(binding)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if options.Resolver == nil {
		if err := r.validatePersisted(ctx); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// validatePersisted 检查持久配置版本没有被换到别的应用上，且未结算的 Turn 与未完成的投递
// 都属于已注册连接。只读取未结算记录，开销与历史数据量无关。
func (r *Runtime) validatePersisted(ctx context.Context) error {
	versions, err := r.store.ListBindingVersions(ctx)
	if err != nil {
		return err
	}
	for _, version := range versions {
		if registered, ok := r.connections[version.BindingID]; ok && registered.Binding.AppID != version.Content.AppID {
			return fmt.Errorf("binding %s app changed: %w", version.BindingID, ErrRuntimeConfig)
		}
	}
	for after := ""; ; {
		turns, err := r.store.ListTurns(ctx, TurnQuery{OpenOnly: true, After: after, Limit: MaxPageSize})
		if err != nil {
			return err
		}
		for _, turn := range turns {
			if err := turnWorkError(turn); isUnsupportedWork(err) {
				r.report(ctx, Observation{Name: EventWorkBlocked, Status: "blocked", ErrorKind: "unsupported_work", BindingID: turn.BindingID, TurnID: turn.ID, ConversationID: turn.ConversationID})
				continue
			}
			if _, ok := r.connections[turn.BindingID]; !ok {
				return fmt.Errorf("binding %s: %w", turn.BindingID, ErrRuntimeConfig)
			}
		}
		if len(turns) < MaxPageSize {
			break
		}
		after = turns[len(turns)-1].ID
	}
	for after := ""; ; {
		deliveries, err := r.store.ListDeliveries(ctx, DeliveryQuery{OpenOnly: true, After: after, Limit: MaxPageSize})
		if err != nil {
			return err
		}
		for _, delivery := range deliveries {
			if err := deliveryWorkError(delivery); isUnsupportedWork(err) {
				r.report(ctx, Observation{Name: EventWorkBlocked, Status: "blocked", ErrorKind: "unsupported_work", BindingID: delivery.BindingID, DeliveryID: delivery.ID})
				continue
			}
			if _, ok := r.connections[delivery.BindingID]; !ok {
				return fmt.Errorf("binding %s: %w", delivery.BindingID, ErrRuntimeConfig)
			}
		}
		if len(deliveries) < MaxPageSize {
			break
		}
		after = deliveries[len(deliveries)-1].ID
	}
	return nil
}

// validateFrozen 确认冻结配置可以由已注册连接执行：连接存在且应用未变。
func (r *Runtime) validateFrozen(binding Binding) error {
	connection, ok := r.connections[binding.ID]
	if !ok || binding.AppID != connection.Binding.AppID {
		return fmt.Errorf("binding %s: %w", binding.ID, ErrRuntimeConfig)
	}
	return nil
}

// Enqueue 在渠道回调返回前持久化入站消息；识别出的 /stop 在同一事务内生效。
func (r *Runtime) Enqueue(ctx context.Context, bindingID string, message Message) (IngestResult, error) {
	return r.EnqueueScoped(ctx, IngestScope{BindingID: bindingID}, message)
}

// EnqueueScoped 在宿主验证身份后按租户和绑定代数持久受理消息。
func (r *Runtime) EnqueueScoped(ctx context.Context, scope IngestScope, message Message) (IngestResult, error) {
	bindingID := scope.BindingID
	version, ok := r.versions[bindingID]
	if r.options.Resolver != nil {
		resolved, err := r.options.Resolver.ResolveIngest(ctx, scope)
		if err != nil {
			return IngestResult{}, err
		}
		defer releaseConnection(resolved.Connection)
		binding := resolved.Connection.Binding
		if binding.ID != bindingID || scope.Owner != "" && binding.Owner != scope.Owner || scope.BindingEpoch != 0 && binding.BindingEpoch != scope.BindingEpoch {
			return IngestResult{}, ErrRuntimeConfig
		}
		if resolved.AdmitUntil.IsZero() || !r.options.Clock().Before(resolved.AdmitUntil) {
			return IngestResult{}, ErrAdmissionExpired
		}
		version = NewBindingVersion(binding)
		message.AdmitUntil = resolved.AdmitUntil
		ok = true
	}
	if !ok || message.BindingID != bindingID {
		return IngestResult{}, ErrRuntimeConfig
	}

	result, err := r.store.Ingest(ctx, version, message)
	route := RouteFor(message)
	switch {
	case err != nil:
		r.report(ctx, Observation{Name: EventIngress, Status: "rejected", ErrorKind: ingressErrorKind(err), BindingID: bindingID})
		return result, err
	case result.Ignored:
		r.report(ctx, Observation{Name: EventIngress, Status: "ignored", BindingID: bindingID})
		return result, nil
	case result.Rejected:
		r.report(ctx, Observation{Name: EventIngress, Status: "rejected", ErrorKind: "queue_full", BindingID: bindingID, ConversationID: route})
		return result, nil
	case result.Duplicate:
		r.report(ctx, Observation{Name: EventIngress, Status: "duplicate", BindingID: bindingID, TurnID: turnIDOf(result), ConversationID: route})
		return result, nil
	}
	r.report(ctx, Observation{Name: EventIngress, Status: "accepted", BindingID: bindingID, TurnID: turnIDOf(result), ConversationID: route})
	if result.Control != nil {
		if result.Stopped != nil {
			r.report(ctx, Observation{Name: EventStopRequest, Status: "applied", BindingID: bindingID, TurnID: result.Stopped.ID, ConversationID: route})
		} else {
			r.report(ctx, Observation{Name: EventStopRequest, Status: "no_target", BindingID: bindingID, ConversationID: route})
		}
	}
	return result, nil
}

func turnIDOf(result IngestResult) string {
	if result.Turn != nil {
		return result.Turn.ID
	}
	return ""
}

// Run 启动 Provider、Turn worker 和独立投递 worker，取消后等待退出。
func (r *Runtime) Run(ctx context.Context) error {
	if !r.running.CompareAndSwap(false, true) {
		return ErrRuntimeConfig
	}
	defer r.running.Store(false)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	r.runCtx.Store(&runCtx)
	var wg sync.WaitGroup
	errorsCh := make(chan error, len(r.connections)+r.options.Workers+1)
	start := func(name string, fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if recover() != nil {
					select {
					case errorsCh <- fmt.Errorf("%s panic", name):
					default:
					}
				}
			}()
			if err := fn(); err != nil && runCtx.Err() == nil {
				select {
				case errorsCh <- fmt.Errorf("%s: %w", name, err):
				default:
				}
			}
		}()
	}
	for id, connection := range r.connections {
		bindingID, provider := id, connection.Provider
		if binder, ok := providerPort[ControlBinder](provider); ok {
			binder.BindControl(r)
		}
		start("provider", func() error {
			err := provider.Start(runCtx, func(ctx context.Context, message Message) error {
				_, err := r.Enqueue(ctx, bindingID, message)
				return err
			})
			if err == nil && runCtx.Err() == nil {
				return errors.New("provider stopped")
			}
			return err
		})
	}
	if r.options.staged() {
		for _, pool := range []struct {
			stage TurnStage
			limit int
		}{
			{StageCreate, r.options.CreateConcurrency},
			{StageSubmit, r.options.SubmitConcurrency},
			{StageObserve, r.options.ObserveConcurrency},
		} {
			start(string(pool.stage)+" pool", func() error { return r.dispatchTurns(runCtx, pool.stage, pool.limit, r.executeTurn) })
		}
	} else {
		for i := 0; i < r.options.Workers; i++ {
			owner := fmt.Sprintf("turn-worker-%s-%d", r.instanceID, i)
			start("turn worker", func() error { return r.turnWorker(runCtx, owner) })
		}
	}
	start("delivery worker", func() error { return r.deliveryWorker(runCtx, "delivery-worker-"+r.instanceID) })
	start("pruner", func() error { r.pruneLoop(runCtx); return nil })
	start("identity", func() error { return r.identityLoop(runCtx) })
	start("remote reconciler", func() error { return r.remoteLoop(runCtx) })
	var err error
	select {
	case <-ctx.Done():
	case err = <-errorsCh:
	}
	cancel()
	wg.Wait()
	return err
}

func (r *Runtime) turnWorker(ctx context.Context, owner string) error {
	claim := ClaimOptions{UserAuth: r.supportsUserAuth(), Owner: owner, TTL: r.options.LeaseTTL, MessageDeadline: r.options.MessageDeadline, AccountLimit: r.options.AccountLimit}
	for ctx.Err() == nil {
		leased, err := r.store.LeaseTurn(ctx, claim)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if isUnsupportedWork(err) {
				if r.waitUnsupported(ctx, Observation{}) != nil {
					return nil
				}
				continue
			}
			if errors.Is(err, ErrLeaseLost) {
				if r.waitPoll(ctx) != nil {
					return nil
				}
				continue
			}
			return err
		}
		if leased == nil {
			if err := r.waitPoll(ctx); err != nil {
				return nil
			}
			continue
		}
		if err := r.executeTurn(ctx, leased, StageAny); err != nil {
			return err
		}
	}
	return nil
}

// executeTurn 统一处理租约执行、停机归还与基础设施故障，所有执行池共用。
func (r *Runtime) executeTurn(ctx context.Context, leased *TurnLease, stage TurnStage) error {
	if err := turnWorkError(leased.Turn); err != nil {
		if !isUnsupportedWork(err) {
			return err
		}
		if r.waitUnsupported(ctx, Observation{BindingID: leased.Turn.BindingID, TurnID: leased.Turn.ID, ConversationID: leased.Turn.ConversationID}) != nil {
			return nil
		}
		return nil
	}
	if r.options.Resolver == nil {
		if err := r.validateFrozen(leased.Binding.Content); err != nil {
			return err
		}
	}
	j := &job{stage: stage, Turn: leased.Turn, msg: leased.Message.Message, binding: leased.Binding.Content}
	err := r.runLeasedTurn(ctx, j)
	if isUnsupportedWork(err) {
		if r.waitUnsupported(ctx, Observation{BindingID: j.BindingID, TurnID: j.ID, ConversationID: j.ConversationID}) != nil {
			return nil
		}
		return nil
	}
	if ctx.Err() != nil {
		// 停机中断了本轮（可能正处在进度写入中）：立即交还租约，重启后无需等待租约过期
		// 即可继续。已结算的 Turn 会返回租约失效，忽略即可。
		releaseCtx, cancel := shortContext()
		_ = r.store.ReleaseTurn(releaseCtx, j.Lease)
		cancel()
		return nil
	}
	if errors.Is(err, ErrLeaseLost) {
		return nil
	}
	if isStoreFailure(err) {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		// 轮次时限在 store 读写途中耗尽（常见于临近消息截止时的最后一次领取）：这不是
		// 存储故障，交还租约后立即重新领取，由 pastDeadline 按截止原因结算。
		releaseCtx, cancel := shortContext()
		_ = r.store.ReleaseTurn(releaseCtx, j.Lease)
		cancel()
		return nil
	}
	if err != nil {
		return err
	}
	return nil
}

// pruneLoop 按间隔删除超过保留期的已结算记录；失败只上报，不终止 Runtime。
func (r *Runtime) pruneLoop(ctx context.Context) {
	timer := time.NewTimer(r.options.PruneInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		r.PruneOnce(ctx)
		timer.Reset(r.options.PruneInterval)
	}
}

// PruneOnce 立即执行一次保留期清理并返回删除结果。
func (r *Runtime) PruneOnce(ctx context.Context) PruneResult {
	callCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	removed, err := r.store.Prune(callCtx, r.options.Clock().Add(-r.options.Retention))
	if err != nil {
		r.report(ctx, Observation{Name: EventCleanup, Status: "failed", ErrorKind: "store_error"})
		return PruneResult{}
	}
	if count := removed.Turns + removed.Controls; count > 0 {
		r.report(ctx, Observation{Name: EventCleanup, Status: "pruned", Count: count})
	}
	return removed
}

func ingressErrorKind(err error) string {
	switch {
	case errors.Is(err, ErrQueueFull):
		return "queue_full"
	case errors.Is(err, ErrConflict):
		return "conflict"
	case errors.Is(err, ErrInvalid):
		return "invalid"
	}
	return "store_error"
}

func (r *Runtime) waitPoll(ctx context.Context) error {
	timer := time.NewTimer(r.options.PollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *Runtime) supportsUserAuth() bool {
	if _, ok := r.store.(UserAuthStore); !ok {
		return false
	}
	if r.options.Resolver != nil {
		return true
	}
	for _, c := range r.connections {
		if c.UserAuthorizer != nil {
			return true
		}
	}
	return false
}
