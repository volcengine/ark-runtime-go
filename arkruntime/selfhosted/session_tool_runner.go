// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package selfhosted

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime/internal/selfhostedlog"
	"github.com/volcengine/ark-runtime-go/arkruntime/toolset"
)

const (
	sessionRunnerStreamBackoffStart = 500 * time.Millisecond
	sessionRunnerStreamBackoffCap   = 10 * time.Second
	sessionRunnerStreamHealthyAfter = 30 * time.Second
	sessionRunnerSendRetries        = 3
	sessionRunnerResultsBuffer      = 32

	defaultSessionRunnerSendTimeout  = 15 * time.Second
	defaultSessionRunnerDrainTimeout = 30 * time.Second
)

var (
	// ErrSessionTerminated 表示 session 已由控制面终止。
	ErrSessionTerminated = errors.New("session terminated")
	// ErrIdleTimeout 表示 session 在 idle 后超过 MaxIdle。
	ErrIdleTimeout = errors.New("session idle after end_turn")
)

// ToolCallStoreDecision 是持久化 store 对某个 tool_use 的裁决。
type ToolCallStoreDecision struct {
	Sent   bool
	Result Event
}

// ToolResultStore 持久化 tool_use 执行状态，避免重启后重复执行有副作用的工具。
type ToolResultStore interface {
	Recover() (map[string]Event, map[string]bool, error)
	Begin(callID string, event Event) (ToolCallStoreDecision, error)
	SaveResult(callID string, result Event) error
	MarkSent(callID string) error
}

// SessionToolRunnerOptions 配置单 session 的 tool event loop。
type SessionToolRunnerOptions struct {
	WorkID string
	// Deprecated: MA send events 接口不再接收 lease_id。
	LeaseID           string
	Tools             *toolset.Set
	CustomTools       map[string]toolset.Tool
	ResultStore       ToolResultStore
	EventPage         string
	EventPollInterval time.Duration
	EventLimit        int
	MaxIdle           *time.Duration
	ToolTimeout       time.Duration
	SendTimeout       time.Duration
	DrainTimeout      time.Duration
	Logger            *log.Logger
	OnToolError       func(Event, error)
}

// ToolCallResult 是一次 tool call 执行并回写后的结果。
type ToolCallResult struct {
	ToolUseID    string
	Name         string
	Custom       bool
	Confirmation string
	Posted       bool
	Event        Event
	Result       Event
}

// SessionToolRunner 执行一个 session 的 tool event loop。
type SessionToolRunner struct {
	ctx     context.Context
	cancel  context.CancelFunc
	api     API
	opts    SessionToolRunnerOptions
	session string
	events  chan ToolCallResult
	logger  *selfhostedlog.Logger

	started bool
	current ToolCallResult
	err     error
	done    chan struct{}

	inFlight sync.WaitGroup
}

// NewSessionToolRunner 创建单 session tool runner；第一次 Next 时开始消费事件。
func NewSessionToolRunner(ctx context.Context, api API, sessionID string, opts SessionToolRunnerOptions) *SessionToolRunner {
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	logger := selfhostedlog.New(opts.Logger)
	r := &SessionToolRunner{
		ctx:     runCtx,
		cancel:  cancel,
		api:     api,
		opts:    opts,
		session: sessionID,
		logger: logger.With(
			"component", "session-tool-runner",
			"work_id", opts.WorkID,
			"session_id", sessionID,
		),
	}
	return r
}

// Next 阻塞直到下一次 tool call 结果可用或 runner 结束。
func (r *SessionToolRunner) Next() bool {
	r.start()
	result, ok := <-r.events
	if !ok {
		return false
	}
	r.current = result
	return true
}

// Current 返回最近一次 Next 产出的 tool call 结果。
func (r *SessionToolRunner) Current() ToolCallResult {
	return r.current
}

// Err 返回 runner 结束原因。
func (r *SessionToolRunner) Err() error {
	return r.err
}

// Close 取消 runner，并等待后台循环完成。
func (r *SessionToolRunner) Close() error {
	r.cancel()
	if !r.started || r.events == nil {
		return nil
	}
	for range r.events {
	}
	return nil
}

func (r *SessionToolRunner) start() {
	if r.started {
		return
	}
	r.started = true
	r.events = make(chan ToolCallResult, sessionRunnerResultsBuffer)
	r.done = make(chan struct{})
	go r.run()
}

func (r *SessionToolRunner) run() {
	defer close(r.done)
	defer close(r.events)
	defer r.drainInFlight()
	r.err = normalizeRunnerErr(r.runLoop())
}

func (r *SessionToolRunner) runLoop() error {
	if r.api == nil {
		return errors.New("session tool runner api must not be nil")
	}
	if r.session == "" {
		return errors.New("session id must not be empty")
	}
	if r.opts.Tools == nil {
		return errors.New("session tool runner tools must not be nil")
	}
	state := &toolRunnerState{
		runner:         r,
		page:           r.opts.EventPage,
		processed:      map[string]bool{},
		seen:           map[string]bool{},
		answered:       map[string]bool{},
		pendingResults: map[string]Event{},
		pendingAsk:     map[string]Event{},
		confirmations:  map[string]Event{},
		externalTools:  map[string]Event{},
	}
	if r.opts.ResultStore != nil {
		pending, processed, err := r.opts.ResultStore.Recover()
		if err != nil {
			return fmt.Errorf("recover tool result store: %w", err)
		}
		state.pendingResults = pending
		state.processed = processed
		for id := range processed {
			state.answered[id] = true
		}
	}
	if streamer, ok := r.api.(EventStreamer); ok {
		if err := state.consumeStreamLoop(r.ctx, streamer); err != nil {
			switch {
			case errors.Is(err, ErrEventStreamUnsupported):
			case errors.Is(err, ErrIdleTimeout),
				errors.Is(err, ErrSessionTerminated),
				errors.Is(err, context.Canceled),
				errors.Is(err, context.DeadlineExceeded):
				return err
			default:
				return err
			}
		}
	}
	return state.consumeList(r.ctx)
}

type toolRunnerState struct {
	runner         *SessionToolRunner
	page           string
	processed      map[string]bool
	seen           map[string]bool
	answered       map[string]bool
	pendingResults map[string]Event
	pendingAsk     map[string]Event
	confirmations  map[string]Event
	externalTools  map[string]Event
	idleArmedAt    time.Time
	idleArmPending bool
}

type pendingToolEvent struct {
	event  Event
	custom bool
}

func (s *toolRunnerState) consumeStreamLoop(ctx context.Context, streamer EventStreamer) error {
	backoff := sessionRunnerStreamBackoffStart
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		stream, err := streamer.StreamEvents(ctx, StreamEventsRequest{
			SessionID: s.runner.session,
		})
		if err != nil {
			if errors.Is(err, ErrEventStreamUnsupported) {
				return err
			}
			if isFatal4xxStatus(err) {
				return fmt.Errorf("stream events: %w", err)
			}
			s.runner.logger.Warn("open event stream failed, retrying", "err", err, "backoff", backoff)
			if err := s.sleepOrIdle(ctx, jitterDuration(backoff)); err != nil {
				return err
			}
			backoff = minDuration(backoff*2, sessionRunnerStreamBackoffCap)
			continue
		}
		if err := s.reconcile(ctx); err != nil {
			_ = stream.Close()
			return err
		}
		if ctx.Err() != nil {
			_ = stream.Close()
			return ctx.Err()
		}
		connectedAt := time.Now()
		err = s.consumeStream(ctx, stream, connectedAt, &backoff)
		_ = stream.Close()
		if err != nil {
			if errors.Is(err, ErrIdleTimeout) ||
				errors.Is(err, ErrSessionTerminated) ||
				errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if isFatal4xxStatus(err) {
				return fmt.Errorf("stream events: %w", err)
			}
			s.runner.logger.Warn("event stream disconnected, retrying", "err", err, "backoff", backoff)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := s.sleepOrIdle(ctx, jitterDuration(backoff)); err != nil {
			return err
		}
		backoff = minDuration(backoff*2, sessionRunnerStreamBackoffCap)
	}
}

func (s *toolRunnerState) consumeStream(ctx context.Context, stream *EventStream, connectedAt time.Time, backoff *time.Duration) error {
	if stream == nil {
		return nil
	}
	idle := s.maxIdle()
	var timer *time.Timer
	var timerC <-chan time.Time
	if idle > 0 {
		timer = time.NewTimer(time.Hour)
		if !timer.Stop() {
			<-timer.C
		}
		defer timer.Stop()
	}
	resetIdle := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerC = nil
		if !s.idleArmedAt.IsZero() {
			remaining := idle - time.Since(s.idleArmedAt)
			if remaining < time.Millisecond {
				remaining = time.Millisecond
			}
			timer.Reset(remaining)
			timerC = timer.C
		}
	}
	for {
		if err := s.flushResults(ctx); err != nil {
			s.runner.logger.Warn("send pending tool result failed", "err", err)
		}
		resetIdle()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timerC:
			if s.idleExpired() {
				return ErrIdleTimeout
			}
			resetIdle()
		case event, ok := <-stream.Events():
			if !ok {
				return stream.Err()
			}
			if time.Since(connectedAt) > sessionRunnerStreamHealthyAfter {
				*backoff = sessionRunnerStreamBackoffStart
			}
			if err := s.handleStreamEvent(ctx, event); err != nil {
				return err
			}
		}
	}
}

func (s *toolRunnerState) reconcile(ctx context.Context) error {
	backoff := sessionRunnerStreamBackoffStart
	for {
		err := s.reconcileOnce(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if isFatal4xxStatus(err) {
			return fmt.Errorf("reconcile list events: %w", err)
		}
		s.runner.logger.Warn("reconcile list events failed, retrying", "err", err, "backoff", backoff)
		if err := s.sleepOrIdle(ctx, jitterDuration(backoff)); err != nil {
			return err
		}
		backoff = minDuration(backoff*2, sessionRunnerStreamBackoffCap)
	}
}

func (s *toolRunnerState) reconcileOnce(ctx context.Context) error {
	page := ""
	limit := s.runner.opts.EventLimit
	if limit <= 0 {
		limit = 1000
	}
	var events []Event
	for {
		resp, err := s.runner.api.ListEvents(ctx, ListEventsRequest{
			SessionID: s.runner.session,
			Page:      page,
			Limit:     limit,
			Order:     EventListOrderAsc,
		})
		if err != nil {
			return err
		}
		if resp != nil {
			events = append(events, resp.Events...)
			if resp.NextPage != "" {
				page = resp.NextPage
				continue
			}
		}
		return s.processListedEvents(ctx, events, true)
	}
}

func (s *toolRunnerState) sleepOrIdle(ctx context.Context, d time.Duration) error {
	deadline := time.Now().Add(d)
	for {
		if s.idleExpired() {
			return ErrIdleTimeout
		}
		remainingSleep := time.Until(deadline)
		if remainingSleep <= 0 {
			return nil
		}
		wait := remainingSleep
		if !s.idleArmedAt.IsZero() {
			if remainingIdle := s.maxIdle() - time.Since(s.idleArmedAt); remainingIdle > 0 && remainingIdle < wait {
				wait = remainingIdle
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *toolRunnerState) consumeList(ctx context.Context) error {
	interval := s.runner.opts.EventPollInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	limit := s.runner.opts.EventLimit
	if limit <= 0 {
		limit = 100
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var timer *time.Timer
	var timerC <-chan time.Time
	if s.maxIdle() > 0 {
		timer = time.NewTimer(time.Hour)
		if !timer.Stop() {
			<-timer.C
		}
		defer timer.Stop()
	}
	resetIdle := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerC = nil
		if !s.idleArmedAt.IsZero() {
			remaining := s.maxIdle() - time.Since(s.idleArmedAt)
			if remaining < time.Millisecond {
				remaining = time.Millisecond
			}
			timer.Reset(remaining)
			timerC = timer.C
		}
	}
	for {
		if err := s.flushResults(ctx); err != nil {
			s.runner.logger.Warn("send pending tool result failed", "err", err)
		}
		var events []Event
		page := s.page
		listed := false
		listFailed := false
		for {
			resp, err := s.runner.api.ListEvents(ctx, ListEventsRequest{
				SessionID: s.runner.session,
				Page:      page,
				Limit:     limit,
				Order:     EventListOrderAsc,
			})
			if err != nil {
				s.runner.logger.Warn("list events failed", "err", err)
				listFailed = true
				break
			}
			listed = true
			if resp == nil {
				break
			}
			events = append(events, resp.Events...)
			if resp.NextPage == "" {
				page = ""
				break
			}
			page = resp.NextPage
		}
		if len(events) > 0 {
			if err := s.processListedEvents(ctx, events, false); err != nil {
				return err
			}
		}
		if listed && !listFailed && page == "" {
			// MA list events 没有可从响应事件安全推进的稳定 cursor。
			// fallback 模式每轮重读完整历史，并由 seen/answered 去重，
			// 避免使用 worker 本地时钟造成新事件永久漏读。
			s.page = ""
		} else if listed && !listFailed {
			s.page = page
		}
		if s.idleExpired() {
			return ErrIdleTimeout
		}
		resetIdle()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timerC:
			if s.idleExpired() {
				return ErrIdleTimeout
			}
		case <-ticker.C:
		}
	}
}

func (s *toolRunnerState) processListedEvents(ctx context.Context, events []Event, reconcile bool) error {
	var pending []pendingToolEvent
	pendingIDs := map[string]bool{}
	touchedIdle := false
	lastWasEndTurn := false
	for _, event := range events {
		seenNow := s.markEventSeen(event)
		if !reconcile && !seenNow {
			continue
		}
		if event.Type != EventTypeUserToolConfirmation {
			touchedIdle = true
			lastWasEndTurn = event.Type == EventTypeSessionStatusIdle &&
				event.StopReasonType() == SessionStopReasonEndTurn
		}
		switch event.Type {
		case EventTypeUserToolConfirmation:
			s.recordConfirmation(event)
		case EventTypeUserToolResult, EventTypeUserCustomToolResult:
			s.markAnswered(toolResultCallID(event))
		case EventTypeAgentToolUse:
			callID := toolUseCallID(event)
			if !pendingIDs[callID] {
				pending = append(pending, pendingToolEvent{event: event})
				pendingIDs[callID] = true
			}
		case EventTypeAgentCustomToolUse:
			callID := toolUseCallID(event)
			if !pendingIDs[callID] {
				pending = append(pending, pendingToolEvent{event: event, custom: true})
				pendingIDs[callID] = true
			}
		case EventTypeSessionStatusTerminated, EventTypeSessionDeleted:
			return ErrSessionTerminated
		}
	}
	if touchedIdle {
		s.disarmIdle()
	}
	for _, toolEvent := range pending {
		if s.isAnswered(toolUseCallID(toolEvent.event)) {
			continue
		}
		if err := s.handleToolUse(ctx, toolEvent.event, toolEvent.custom); err != nil {
			return err
		}
	}
	if err := s.releaseConfirmedToolUses(ctx); err != nil {
		return err
	}
	if touchedIdle && lastWasEndTurn {
		if s.hasUnblockedOutstandingTool(pending) {
			s.disarmIdle()
		} else {
			s.armIdle()
		}
	}
	return nil
}

func (s *toolRunnerState) noteIdleEvent(event Event) {
	if event.Type == EventTypeUserToolConfirmation {
		return
	}
	if event.Type == EventTypeSessionStatusIdle && event.StopReasonType() == SessionStopReasonEndTurn {
		s.armIdle()
		return
	}
	s.disarmIdle()
}

func (s *toolRunnerState) handleStreamEvent(ctx context.Context, event Event) error {
	if !s.markEventSeen(event) {
		return nil
	}
	s.noteIdleEvent(event)
	return s.handleEvent(ctx, event)
}

func (s *toolRunnerState) armIdle() {
	if s.maxIdle() <= 0 {
		return
	}
	if s.hasIdleBlockers() {
		s.idleArmPending = true
		s.idleArmedAt = time.Time{}
		return
	}
	s.idleArmPending = false
	s.idleArmedAt = time.Now()
}

func (s *toolRunnerState) disarmIdle() {
	s.idleArmPending = false
	s.idleArmedAt = time.Time{}
}

func (s *toolRunnerState) maybeArmPendingIdle() {
	if !s.idleArmPending || s.hasIdleBlockers() {
		return
	}
	s.idleArmPending = false
	s.idleArmedAt = time.Now()
}

func (s *toolRunnerState) hasIdleBlockers() bool {
	return len(s.pendingAsk) > 0 || len(s.pendingResults) > 0 || len(s.externalTools) > 0
}

func (s *toolRunnerState) idleExpired() bool {
	return s.maxIdle() > 0 && !s.idleArmedAt.IsZero() && time.Since(s.idleArmedAt) >= s.maxIdle()
}

func (s *toolRunnerState) handleEvent(ctx context.Context, event Event) error {
	switch event.Type {
	case EventTypeUserToolConfirmation:
		s.recordConfirmation(event)
		return s.releaseConfirmedToolUses(ctx)
	case EventTypeUserToolResult, EventTypeUserCustomToolResult:
		s.markAnswered(toolResultCallID(event))
	case EventTypeAgentToolUse:
		return s.handleToolUse(ctx, event, false)
	case EventTypeAgentCustomToolUse:
		return s.handleToolUse(ctx, event, true)
	case EventTypeSessionStatusTerminated, EventTypeSessionDeleted:
		return ErrSessionTerminated
	}
	return nil
}

func (s *toolRunnerState) markEventSeen(event Event) bool {
	key := event.ID
	if key == "" {
		key = toolUseCallID(event)
	}
	if key == "" {
		return true
	}
	if s.seen[key] {
		return false
	}
	s.seen[key] = true
	return true
}

func (s *toolRunnerState) markAnswered(callID string) {
	if callID == "" {
		return
	}
	s.answered[callID] = true
	s.processed[callID] = true
	delete(s.pendingResults, callID)
	delete(s.pendingAsk, callID)
	delete(s.externalTools, callID)
	s.maybeArmPendingIdle()
}

func (s *toolRunnerState) isAnswered(callID string) bool {
	return callID != "" && s.answered[callID]
}

func (s *toolRunnerState) recordConfirmation(event Event) {
	callID := toolConfirmationCallID(event)
	if callID == "" || s.isAnswered(callID) {
		return
	}
	s.confirmations[callID] = event
}

func (s *toolRunnerState) releaseConfirmedToolUses(ctx context.Context) error {
	var ready []Event
	for callID, event := range s.pendingAsk {
		if _, ok := s.confirmations[callID]; ok {
			ready = append(ready, event)
		}
	}
	for _, event := range ready {
		if err := s.handleToolUse(ctx, event, event.Type == EventTypeAgentCustomToolUse); err != nil {
			return err
		}
	}
	return nil
}

func (s *toolRunnerState) hasUnblockedOutstandingTool(pending []pendingToolEvent) bool {
	for _, toolEvent := range pending {
		callID := toolUseCallID(toolEvent.event)
		if callID == "" || s.isAnswered(callID) {
			continue
		}
		if _, ok := s.pendingAsk[callID]; ok {
			continue
		}
		if _, ok := s.pendingResults[callID]; ok {
			continue
		}
		return true
	}
	return false
}

func (s *toolRunnerState) handleToolUse(ctx context.Context, event Event, custom bool) error {
	callID := toolUseCallID(event)
	if callID == "" || s.isAnswered(callID) {
		return nil
	}
	if pending := s.pendingResults[callID]; pending.ID != "" {
		return s.sendResult(ctx, callID, event, custom, "", pending)
	}
	if !s.ownsTool(event, custom) {
		s.runner.logger.Info("tool not owned by this runner", "tool_use_id", callID, "tool", event.Name, "custom", custom)
		return s.skipExternalToolUse(ctx, event, custom, callID)
	}
	confirmation, allowed, err := s.permissionAllows(ctx, event, custom, callID)
	if err != nil || !allowed {
		return err
	}
	if s.runner.opts.ResultStore != nil {
		decision, err := s.runner.opts.ResultStore.Begin(callID, event)
		if err != nil {
			return fmt.Errorf("begin tool result store for %s: %w", callID, err)
		}
		if decision.Sent {
			s.markAnswered(callID)
			return nil
		}
		if decision.Result.ID != "" {
			s.pendingResults[callID] = decision.Result
			return s.sendResult(ctx, callID, event, custom, "", decision.Result)
		}
	}
	var result toolset.Result
	input := json.RawMessage(event.Input)
	if custom {
		tool := s.runner.opts.CustomTools[event.Name]
		result = s.executeWithTimeout(ctx, event, func(toolCtx context.Context) toolset.Result {
			return tool.Execute(toolCtx, input)
		})
	} else {
		result = s.executeWithTimeout(ctx, event, func(toolCtx context.Context) toolset.Result {
			return s.runner.opts.Tools.Execute(toolCtx, event.Name, input)
		})
	}
	return s.postResult(ctx, event, custom, callID, result, confirmation)
}

func (s *toolRunnerState) ownsTool(event Event, custom bool) bool {
	if custom {
		return s.runner.opts.CustomTools[event.Name] != nil
	}
	return s.runner.opts.Tools.Has(event.Name)
}

func (s *toolRunnerState) permissionAllows(ctx context.Context, event Event, custom bool, callID string) (string, bool, error) {
	if custom {
		return "", true, nil
	}
	switch event.EvaluatedPermission {
	case "":
		return "", true, nil
	case PermissionAllow:
		return "", true, nil
	case PermissionAsk:
		confirmation, ok := s.confirmations[callID]
		if !ok {
			s.pendingAsk[callID] = event
			return "", false, nil
		}
		switch confirmation.Result {
		case ConfirmationAllow:
			return "allow", true, nil
		case ConfirmationDeny:
			return ConfirmationDeny, false, s.resolveToolUseWithoutPost(ctx, event, false, callID, ConfirmationDeny)
		default:
			return ConfirmationDeny, false, s.resolveToolUseWithoutPost(ctx, event, false, callID, ConfirmationDeny)
		}
	case PermissionDeny:
		return ConfirmationDeny, false, s.resolveToolUseWithoutPost(ctx, event, false, callID, ConfirmationDeny)
	default:
		confirmation, ok := s.confirmations[callID]
		if !ok {
			s.pendingAsk[callID] = event
			return "", false, nil
		}
		if confirmation.Result == ConfirmationAllow {
			return "allow", true, nil
		}
		return ConfirmationDeny, false, s.resolveToolUseWithoutPost(ctx, event, false, callID, ConfirmationDeny)
	}
}

func (s *toolRunnerState) executeWithTimeout(ctx context.Context, event Event, fn func(context.Context) toolset.Result) toolset.Result {
	timeout := s.runner.opts.ToolTimeout
	if timeout <= 0 {
		timeout = DefaultToolTimeout
	}
	toolCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	done := make(chan toolset.Result, 1)
	s.runner.inFlight.Add(1)
	go func() {
		defer s.runner.inFlight.Done()
		defer func() {
			if recovered := recover(); recovered != nil {
				s.runner.logger.Error(
					"tool execution panicked",
					"tool_use_id", toolUseCallID(event),
					"tool", event.Name,
					"panic", recovered,
					"stack", string(debug.Stack()),
				)
				done <- toolset.ErrorResult(fmt.Sprintf("tool execution panicked: %v", recovered))
			}
		}()
		done <- fn(toolCtx)
	}()
	select {
	case result := <-done:
		return result
	case <-toolCtx.Done():
		if errors.Is(toolCtx.Err(), context.DeadlineExceeded) {
			return toolset.ErrorResult(fmt.Sprintf("tool execution timed out after %s", timeout))
		}
		return toolset.ErrorResult(toolCtx.Err().Error())
	}
}

func (s *toolRunnerState) postResult(ctx context.Context, event Event, custom bool, callID string, result toolset.Result, confirmation string) error {
	var out Event
	if custom {
		out = NewUserCustomToolResultEvent(callID, runnerContentBlocks(result.Content), result.IsError, event.SessionThreadID)
	} else {
		out = NewUserToolResultEvent(callID, runnerContentBlocks(result.Content), result.IsError, event.SessionThreadID)
	}
	if s.runner.opts.ResultStore != nil {
		if err := s.runner.opts.ResultStore.SaveResult(callID, out); err != nil {
			s.runner.logger.Warn("persist tool result failed", "tool_use_id", callID, "err", err)
		}
		s.pendingResults[callID] = out
	}
	return s.sendResult(ctx, callID, event, custom, confirmation, out)
}

func (s *toolRunnerState) sendResult(ctx context.Context, callID string, event Event, custom bool, confirmation string, out Event) error {
	req := SendEventRequest{
		SessionID: s.runner.session,
		Event:     out,
	}
	posted, err := s.retrySendEvent(ctx, req, callID)
	if err != nil && s.runner.opts.OnToolError != nil {
		s.runner.opts.OnToolError(event, err)
	}
	if posted {
		s.markAnswered(callID)
		if s.runner.opts.ResultStore != nil {
			if err := s.runner.opts.ResultStore.MarkSent(callID); err != nil {
				s.runner.logger.Warn("mark tool result sent failed", "tool_use_id", callID, "event_id", out.ID, "err", err)
			}
		}
	} else if s.runner.opts.ResultStore != nil {
		s.pendingResults[callID] = out
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.runner.events <- ToolCallResult{
		ToolUseID:    callID,
		Name:         event.Name,
		Custom:       custom,
		Confirmation: confirmation,
		Posted:       posted,
		Event:        event,
		Result:       out,
	}:
		return nil
	}
}

func (s *toolRunnerState) resolveToolUseWithoutPost(ctx context.Context, event Event, custom bool, callID string, confirmation string) error {
	s.markAnswered(callID)
	s.maybeArmPendingIdle()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.runner.events <- ToolCallResult{
		ToolUseID:    callID,
		Name:         event.Name,
		Custom:       custom,
		Confirmation: confirmation,
		Posted:       false,
		Event:        event,
	}:
		return nil
	}
}

func (s *toolRunnerState) skipExternalToolUse(ctx context.Context, event Event, custom bool, callID string) error {
	s.externalTools[callID] = event
	s.maybeArmPendingIdle()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.runner.events <- ToolCallResult{
		ToolUseID: callID,
		Name:      event.Name,
		Custom:    custom,
		Posted:    false,
		Event:     event,
	}:
		return nil
	}
}

func (s *toolRunnerState) flushResults(ctx context.Context) error {
	var first error
	for callID, event := range s.pendingResults {
		req := SendEventRequest{
			SessionID: s.runner.session,
			Event:     event,
		}
		posted, err := s.retrySendEvent(ctx, req, callID)
		if !posted {
			if first == nil {
				first = err
			}
			continue
		}
		s.markAnswered(callID)
		if s.runner.opts.ResultStore != nil {
			if err := s.runner.opts.ResultStore.MarkSent(callID); err != nil {
				s.runner.logger.Warn("mark pending tool result sent failed", "tool_use_id", callID, "event_id", event.ID, "err", err)
			}
		}
	}
	s.maybeArmPendingIdle()
	return first
}

func (s *toolRunnerState) retrySendEvent(ctx context.Context, req SendEventRequest, callID string) (bool, error) {
	var err error
	for attempt := 0; attempt < sessionRunnerSendRetries; attempt++ {
		// 工具已经执行后，即使 runner 开始退出，也为结果保留一次有界的最终投递机会。
		sendCtx, cancel := context.WithTimeout(context.Background(), s.sendTimeout())
		err = s.runner.api.SendEvent(sendCtx, req)
		cancel()
		if err == nil {
			return true, nil
		}
		if isFatal4xxStatus(err) {
			s.runner.logger.Error("tool result send hit permanent 4xx", "tool_use_id", callID, "err", err)
			return false, err
		}
		if ctx.Err() != nil {
			s.runner.logger.Debug("tool result send abandoned after context cancellation", "tool_use_id", callID, "err", err)
			return false, err
		}
		s.runner.logger.Warn("tool result send failed, retrying", "tool_use_id", callID, "attempt", attempt+1, "err", err)
		if attempt < sessionRunnerSendRetries-1 {
			sleepCtx(ctx, time.Duration(attempt+1)*time.Second)
		}
	}
	return false, err
}

func (s *toolRunnerState) maxIdle() time.Duration {
	if s.runner.opts.MaxIdle == nil {
		return DefaultMaxIdle
	}
	return *s.runner.opts.MaxIdle
}

func (s *toolRunnerState) sendTimeout() time.Duration {
	if s.runner.opts.SendTimeout > 0 {
		return s.runner.opts.SendTimeout
	}
	return defaultSessionRunnerSendTimeout
}

func (r *SessionToolRunner) drainInFlight() {
	done := make(chan struct{})
	go func() {
		r.inFlight.Wait()
		close(done)
	}()
	timer := time.NewTimer(r.drainTimeout())
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		r.logger.Warn("drain timeout exceeded; in-flight tools may still be running", "drain_timeout", r.drainTimeout())
	}
}

func (r *SessionToolRunner) drainTimeout() time.Duration {
	if r.opts.DrainTimeout > 0 {
		return r.opts.DrainTimeout
	}
	return defaultSessionRunnerDrainTimeout
}

func normalizeRunnerErr(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return err
}

func sleepCtx(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func jitterDuration(d time.Duration) time.Duration {
	if d <= 1 {
		return d
	}
	half := d / 2
	return half + time.Duration(rand.Int63n(int64(d-half)))
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func isFatal4xxStatus(err error) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.StatusCode >= 400 &&
		apiErr.StatusCode < 500 &&
		apiErr.StatusCode != http.StatusRequestTimeout &&
		apiErr.StatusCode != http.StatusConflict &&
		apiErr.StatusCode != http.StatusTooManyRequests
}

func runnerContentBlocks(blocks []toolset.ContentBlock) []ContentBlock {
	out := make([]ContentBlock, 0, len(blocks))
	for _, block := range blocks {
		out = append(out, ContentBlock{
			Type:      block.Type,
			Text:      block.Text,
			MediaType: block.MediaType,
			Data:      block.Data,
		})
	}
	return out
}

func toolConfirmationCallID(event Event) string {
	if event.ToolUseID != "" {
		return event.ToolUseID
	}
	return event.CustomToolUseID
}

func toolUseCallID(event Event) string {
	if event.ToolUseID != "" {
		return event.ToolUseID
	}
	if event.CustomToolUseID != "" {
		return event.CustomToolUseID
	}
	return event.ID
}

func toolResultCallID(event Event) string {
	if event.ToolUseID != "" {
		return event.ToolUseID
	}
	return event.CustomToolUseID
}
