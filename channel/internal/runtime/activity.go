// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"
)

const maxActivityEvents = 10000
const maxActivityStreams = 64
const maxActivityBlocks = 128

// TurnActivity 是无正文的进展检查点；历史 ID 摘要和流字节高水位用于跨重启去重。
type TurnActivity struct {
	SessionID      string              `json:"session_id,omitempty"`
	InputEventID   string              `json:"input_event_id,omitempty"`
	LastProgressAt time.Time           `json:"last_progress_at"`
	SeenEvents     map[string]bool     `json:"seen_events,omitempty"`
	Initialized    bool                `json:"initialized"`
	Streams        map[string][]uint64 `json:"streams,omitempty"`
}

func cloneActivity(a *TurnActivity) TurnActivity {
	if a == nil {
		return TurnActivity{}
	}
	next := *a
	next.SeenEvents = nil
	if len(a.SeenEvents) > 0 {
		next.SeenEvents = make(map[string]bool, len(a.SeenEvents))
		for id, seen := range a.SeenEvents {
			next.SeenEvents[id] = seen
		}
	}
	next.Streams = nil
	if len(a.Streams) > 0 {
		next.Streams = make(map[string][]uint64, len(a.Streams))
	}
	for id, offsets := range a.Streams {
		next.Streams[id] = append([]uint64(nil), offsets...)
	}
	return next
}

// activityTracker 只记录本轮可证明的事件；连接成功和相同历史不会推进时间。
type activityTracker struct {
	mu                 sync.Mutex
	state              TurnActivity
	legacy             bool
	anchored           bool
	sessionID, inputID string
	ignored            map[string]bool
	current            map[string][]uint64
}

func newActivityTracker(j *job) *activityTracker {
	state := cloneActivity(j.Activity)
	if state.SessionID != j.Session.SessionID || state.InputEventID != j.InputEventID {
		state.SeenEvents = nil
		state.Streams = nil
	}
	state.SessionID, state.InputEventID = j.Session.SessionID, j.InputEventID
	if state.LastProgressAt.IsZero() {
		state.LastProgressAt = j.Schedule.StartedAt
	}
	if state.SeenEvents == nil {
		state.SeenEvents = map[string]bool{}
	}
	if state.Streams == nil {
		state.Streams = map[string][]uint64{}
	}
	return &activityTracker{state: state, legacy: j.Activity == nil || !state.Initialized, sessionID: j.Session.SessionID, inputID: j.InputEventID, ignored: map[string]bool{}, current: map[string][]uint64{}}
}

func effectiveProgress(event Event) bool {
	if toolEvent(event.Type) {
		return true
	}
	if event.Type != "agent.message" && event.Type != "agent.thinking" {
		return false
	}
	for _, block := range event.Content {
		if block.Text != "" {
			return true
		}
	}
	return false
}

func (a *activityTracker) confirm(events []Event, j *job, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	found := false
	for _, event := range events {
		a.ignored[event.ID] = true
		if !found {
			if matchingHistoryInput(event, j) {
				found = true
				a.anchored = true
			}
			continue
		}
		if !onMainThread(event, j.Session.SessionID) {
			continue
		}
		if event.Type == "user.message" {
			a.anchored = false
			break
		}
		delete(a.state.Streams, stableKey(event.ID))
		delete(a.current, stableKey(event.ID))
		if !effectiveProgress(event) {
			continue
		}
		key := stableKey(event.ID)
		if a.state.SeenEvents[key] {
			continue
		}
		if len(a.state.SeenEvents) >= maxActivityEvents {
			continue
		}
		a.state.SeenEvents[key] = true
		at := now
		if event.ProcessedAt != nil {
			at = *event.ProcessedAt
		} else if a.legacy {
			at = j.Schedule.StartedAt
		}
		a.advance(at, now)
	}
	if found {
		a.legacy = false
		a.state.Initialized = true
	}
}

func (a *activityTracker) advance(at, now time.Time) {
	if at.After(now) {
		at = now
	}
	if at.After(a.state.LastProgressAt) {
		a.state.LastProgressAt = at.UTC()
	}
}

func (a *activityTracker) resetStream() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.current = map[string][]uint64{}
}

func (a *activityTracker) accept(event Event, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.anchored || event.SessionThreadID != mainThread(a.sessionID) || event.Preview == nil {
		return
	}
	v := event.Preview
	key := stableKey(v.EventID)
	if v.EventID == "" || a.ignored[v.EventID] {
		return
	}
	// MA 主线程订阅是只直播的 pubsub；接管可能只收到 delta，不要求重现 start。
	// 可回放适配器不得声明 Live，仍使用下面的字节高水位去重。
	if event.Type == "event_delta" && v.Live && v.Index >= 0 && v.Index < maxActivityBlocks && v.Text != "" {
		a.advance(now, now)
		return
	}
	if event.Type == "event_start" {
		if v.EventType != "agent.message" && v.EventType != "agent.thinking" {
			return
		}
		if _, ok := a.state.Streams[key]; !ok && len(a.state.Streams) >= maxActivityStreams {
			return
		}
		if _, ok := a.current[key]; ok {
			return
		}
		a.current[key] = make([]uint64, maxActivityBlocks)
		if a.state.Streams[key] == nil {
			a.state.Streams[key] = make([]uint64, maxActivityBlocks)
		}
		return
	}
	offsets := a.current[key]
	if event.Type != "event_delta" || len(offsets) != maxActivityBlocks || v.Index < 0 || v.Index >= maxActivityBlocks || v.Text == "" {
		return
	}
	offsets[v.Index] += uint64(len(v.Text))
	if offsets[v.Index] > a.state.Streams[key][v.Index] {
		a.state.Streams[key][v.Index] = offsets[v.Index]
		a.advance(now, now)
	}
}

func (a *activityTracker) snapshot() TurnActivity {
	a.mu.Lock()
	defer a.mu.Unlock()
	return cloneActivity(&a.state)
}

func (r *Runtime) saveActivity(ctx context.Context, j *job, activity *activityTracker) error {
	next := activity.snapshot()
	if j.Activity != nil && reflect.DeepEqual(next, *j.Activity) {
		return nil
	}
	return r.updateTurn(ctx, j, TurnUpdate{Phase: j.Phase, Session: j.Session, Submit: j.Submit, Activity: &next, IdleTimeout: r.options.IdleTimeout})
}

// reconcileIdleExpiry 在任何可能远端执行的路径超时前复核历史，包括接管、身份失败和授权续跑。
func (r *Runtime) reconcileIdleExpiry(j *job) (bool, error) {
	if !mayBeRunning(j) || !r.pastDeadline(j) || !j.StopRequestedAt.IsZero() {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(j.workContext(), r.options.DeliveryTimeout)
	defer cancel()
	connection, err := r.resolveCleanup(ctx, j)
	if err != nil {
		if errors.Is(err, ErrLeaseLost) || isStoreFailure(err) || isUnsupportedWork(err) {
			return true, err
		}
		return true, r.failTurn(j, "interrupt_unconfirmed")
	}
	defer releaseConnection(connection)
	kind := "idle_timeout"
	events, readErr := scanEvents(ctx, connection.MAClient, j.Session.SessionID, historyWindow(j))
	if readErr == nil {
		activity := newActivityTracker(j)
		activity.confirm(events, j, r.options.Clock())
		if err = r.saveActivity(ctx, j, activity); err != nil {
			return true, err
		}
		// 即使完成事件的时间已很早，也必须进入正常交付路径，不能用静默时限覆盖已完成结果。
		outcome := judgeTurn(events, j)
		j.terminalObserved = outcome.Status != ""
		if outcome.WaitingAction {
			kind = "requires_action_timeout"
		}
	} else if errors.Is(readErr, errHistoryLimit) || errors.Is(readErr, errHistoryConflict) {
		j.connection = connection
		return true, r.giveUp(j, historyErrorKind(readErr))
	}
	if r.pastDeadline(j) {
		j.connection = connection
		return true, r.giveUp(j, kind)
	}
	return false, nil
}
