// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"strings"
	"time"
)

// ProgressStep 是一条可对用户展示的过程步骤，是否展示由 Binding 的过程展示配置决定。
type ProgressStep struct {
	// EventID 是 MA 事件 ID，渠道用于去重。
	EventID string
	// Kind 为 tool / mcp_tool / custom_tool / thinking。
	Kind string
	// Name 是工具名（mcp_tool 形如 server/tool）。
	Name string
	// Arguments 是经过脱敏与截断的工具参数，仅用于原生 COT 明细，不进入持久化正文或日志。
	Arguments string `json:",omitempty"`
	// Output 是原生 COT 使用的脱敏、截断工具文本结果，不包含图片等非文本块。
	Output string `json:",omitempty"`
	// Done 表示该步骤已有结果。
	Done bool
	// Failed 表示工具返回错误。
	Failed bool
	// DurationMS 只在事件时间可证明耗时时设置。
	DurationMS int64
}

// ProgressState 是过程展示的整体状态。
type ProgressState string

const (
	// ProgressRunning 表示轮次仍在执行。
	ProgressRunning ProgressState = "running"
	// ProgressDone 表示轮次正常结束。
	ProgressDone ProgressState = "done"
	// ProgressFailed 表示轮次失败或超时，用户主动停止使用 ProgressStopped。
	ProgressFailed ProgressState = "failed"
	// ProgressStopped 表示用户主动停止。
	ProgressStopped ProgressState = "stopped"
)

// ProgressUpdate 是一次过程展示的完整快照（不是增量），渠道可直接覆盖渲染。
type ProgressUpdate struct {
	TurnID      string
	ReplyTarget ReplyTarget
	State       ProgressState
	Steps       []ProgressStep
	// Summary 是终态说明（如停止原因），为空表示无额外说明。
	Summary string
	At      time.Time
	// TextID/Text 是仅用于过程预览的公开消息文本，不参与最终答案判断。
	TextID, Text string
	SessionID    string
	Protocol     string
}

// ProgressReporter 是 Provider 可选实现的过程展示端口（对应飞书 COT / 过程卡片）。
//
// 它是旁路：Runtime 只尽力调用，失败只记观测，不重试到阻塞答案、不影响结算与投递。
// handle 是渠道侧消息引用，首次 Report 返回，后续更新原样传回；Runtime 只在内存中保存它，
// 进程重启后从新卡片重新开始。同一 TurnID 的调用由 Runtime 串行化并节流，终态后不再调用。
type ProgressReporter interface {
	ReportProgress(ctx context.Context, handle string, update ProgressUpdate) (string, error)
}

// progressFromEvents 把本轮输入之后的主线程事件投影为过程步骤。
func progressFromEvents(events []Event, j *job) []ProgressStep {
	index := -1
	for i, event := range events {
		if event.ID == j.InputEventID {
			if !matchingHistoryInput(event, j) {
				return nil
			}
			index = i
			break
		}
	}
	if index < 0 {
		return nil
	}
	steps := make([]ProgressStep, 0)
	position := map[string]int{}
	started := map[string]time.Time{}
	redactors := map[string]*toolDetailRedactor{}
	for _, event := range events[index+1:] {
		if !onMainThread(event, j.Session.SessionID) {
			continue
		}
		if event.Type == "user.message" {
			break
		}
		if event.Type == "session.status_terminated" || event.Type == "session.deleted" || event.Type == "session.status_idle" && event.StopReason != "requires_action" {
			break
		}
		switch event.Type {
		case "agent.tool_use", "agent.mcp_tool_use", "agent.custom_tool_use":
			kind := map[string]string{"agent.tool_use": "tool", "agent.mcp_tool_use": "mcp_tool", "agent.custom_tool_use": "custom_tool"}[event.Type]
			name := event.ToolName
			if event.Type == "agent.mcp_tool_use" && event.MCPServerName != "" {
				name = event.MCPServerName + "/" + name
			}
			position[event.ID] = len(steps)
			if event.ProcessedAt != nil {
				started[event.ID] = *event.ProcessedAt
			}
			step := ProgressStep{EventID: event.ID, Kind: kind, Name: name}
			if j.binding.ProgressProtocol == "native_cot" {
				redactor := &toolDetailRedactor{}
				redactors[event.ID] = redactor
				step.Arguments = redactor.preview(event.ToolInput)
			}
			steps = append(steps, step)
		case "agent.tool_result", "agent.mcp_tool_result":
			if i, ok := position[event.ToolUseID]; ok {
				steps[i].Done = true
				steps[i].Failed = event.IsError
				if redactor := redactors[event.ToolUseID]; redactor != nil {
					var blocks []string
					for _, block := range event.Content {
						if block.Type == "text" {
							blocks = append(blocks, block.Text)
						}
					}
					steps[i].Output = redactor.preview(strings.Join(blocks, "\n"))
				}
				if start := started[event.ToolUseID]; !start.IsZero() && event.ProcessedAt != nil && event.ProcessedAt.After(start) {
					steps[i].DurationMS = event.ProcessedAt.Sub(start).Milliseconds()
				}
			}
		}
	}
	return steps
}

// progressState 是一轮过程卡片的内存状态；进程重启后丢失，恢复的轮次会新建一张卡片。
type progressState struct {
	handle string
	last   time.Time
	steps  []ProgressStep
}

func sameSteps(a, b []ProgressStep) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// reportProgress 尽力更新过程卡片。running 状态按间隔节流且只在步骤变化时更新；终态强制发送
// 一次并释放状态。托管 ProgressMode 在首次扫描前已结束时也创建最终状态卡片。
func (r *Runtime) reportProgress(j *job, steps []ProgressStep, state ProgressState, summary string) {
	r.reportProgressText(j, steps, state, summary, "", "")
}

func (r *Runtime) reportProgressText(j *job, steps []ProgressStep, state ProgressState, summary, textID, text string) {
	if j.binding.ProgressProtocol == "native_cot" {
		if state != ProgressRunning {
			return
		}
		if j.binding.ProgressMode == "status_only" || j.binding.ProgressMode == "" && !j.binding.ShowProgress {
			steps = nil
		}
		r.reportNativeProgress(j, ProgressUpdate{TurnID: j.ID, SessionID: j.Session.SessionID, ReplyTarget: ReplyTargetFor(j.msg, j.binding), State: state, Steps: steps, TextID: textID, Text: text, At: r.options.Clock(), Protocol: "native_cot"}, false)
		return
	}
	connection := r.jobConnection(j)
	reporter, ok := providerPort[ProgressReporter](connection.Provider)
	if !ok || !j.binding.ShowProgress && j.binding.ProgressMode == "" {
		return
	}
	if j.binding.ProgressMode == "status_only" {
		steps = nil
	}
	now := r.options.Clock()
	terminal := state != ProgressRunning
	r.progressMu.Lock()
	current := r.progress[j.ID]
	switch {
	case current == nil && terminal && j.binding.ProgressMode == "":
		r.progressMu.Unlock()
		return
	case current == nil:
		current = &progressState{}
		r.progress[j.ID] = current
	case !terminal && (sameSteps(current.steps, steps) || now.Sub(current.last) < r.options.ProgressInterval):
		r.progressMu.Unlock()
		return
	}
	if terminal && steps == nil {
		steps = current.steps
	}
	handle := current.handle
	r.progressMu.Unlock()
	update := ProgressUpdate{TurnID: j.ID, ReplyTarget: ReplyTargetFor(j.msg, j.binding), State: state, Steps: append([]ProgressStep(nil), steps...), Summary: summary, At: now}
	timeout := r.options.DeliveryTimeout
	if timeout > 5*time.Second {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(j.workContext(), timeout)
	if terminal {
		if ctx.Err() != nil {
			cancel()
			return
		}
	} else if j.checkLease(ctx) != nil {
		cancel()
		return
	}
	newHandle, err := func() (created string, err error) {
		defer func() {
			if recover() != nil {
				err = errProgressPanic
			}
		}()
		return reporter.ReportProgress(ctx, handle, update)
	}()
	cancel()
	status := "updated"
	switch {
	case err != nil:
		status = "failed"
	case handle == "":
		status = "created"
	case terminal:
		status = "completed"
	}
	r.reportTurn(EventProgress, status, "", j)
	r.progressMu.Lock()
	defer r.progressMu.Unlock()
	if terminal {
		delete(r.progress, j.ID)
		return
	}
	current.last, current.steps = now, append([]ProgressStep(nil), steps...)
	if err == nil && newHandle != "" {
		current.handle = newHandle
	}
}

var errProgressPanic = errors.New("progress reporter panic")

// progressStateFor 把结算结果映射为过程卡片终态。
func progressStateFor(status TurnStatus) ProgressState {
	switch status {
	case TurnStopped:
		return ProgressStopped
	case TurnDone:
		return ProgressDone
	}
	return ProgressFailed
}
