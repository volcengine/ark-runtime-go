// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"
)

var errHistoryLimit = errors.New("history limit")
var errHistoryConflict = errors.New("history conflict")

// historySkew 是对账窗口相对本地提交时间的回退量，用于吸收 Runner 与 MA 之间的时钟偏差。
// MA 的 created_at 是服务端写入时间，本轮事件必然晚于提交；回退只会多读少量旧事件，
// 旧事件由输入边界排除。
const historySkew = 10 * time.Minute

// observedTypes 是判定一轮结果所需的事件类型；其余类型（span、thinking、usage 等）不参与，
// 过滤后历史读取量与轮次长度无关地保持很小。
var observedTypes = []string{
	"user.message",
	"user.interrupt",
	"agent.message",
	"agent.thinking",
	"agent.tool_use",
	"agent.tool_result",
	"agent.custom_tool_use",
	"agent.mcp_tool_use",
	"agent.mcp_tool_result",
	"user.tool_confirmation",
	"session.status_idle",
	"session.status_running",
	"session.status_terminated",
	"session.deleted",
	"session.error",
}

func historyErrorKind(err error) string {
	if errors.Is(err, errHistoryLimit) {
		return "history_limit"
	}
	if errors.Is(err, errHistoryConflict) {
		return "history_conflict"
	}
	return "history_unavailable"
}

// historyWindow 返回本轮对账的起点；没有提交时间（旧记录）时不限定窗口。
func historyWindow(j *job) *time.Time {
	if j.Submit.SubmittedAt.IsZero() {
		return nil
	}
	start := j.Submit.SubmittedAt.Add(-historySkew)
	return &start
}

func mainThread(sessionID string) string {
	if !strings.HasPrefix(sessionID, "sesn-") || len(sessionID) <= len("sesn-") {
		return ""
	}
	return "sthr-" + strings.TrimPrefix(sessionID, "sesn-")
}

func onMainThread(event Event, sessionID string) bool {
	return event.SessionThreadID == "" || event.SessionThreadID == mainThread(sessionID)
}

// matchingEcho 校验同步回包的输入身份；正文保持一致，附件允许平台逐块降级为文本。
func matchingEcho(event Event, j *job) bool {
	if event.ID != j.InputEventID || event.Type != "user.message" || !onMainThread(event, j.Session.SessionID) {
		return false
	}
	input := userInput(j)
	if len(input.Blocks) == 0 {
		// 纯文本沿用拼接校验，兼容上游拆分或合并文本块。
		var text strings.Builder
		for _, block := range event.Content {
			if block.Type != "text" {
				return false
			}
			text.WriteString(block.Text)
		}
		return text.String() == input.ExpectedText()
	}
	return matchingAttachmentEcho(event.Content, input)
}

// matchingAttachmentEcho 按 SendMessage 的块顺序匹配；只有附件本体允许替换为非空文本。
func matchingAttachmentEcho(content []TextBlock, input UserInput) bool {
	matchText := func(text string) bool {
		if text == "" {
			return true
		}
		if len(content) == 0 || content[0].Type != "text" || content[0].Text != text {
			return false
		}
		content = content[1:]
		return true
	}
	if !matchText(input.Context) || !matchText(input.Text) {
		return false
	}
	for _, block := range input.Blocks {
		switch block.Type {
		case "image", "document", "video":
			if len(content) == 0 {
				return false
			}
			item := content[0]
			if item.Type != block.Type && (item.Type != "text" || strings.TrimSpace(item.Text) == "") {
				return false
			}
			content = content[1:]
		case InputBlockNotice:
			// notice 本来就作为文本发送，不能套用附件本体的宽松匹配。
		default:
			return false
		}
		if !matchText(input.AttachmentText(block)) {
			return false
		}
	}
	return len(content) == 0
}

// matchingHistoryInput 按 ID、类型与主线程认定历史中的本轮输入。ID 由 Channel 按任务确定性生成，
// 历史内容可能经过平台预处理，不能作为身份判定依据。
func matchingHistoryInput(event Event, j *job) bool {
	return event.ID == j.InputEventID && event.Type == "user.message" && onMainThread(event, j.Session.SessionID)
}

func inputVisible(events []Event, j *job) (found, conflict bool) {
	for _, event := range events {
		if event.ID != j.InputEventID {
			continue
		}
		if !matchingHistoryInput(event, j) {
			return false, true
		}
		return true, false
	}
	return false, false
}

func toolEvent(eventType string) bool {
	switch eventType {
	case "agent.tool_use", "agent.tool_result", "agent.custom_tool_use", "agent.mcp_tool_use", "agent.mcp_tool_result":
		return true
	}
	return false
}

// turnOutcome 是一次历史扫描对当前轮次的判定。
type turnOutcome struct {
	// Status 为空表示尚未结束；done / agent_failed / session_gone / unsupported_contract / input_conflict 为终态。
	Status    string
	Text      string
	Artifacts []Artifact
	// WaitingAction 表示本轮停在 requires_action，等待外部确认或工具结果。
	WaitingAction bool
}

func messageText(event Event) (string, bool) {
	texts := make([]string, 0, len(event.Content))
	for _, block := range event.Content {
		if block.Type != "text" {
			return "", false
		}
		texts = append(texts, block.Text)
	}
	return strings.Join(texts, "\n"), true
}

// judgeTurn 按输入边界判定本轮结果。
//
// 本轮由历史中本轮输入之后的第一个主线程终态界定。下一条主线程输入会使边界失效。
// 最终答案只从本轮最后一次工具调用后的主线程 agent.message 中选择。
func judgeTurn(events []Event, j *job) turnOutcome {
	index := -1
	for i, event := range events {
		if event.ID != j.InputEventID {
			continue
		}
		if !matchingHistoryInput(event, j) {
			return turnOutcome{Status: "input_conflict"}
		}
		index = i
		break
	}
	if index < 0 {
		return turnOutcome{}
	}
	var answers []Event
	toolUses := make(map[string]string)
	waiting := false
	for offset, event := range events[index+1:] {
		if !onMainThread(event, j.Session.SessionID) {
			continue
		}
		// 下一条主线程输入打破独占写入前提，不能把它的答案算入当前轮次。
		if event.Type == "user.message" {
			return turnOutcome{Status: "unsupported_contract"}
		}
		switch {
		case toolEvent(event.Type):
			// 本轮已知调用的结果可能迟于最终答案，不能将其当作新一轮工具执行。
			knownResult := event.ToolUseID != "" && (event.Type == "agent.tool_result" && toolUses[event.ToolUseID] == "agent.tool_use" ||
				event.Type == "agent.mcp_tool_result" && toolUses[event.ToolUseID] == "agent.mcp_tool_use")
			if !knownResult {
				// 新调用及无法关联的结果仍清除过程文字，避免将工具前的文本当作答案。
				answers = answers[:0]
			}
			if event.ID != "" && (event.Type == "agent.tool_use" || event.Type == "agent.mcp_tool_use") {
				toolUses[event.ID] = event.Type
			}
		case event.Type == "agent.message":
			answers = append(answers, event)
		case event.Type == "session.status_terminated" || event.Type == "session.deleted":
			return turnOutcome{Status: eventTerminalStatus(event)}
		case event.Type == "session.error":
			if status := eventTerminalStatus(event); status != "" {
				return turnOutcome{Status: status}
			}
		case event.Type == "session.status_idle":
			if event.StopReason == "requires_action" {
				waiting = true
				continue
			}
			if status := eventTerminalStatus(event); status != "done" {
				return turnOutcome{Status: status}
			}
			// end_turn 已确认正常结束；工具之后没有最终文字也允许静默完成。
			text, ok := selectFinalAnswer(answers)
			if !ok {
				return turnOutcome{Status: "unsupported_contract"}
			}
			return turnOutcome{Status: "done", Text: text, Artifacts: artifactsFromEvents(events[:index+2+offset], j, text, j.connection.Outputs != nil, j.binding.OutputArtifactsAll)}
		}
		waiting = false
	}
	return turnOutcome{WaitingAction: waiting}
}

// wakeOnStream 在后台保持一条 SSE 长连，收到任意事件就发出唤醒信号；断开后按轮询间隔重连。
// SSE 只用于降低延迟，结果始终以历史扫描为准。
func (r *Runtime) wakeOnStream(ctx context.Context, client MAClient, sessionID string, preview *streamPreview, activity *activityTracker) (<-chan struct{}, func()) {
	wake := make(chan struct{}, 1)
	streamCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() { _ = recover() }()
		for streamCtx.Err() == nil {
			var stream EventStream
			var err error
			if previews, ok := client.(PreviewStreamer); ok {
				if preview != nil {
					preview.reset()
				}
				activity.resetStream()
				stream, err = previews.StreamEventPreviews(streamCtx, sessionID)
			} else {
				stream, err = client.StreamEvents(streamCtx, sessionID)
			}
			if err == nil {
				for {
					event, recvErr := stream.Recv(streamCtx)
					if recvErr != nil {
						break
					}
					activity.accept(event, r.options.Clock())
					if preview != nil {
						preview.accept(event)
					}
					select {
					case wake <- struct{}{}:
					default:
					}
				}
				_ = stream.Close()
			}
			if streamCtx.Err() == nil {
				r.report(streamCtx, Observation{Name: EventStreamDrop, Status: "reconnect", SessionID: sessionID})
			}
			if r.waitPoll(streamCtx) != nil {
				return
			}
		}
	}()
	return wake, func() { cancel(); wg.Wait() }
}

func (r *Runtime) observe(ctx context.Context, j *job, connection Connection) error {
	if !j.stage.Accepts(PhaseObserve) {
		return r.releaseStage(ctx, j)
	}
	var preview *streamPreview
	if j.binding.StreamProgress && j.binding.ProgressProtocol == "native_cot" {
		preview = newStreamPreview(j.Session.SessionID, j.InputEventID)
	}
	activity := newActivityTracker(j)
	wake, stop := r.wakeOnStream(ctx, connection.MAClient, j.Session.SessionID, preview, activity)
	defer stop()
	history := &historyScanner{client: connection.MAClient, sessionID: j.Session.SessionID, since: historyWindow(j), clock: r.options.Clock}
	waitingAction := false
	for ctx.Err() == nil {
		events, err := history.readTurn(ctx, j)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			if errors.Is(err, errHistoryLimit) || errors.Is(err, errHistoryConflict) {
				return r.abortTurn(j, connection, historyErrorKind(err))
			}
			// 历史暂不可读不改变本轮结论，继续等待到截止时间。
			r.reportTurn(EventHistoryError, "unavailable", historyErrorKind(err), j)
		} else {
			activity.confirm(events, j, r.options.Clock())
			if preview != nil {
				preview.confirm(events, j)
			}
			outcome := judgeTurn(events, j)
			if outcome.Status == "done" || outcome.Status == "session_gone" || outcome.Status == "agent_failed" {
				r.flushToolProgress(j, events)
				if j.progressErr != nil {
					return j.progressErr
				}
			}
			waitingAction = outcome.WaitingAction
			if waitingAction {
				r.promptApprovals(ctx, j, events)
			}
			switch outcome.Status {
			case "":
			case "done":
				if err := r.proveRemote(j, RemoteInput, RemoteTerminal, j.InputEventID); err != nil {
					return err
				}
				return r.settleAnswer(j, outcome.Text, outcome.Artifacts)
			case "session_gone":
				if err := r.proveRemote(j, RemoteInput, RemoteGone, j.Session.SessionID); err != nil {
					return err
				}
				return r.failTurn(j, "session_gone")
			case "agent_failed":
				if err := r.proveRemote(j, RemoteInput, RemoteTerminal, j.InputEventID); err != nil {
					return err
				}
				return r.failTurn(j, "agent_failed")
			default:
				return r.abortTurn(j, connection, outcome.Status)
			}
			var textID, text string
			if preview != nil {
				textID, text = preview.snapshot()
			}
			r.reportProgressText(j, progressFromEvents(events, j), ProgressRunning, "", textID, text)
			if j.progressErr != nil {
				return j.progressErr
			}
		}
		if err := r.saveActivity(ctx, j, activity); err != nil {
			return err
		}
		// 已完成的轮次优先交付答案；仍在运行时才响应停止请求。
		if r.stopRequested(ctx, j) {
			return r.stopTurn(j)
		}
		if r.pastDeadline(j) {
			// 到期前完整复核一次，避免缓存、接管或刚到达的终态被旧截止时间抢先中断。
			events, err := history.read(ctx, true)
			if err == nil {
				activity.confirm(events, j, r.options.Clock())
				if err = r.saveActivity(ctx, j, activity); err != nil {
					return err
				}
				if judgeTurn(events, j).Status != "" {
					continue
				}
			}
			if r.pastDeadline(j) {
				if waitingAction {
					return r.giveUp(j, "requires_action_timeout")
				}
				return r.giveUp(j, "idle_timeout")
			}
		}
		timer := time.NewTimer(r.options.PollInterval)
		select {
		case <-ctx.Done():
		case <-wake:
		case <-timer.C:
		}
		timer.Stop()
	}
	// 单次观察到期只释放 Worker；下次领取先对账进展，再判定无进展超时。
	if waitingAction {
		return r.wait(j, "requires_action_timeout")
	}
	return r.wait(j, "turn_timeout")
}

// settleAnswer 以答案加产物结算：答案（Seq 0）与产物（Seq 1..n）在同一事务内写入，存储层按
// Seq 串行投递；未开启产物投递时只有答案。
func (r *Runtime) settleAnswer(j *job, answer string, artifacts []Artifact) error {
	prepareCtx, cancel := context.WithTimeout(j.workContext(), r.options.DeliveryTimeout)
	defer cancel()
	resolved, err := r.collectSessionArtifacts(prepareCtx, j, answer, artifacts)
	artifacts = resolved
	if err != nil {
		answer += "\n\n（暂时无法获取完整文件清单，部分文件可能未发送。）"
	}
	outputKey := "answer"
	if strings.TrimSpace(answer) == "" {
		// 产物超额等提示不能充当 history 已确认的最终答案。
		outputKey = "notice"
	}
	var extra []Delivery
	if j.binding.DeliverArtifacts && len(artifacts) > 0 {
		skipped := max(0, len(artifacts)-r.options.Artifacts.MaxCount)
		artifacts = artifacts[:len(artifacts)-skipped]
		if skipped > 0 {
			answer = strings.TrimSpace(answer + "\n\n（另有 " + strconv.Itoa(skipped) + " 个文件超出单次发送数量，未发送。）")
		}
	}
	answer, artifacts = r.prepareRichReply(prepareCtx, j, answer, artifacts)
	if len(j.richAnswer) == 0 {
		answer, artifacts = r.prepareAnswerImages(prepareCtx, j, answer, artifacts)
	}
	cancel()
	if err := j.checkLease(j.workContext()); err != nil {
		return err
	}
	if j.binding.DeliverArtifacts {
		extra, _ = r.artifactDeliveries(j, artifacts)
	}
	return r.settle(j, TurnDone, "", outputKey, answer, extra)
}
