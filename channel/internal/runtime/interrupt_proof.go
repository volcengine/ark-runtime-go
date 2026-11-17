// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

// interruptProof 按本轮输入、中断 ID 回显和主线程终态尽力对账。
// 回显不证明后台取消已执行完；调用者还须在扫描之后确认 Session idle。
func interruptProof(events []Event, j *job) *RemoteProof {
	input, interrupted, terminal := false, false, false
	for _, event := range events {
		if event.ID == j.InputEventID {
			if input || !matchingHistoryInput(event, j) {
				return nil
			}
			input = true
			continue
		}
		if !input {
			continue
		}
		if event.ID == interruptEventID(j) {
			if event.Type != "user.interrupt" || !onMainThread(event, j.Session.SessionID) {
				return nil
			}
			interrupted = true
			continue
		}
		if !onMainThread(event, j.Session.SessionID) {
			continue
		}
		if event.Type == "user.message" || event.Type == "user.interrupt" {
			return nil
		}
		switch event.Type {
		case "session.status_idle":
			terminal = event.StopReason == "end_turn" || event.StopReason == "retries_exhausted"
		case "session.status_running", "session.error":
			terminal = false
		}
	}
	if !interrupted || !terminal {
		return nil
	}
	return &RemoteProof{Kind: RemoteInterrupted, Reference: interruptEventID(j)}
}
