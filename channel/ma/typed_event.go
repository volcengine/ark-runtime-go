// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/session"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// projectSDKEvent maps the SDK's decoded event without decoding its JSON again.
func projectSDKEvent(value session.ManagedAgentsSessionEvent) (core.Event, error) {
	if value == nil {
		return core.Event{}, errors.New("missing SDK event")
	}
	if unknown, ok := value.(*session.ManagedAgentsUnknownSessionEvent); ok {
		return projectEvent(unknown.RawPayload)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return core.Event{}, errors.New("invalid SDK event")
	}
	event := core.Event{ID: value.GetID(), Type: value.Type(), Raw: raw}
	if at, err := time.Parse(time.RFC3339Nano, value.GetProcessedAt()); err == nil {
		event.ProcessedAt = &at
	}
	stop := func(reason *session.ManagedAgentsStopReason) {
		if reason != nil {
			event.StopReason = reason.Type
			if reason.EventIDs != nil {
				event.ActionEventIDs = append([]string(nil), (*reason.EventIDs)...)
			}
		}
	}
	failure := func(payload session.ManagedAgentsSessionErrorPayload) {
		if payload.RetryStatus != nil {
			event.RetryStatus = payload.RetryStatus.Type
		}
	}
	switch v := value.(type) {
	case *session.ManagedAgentsUserMessageEvent:
		event.SessionThreadID = v.SessionThreadID
		event.Content = sdkTextBlocks(v.Content)
	case *session.ManagedAgentsAgentMessageEvent:
		event.SessionThreadID = v.SessionThreadID
		event.Content = sdkTextBlocks(v.Content)
	case *session.ManagedAgentsAgentThinkingEvent:
		event.SessionThreadID = v.SessionThreadID
		event.Content = sdkTextBlocks(v.Content)
	case *session.ManagedAgentsAgentToolUseEvent:
		event.SessionThreadID = v.SessionThreadID
		event.ToolUseID = v.ToolUseID
		event.ToolName = v.Name
		event.ToolInput = rawString(v.Input)
		event.Permission = v.EvaluatedPermission
	case *session.ManagedAgentsAgentCustomToolUseEvent:
		event.SessionThreadID = v.SessionThreadID
		event.ToolUseID = v.CustomToolUseID
		event.ToolName = v.Name
		event.ToolInput = rawString(v.Input)
	case *session.ManagedAgentsAgentMCPToolUseEvent:
		event.SessionThreadID = v.SessionThreadID
		event.ToolUseID = v.ToolUseID
		event.ToolName = v.Name
		event.MCPServerName = v.MCPServerName
		event.ToolInput = rawString(v.Input)
	case *session.ManagedAgentsAgentToolResultEvent:
		event.SessionThreadID = v.SessionThreadID
		event.ToolUseID = v.ToolUseID
		event.IsError = v.IsError
		event.Content = sdkTextBlocks(v.Content)
	case *session.ManagedAgentsAgentMCPToolResultEvent:
		event.SessionThreadID = v.SessionThreadID
		event.ToolUseID = v.ToolUseID
		event.IsError = v.IsError
		event.MCPServerName = v.MCPServerName
		event.Content = sdkTextBlocks(v.Content)
	case *session.ManagedAgentsSessionStatusIdleEvent:
		stop(v.StopReason)
	case *session.ManagedAgentsSessionStatusTerminatedEvent:
		stop(v.StopReason)
	case *session.ManagedAgentsSessionThreadStatusIdleEvent:
		event.SessionThreadID = v.SessionThreadID
		stop(v.StopReason)
	case *session.ManagedAgentsSessionThreadStatusTerminatedEvent:
		event.SessionThreadID = v.SessionThreadID
		stop(v.StopReason)
	case *session.ManagedAgentsSessionErrorEvent:
		failure(v.Error)
	case *session.ManagedAgentsSessionThreadCreatedEvent:
		event.SessionThreadID = v.SessionThreadID
	case *session.ManagedAgentsSessionThreadStatusRunningEvent:
		event.SessionThreadID = v.SessionThreadID
	case *session.ManagedAgentsSpanModelRequestStartEvent:
		event.SessionThreadID = v.SessionThreadID
	case *session.ManagedAgentsSpanModelRequestEndEvent:
		event.SessionThreadID = v.SessionThreadID
		event.IsError = v.IsError
	case *session.ManagedAgentsAgentThreadContextCompactedEvent:
		event.SessionThreadID = v.SessionThreadID

	case *session.ManagedAgentsUserToolConfirmationEvent:
		event.SessionThreadID = v.SessionThreadID
		event.ToolUseID = v.ToolUseID
	case *session.ManagedAgentsUserInterruptEvent:
		event.SessionThreadID = v.SessionThreadID
	}
	return event, nil
}

func sdkTextBlocks(values []session.ManagedAgentsOutputContentBlock) []core.TextBlock {
	if values == nil {
		return nil
	}
	result := make([]core.TextBlock, 0, len(values))
	for _, v := range values {
		result = append(result, core.TextBlock{Type: v.Type, Text: v.Text})
	}
	return result
}
