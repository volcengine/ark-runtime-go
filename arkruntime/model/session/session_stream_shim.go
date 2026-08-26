// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package session — SSE stream types for the managed-agents session domain.
//
// The SSE stream endpoints on /sessions/:id/events/stream and
// /threads/:id/stream ship a heterogeneous wire union of 20+ event
// variants (agent.message / span.outcome_evaluation_end /
// session.status_idle / …). ogen can't emit that union directly (the
// spec declares response bodies as `text/event-stream` — a byte stream —
// not a JSON schema), so the typed variants are hand-written here.
//
// Consumption pattern — a plain Go type-switch:
//
//	dec, closer, _ := client.StreamSessionEvents(ctx, sessionID)
//	defer closer.Close()
//	for dec.Next() {
//	    switch ev := dec.Event().Data.(type) {
//	    case *session.ManagedAgentsAgentMessageEvent:
//	        for _, block := range ev.Content {
//	            if block.Text != "" { fmt.Println(block.Text) }
//	        }
//	    case *session.ManagedAgentsSessionStatusIdleEvent:
//	        return
//	    case *session.ManagedAgentsSessionErrorEvent:
//	        log.Printf("stream error: %s", ev.Error.Message)
//	        return
//	    case *session.ManagedAgentsUnknownSessionEvent:
//	        // A wire event this SDK version doesn't yet know about;
//	        // ev.Type() gives the discriminator, ev.RawPayload gives
//	        // the untouched JSON body.
//	    }
//	}
//
// Any new wire event that isn't in the switch-list below falls through
// to [ManagedAgentsUnknownSessionEvent] — that lets the SDK ship
// forwards-compatibly against server upgrades.
package session

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/volcengine/ark-runtime-go/arkruntime/utils"
)

// ManagedAgentsSessionEvent is the common interface every wire event
// (typed variant or Unknown fallback) implements. Type() gives the wire
// discriminator — callers can either compare it as a string or dispatch
// on the concrete pointer type via a Go type-switch. GetID() and
// GetProcessedAt() surface the envelope-level fields shared by every
// variant so callers can read metadata off a heterogeneous slice without
// type-asserting each variant separately.
type ManagedAgentsSessionEvent interface {
	// Type returns the wire `type` discriminator ("agent.message",
	// "span.outcome_evaluation_end", …). Never empty for well-formed frames.
	Type() string
	// GetID returns the server-stamped event id ("sevt-<ts>-<rand>").
	// Empty when the frame doesn't carry one.
	GetID() string
	// GetProcessedAt returns the server RFC 3339 timestamp. Empty when
	// the frame doesn't carry one.
	GetProcessedAt() string
}

// sessionEventBase carries fields every wire event ships and provides the
// interface implementation once. Every typed event struct embeds it.
type sessionEventBase struct {
	EventType   string `json:"type"`
	ID          string `json:"id,omitempty"`
	ProcessedAt string `json:"processed_at,omitempty"`
}

// Type returns the wire `type` discriminator.
func (e sessionEventBase) Type() string { return e.EventType }

// GetID returns the server-stamped event id.
func (e sessionEventBase) GetID() string { return e.ID }

// GetProcessedAt returns the server RFC 3339 timestamp.
func (e sessionEventBase) GetProcessedAt() string { return e.ProcessedAt }

// ---- Nested payload helpers (shared across variants) ----------------------

// ManagedAgentsStopReason categorizes why a session or thread went idle.
// Common values: "end_turn", "retries_exhausted", "user_interrupt".
type ManagedAgentsStopReason struct {
	Type string `json:"type"`
}

// ManagedAgentsRetryStatus optionally accompanies a session error to
// hint whether the runtime plans to retry the failed step. Common
// values: "exhausted", "pending", "in_progress".
type ManagedAgentsRetryStatus struct {
	Type string `json:"type"`
}

// ManagedAgentsSessionErrorPayload is the `error` field on
// session.error / session.thread_status_terminated frames.
type ManagedAgentsSessionErrorPayload struct {
	Type        string                    `json:"type"`
	Message     string                    `json:"message"`
	RetryStatus *ManagedAgentsRetryStatus `json:"retry_status,omitempty"`
}

// ManagedAgentsModelUsage is emitted on span.model_request_end and
// span.outcome_evaluation_end frames to report token accounting.
type ManagedAgentsModelUsage struct {
	InputTokens              int `json:"input_tokens"`
	OutputTokens             int `json:"output_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens,omitempty"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens,omitempty"`
}

// ManagedAgentsOutputContentBlock is the content-list element carried
// by user.message / agent.message / agent.thinking / agent.tool_result
// frames. Fields overlap across block types — dispatch on
// `.Type == "text" | "image" | "document"` in caller code.
type ManagedAgentsOutputContentBlock struct {
	Type    string          `json:"type"`
	Text    string          `json:"text,omitempty"`
	Source  json.RawMessage `json:"source,omitempty"`
	Title   string          `json:"title,omitempty"`
	Context string          `json:"context,omitempty"`
}

// ---- Session-lifecycle events ---------------------------------------------

// ManagedAgentsSessionStatusRunningEvent — the session has an in-flight
// turn. Fires at the start of every send-events -> agent-loop round.
type ManagedAgentsSessionStatusRunningEvent struct {
	sessionEventBase
}

// ManagedAgentsSessionStatusIdleEvent — the session finished a turn and
// is ready for more input. Carries a StopReason indicating whether the
// turn ended normally (`end_turn`) or hit a bail-out.
type ManagedAgentsSessionStatusIdleEvent struct {
	sessionEventBase
	StopReason *ManagedAgentsStopReason `json:"stop_reason,omitempty"`
}

// ManagedAgentsSessionStatusTerminatedEvent — the session was terminated
// (client delete or server-side kill). No further events will fire.
type ManagedAgentsSessionStatusTerminatedEvent struct {
	sessionEventBase
	StopReason *ManagedAgentsStopReason `json:"stop_reason,omitempty"`
}

// ManagedAgentsSessionErrorEvent — the runtime hit an unrecoverable
// error mid-turn (model provider down, tool crashed, etc.).
type ManagedAgentsSessionErrorEvent struct {
	sessionEventBase
	Error ManagedAgentsSessionErrorPayload `json:"error"`
}

// ---- Thread-lifecycle events ----------------------------------------------

// ManagedAgentsSessionThreadCreatedEvent — a sub-agent thread was spawned
// (either the primary thread on first turn or a sub-agent delegation).
type ManagedAgentsSessionThreadCreatedEvent struct {
	sessionEventBase
	SessionThreadID string `json:"session_thread_id"`
	AgentName       string `json:"agent_name"`
	ParentThreadID  string `json:"parent_thread_id,omitempty"`
}

// ManagedAgentsSessionThreadStatusRunningEvent — a specific thread is
// executing.
type ManagedAgentsSessionThreadStatusRunningEvent struct {
	sessionEventBase
	SessionThreadID string `json:"session_thread_id"`
	AgentName       string `json:"agent_name"`
}

// ManagedAgentsSessionThreadStatusIdleEvent — a specific thread finished
// and is idle.
type ManagedAgentsSessionThreadStatusIdleEvent struct {
	sessionEventBase
	SessionThreadID string                   `json:"session_thread_id"`
	AgentName       string                   `json:"agent_name"`
	StopReason      *ManagedAgentsStopReason `json:"stop_reason,omitempty"`
}

// ManagedAgentsSessionThreadStatusTerminatedEvent — a specific thread
// terminated.
type ManagedAgentsSessionThreadStatusTerminatedEvent struct {
	sessionEventBase
	SessionThreadID string                   `json:"session_thread_id"`
	AgentName       string                   `json:"agent_name"`
	StopReason      *ManagedAgentsStopReason `json:"stop_reason,omitempty"`
}

// ---- User-side echo events -------------------------------------------------

// ManagedAgentsUserMessageEvent — the server's echo of a
// user.message input event pushed via SendSessionEvents.
type ManagedAgentsUserMessageEvent struct {
	sessionEventBase
	SessionThreadID string                            `json:"session_thread_id,omitempty"`
	Content         []ManagedAgentsOutputContentBlock `json:"content,omitempty"`
}

// ManagedAgentsUserDefineOutcomeEvent — echo of a user.define_outcome
// input event.
type ManagedAgentsUserDefineOutcomeEvent struct {
	sessionEventBase
	OutcomeID     string          `json:"outcome_id"`
	Description   string          `json:"description"`
	MaxIterations int32           `json:"max_iterations,omitempty"`
	Rubric        json.RawMessage `json:"rubric,omitempty"`
}

// ManagedAgentsUserInterruptEvent — echo of a user.interrupt input event.
type ManagedAgentsUserInterruptEvent struct {
	sessionEventBase
	SessionThreadID string `json:"session_thread_id,omitempty"`
	Reason          string `json:"reason,omitempty"`
}

// ManagedAgentsUserToolConfirmationEvent — echo of a
// user.tool_confirmation input event.
type ManagedAgentsUserToolConfirmationEvent struct {
	sessionEventBase
	SessionThreadID string `json:"session_thread_id,omitempty"`
	ToolUseID       string `json:"tool_use_id"`
	Result          string `json:"result"`
	DenyMessage     string `json:"deny_message,omitempty"`
	TurnID          string `json:"turn_id,omitempty"`
}

// ---- Agent output events ---------------------------------------------------

// ManagedAgentsAgentMessageEvent — the assistant's response. `Content`
// is a list of typed blocks; the common case is a single
// `{type:"text", text:"..."}` block, but the agent may emit multiple.
type ManagedAgentsAgentMessageEvent struct {
	sessionEventBase
	SessionThreadID string                            `json:"session_thread_id,omitempty"`
	Content         []ManagedAgentsOutputContentBlock `json:"content,omitempty"`
}

// ManagedAgentsAgentThinkingEvent — the assistant's internal
// deliberation frame. `Content` may be empty or carry a thinking-mode
// summary block.
type ManagedAgentsAgentThinkingEvent struct {
	sessionEventBase
	SessionThreadID string                            `json:"session_thread_id,omitempty"`
	Content         []ManagedAgentsOutputContentBlock `json:"content,omitempty"`
}

// ManagedAgentsAgentToolUseEvent — the agent invoked a tool. The
// `Input` is the tool's argument object (schema per tool).
type ManagedAgentsAgentToolUseEvent struct {
	sessionEventBase
	SessionThreadID     string          `json:"session_thread_id,omitempty"`
	ToolUseID           string          `json:"tool_use_id"`
	Name                string          `json:"name"`
	Input               json.RawMessage `json:"input,omitempty"`
	EvaluatedPermission string          `json:"evaluated_permission,omitempty"`
}

// ManagedAgentsAgentToolResultEvent — the tool's result (post
// user.tool_result if the tool required client-side execution, or
// runtime-owned if it was a builtin/MCP tool).
type ManagedAgentsAgentToolResultEvent struct {
	sessionEventBase
	SessionThreadID string                            `json:"session_thread_id,omitempty"`
	ToolUseID       string                            `json:"tool_use_id"`
	Content         []ManagedAgentsOutputContentBlock `json:"content,omitempty"`
	IsError         bool                              `json:"is_error,omitempty"`
}

// ManagedAgentsAgentCustomToolUseEvent — the agent invoked a user-defined
// custom tool handled by the self-hosted worker.
type ManagedAgentsAgentCustomToolUseEvent struct {
	sessionEventBase
	SessionThreadID string          `json:"session_thread_id,omitempty"`
	CustomToolUseID string          `json:"custom_tool_use_id,omitempty"`
	Name            string          `json:"name"`
	Input           json.RawMessage `json:"input,omitempty"`
}

// ManagedAgentsAgentMCPToolUseEvent — the agent invoked an MCP tool
// (registered via a Vault credential). Distinct wire event from
// agent.tool_use to signal the extra `mcp_server_name` routing metadata.
type ManagedAgentsAgentMCPToolUseEvent struct {
	sessionEventBase
	SessionThreadID string          `json:"session_thread_id,omitempty"`
	ToolUseID       string          `json:"tool_use_id"`
	MCPServerName   string          `json:"mcp_server_name"`
	Name            string          `json:"name"`
	Input           json.RawMessage `json:"input,omitempty"`
}

// ManagedAgentsAgentMCPToolResultEvent — result of an MCP tool
// invocation. Same shape as agent.tool_result but on the MCP path.
type ManagedAgentsAgentMCPToolResultEvent struct {
	sessionEventBase
	SessionThreadID string                            `json:"session_thread_id,omitempty"`
	ToolUseID       string                            `json:"tool_use_id"`
	MCPServerName   string                            `json:"mcp_server_name,omitempty"`
	Content         []ManagedAgentsOutputContentBlock `json:"content,omitempty"`
	IsError         bool                              `json:"is_error,omitempty"`
}

// ManagedAgentsAgentThreadContextCompactedEvent — the runtime summarized
// an older section of thread history to fit under the token budget.
// Subsequent turns run against the compacted context.
type ManagedAgentsAgentThreadContextCompactedEvent struct {
	sessionEventBase
	SessionThreadID string `json:"session_thread_id,omitempty"`
	Summary         string `json:"summary,omitempty"`
}

// ---- Cross-thread relay events --------------------------------------------

// ManagedAgentsAgentThreadMessageSentEvent — a coordinator agent sent a
// delegation prompt to a sub-agent thread (multiagent).
type ManagedAgentsAgentThreadMessageSentEvent struct {
	sessionEventBase
	FromSessionThreadID string `json:"from_session_thread_id"`
	ToSessionThreadID   string `json:"to_session_thread_id"`
}

// ManagedAgentsAgentThreadMessageReceivedEvent — a sub-agent received a
// coordinator's delegation prompt (multiagent).
type ManagedAgentsAgentThreadMessageReceivedEvent struct {
	sessionEventBase
	FromSessionThreadID string `json:"from_session_thread_id"`
	ToSessionThreadID   string `json:"to_session_thread_id"`
}

// ---- Span (observability) events ------------------------------------------

// ManagedAgentsSpanModelRequestStartEvent — a model provider call was
// about to be issued. Pair with the matching end event.
type ManagedAgentsSpanModelRequestStartEvent struct {
	sessionEventBase
	SessionThreadID string `json:"session_thread_id,omitempty"`
}

// ManagedAgentsSpanModelRequestEndEvent — a model provider call
// completed (success or error). `ModelUsage` reports token accounting.
type ManagedAgentsSpanModelRequestEndEvent struct {
	sessionEventBase
	SessionThreadID     string                   `json:"session_thread_id,omitempty"`
	ModelRequestStartID string                   `json:"model_request_start_id"`
	ModelUsage          *ManagedAgentsModelUsage `json:"model_usage,omitempty"`
	IsError             bool                     `json:"is_error,omitempty"`
	ErrorMessage        string                   `json:"error_message,omitempty"`
}

// ManagedAgentsSpanOutcomeEvaluationStartEvent — the runtime kicked off
// grading a completed iteration against the user's rubric.
type ManagedAgentsSpanOutcomeEvaluationStartEvent struct {
	sessionEventBase
	Iteration int32  `json:"iteration"`
	OutcomeID string `json:"outcome_id"`
}

// ManagedAgentsSpanOutcomeEvaluationOngoingEvent — periodic progress
// while the grader is running.
type ManagedAgentsSpanOutcomeEvaluationOngoingEvent struct {
	sessionEventBase
	OutcomeID string `json:"outcome_id"`
}

// ManagedAgentsSpanOutcomeEvaluationEndEvent — grader verdict for one
// iteration. Common `Result` values: "satisfied" / "needs_revision" /
// "max_iterations_reached" / "failed" / "interrupted".
type ManagedAgentsSpanOutcomeEvaluationEndEvent struct {
	sessionEventBase
	Iteration                int32                    `json:"iteration"`
	OutcomeEvaluationStartID string                   `json:"outcome_evaluation_start_id"`
	OutcomeID                string                   `json:"outcome_id"`
	Result                   string                   `json:"result"`
	Explanation              string                   `json:"explanation,omitempty"`
	Usage                    *ManagedAgentsModelUsage `json:"usage,omitempty"`
}

// ---- Fallback -------------------------------------------------------------

// ManagedAgentsUnknownSessionEvent is what the decoder yields for wire
// events this SDK version doesn't have a typed variant for yet. It
// preserves the untouched JSON payload in RawPayload so callers can
// json.Unmarshal into their own struct if needed.
type ManagedAgentsUnknownSessionEvent struct {
	sessionEventBase
	RawPayload []byte
}

// ---- Decoder dispatch -----------------------------------------------------

// decodeSessionEvent parses a raw SSE data payload into the concrete
// typed variant matching its `type` discriminator. Unknown / malformed
// payloads become [ManagedAgentsUnknownSessionEvent] with the raw bytes
// preserved.
func decodeSessionEvent(raw []byte) ManagedAgentsSessionEvent {
	var peek struct {
		Type string `json:"type"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &peek) != nil {
		return &ManagedAgentsUnknownSessionEvent{
			sessionEventBase: sessionEventBase{EventType: peek.Type},
			RawPayload:       append([]byte(nil), raw...),
		}
	}
	var out ManagedAgentsSessionEvent
	switch peek.Type {
	case "session.status_running":
		out = &ManagedAgentsSessionStatusRunningEvent{}
	case "session.status_idle":
		out = &ManagedAgentsSessionStatusIdleEvent{}
	case "session.status_terminated":
		out = &ManagedAgentsSessionStatusTerminatedEvent{}
	case "session.error":
		out = &ManagedAgentsSessionErrorEvent{}
	case "session.thread_created":
		out = &ManagedAgentsSessionThreadCreatedEvent{}
	case "session.thread_status_running":
		out = &ManagedAgentsSessionThreadStatusRunningEvent{}
	case "session.thread_status_idle":
		out = &ManagedAgentsSessionThreadStatusIdleEvent{}
	case "session.thread_status_terminated":
		out = &ManagedAgentsSessionThreadStatusTerminatedEvent{}
	case "user.message":
		out = &ManagedAgentsUserMessageEvent{}
	case "user.define_outcome":
		out = &ManagedAgentsUserDefineOutcomeEvent{}
	case "user.interrupt":
		out = &ManagedAgentsUserInterruptEvent{}
	case "user.tool_confirmation":
		out = &ManagedAgentsUserToolConfirmationEvent{}
	case "agent.message":
		out = &ManagedAgentsAgentMessageEvent{}
	case "agent.thinking":
		out = &ManagedAgentsAgentThinkingEvent{}
	case "agent.tool_use":
		out = &ManagedAgentsAgentToolUseEvent{}
	case "agent.tool_result":
		out = &ManagedAgentsAgentToolResultEvent{}
	case "agent.custom_tool_use":
		out = &ManagedAgentsAgentCustomToolUseEvent{}
	case "agent.mcp_tool_use":
		out = &ManagedAgentsAgentMCPToolUseEvent{}
	case "agent.mcp_tool_result":
		out = &ManagedAgentsAgentMCPToolResultEvent{}
	case "agent.thread_context_compacted":
		out = &ManagedAgentsAgentThreadContextCompactedEvent{}
	case "agent.thread_message_sent":
		out = &ManagedAgentsAgentThreadMessageSentEvent{}
	case "agent.thread_message_received":
		out = &ManagedAgentsAgentThreadMessageReceivedEvent{}
	case "span.model_request_start":
		out = &ManagedAgentsSpanModelRequestStartEvent{}
	case "span.model_request_end":
		out = &ManagedAgentsSpanModelRequestEndEvent{}
	case "span.outcome_evaluation_start":
		out = &ManagedAgentsSpanOutcomeEvaluationStartEvent{}
	case "span.outcome_evaluation_ongoing":
		out = &ManagedAgentsSpanOutcomeEvaluationOngoingEvent{}
	case "span.outcome_evaluation_end":
		out = &ManagedAgentsSpanOutcomeEvaluationEndEvent{}
	default:
		return &ManagedAgentsUnknownSessionEvent{
			sessionEventBase: sessionEventBase{EventType: peek.Type},
			RawPayload:       append([]byte(nil), raw...),
		}
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return &ManagedAgentsUnknownSessionEvent{
			sessionEventBase: sessionEventBase{EventType: peek.Type},
			RawPayload:       append([]byte(nil), raw...),
		}
	}
	return out
}

// ---- Stream event envelope + decoder -------------------------------------

// StreamEvent is one decoded frame yielded by [StreamDecoder]. `Type`
// mirrors the effective wire discriminator (SSE `event:` line first,
// JSON `type` field second) so callers can `switch` on either
// `frame.Type` string or on `frame.Data.(type)` for typed dispatch.
// `RawPayload` preserves the untouched JSON body so consumers that
// need forward-compat access to fields not yet in the typed variants
// can Unmarshal it themselves.
type StreamEvent struct {
	// Type is the effective event type discriminator; equals Data.Type().
	Type string
	// Data is the typed session event, one of the ManagedAgents*Event
	// concrete structs above (or ManagedAgentsUnknownSessionEvent).
	Data ManagedAgentsSessionEvent
	// RawPayload is the untouched JSON body from the SSE `data:` line;
	// use it when Data doesn't cover a field you need (e.g. a wire
	// event this SDK version doesn't have a typed struct for).
	RawPayload []byte
}

// StreamDecoder wraps a raw [utils.EventStreamDecoder] and exposes each
// frame as a typed [StreamEvent]. Same iteration contract as the
// underlying decoder (Next → Event → Err), same Close semantics.
type StreamDecoder struct {
	raw *utils.EventStreamDecoder
	cur StreamEvent
	err error
}

// NewStreamDecoder wraps a raw SSE decoder so its frames come out typed.
func NewStreamDecoder(raw *utils.EventStreamDecoder) *StreamDecoder {
	return &StreamDecoder{raw: raw}
}

// Next advances to the next frame. Returns false at EOF or on any decode
// error (which is retrievable via Err).
func (d *StreamDecoder) Next() bool {
	if d.err != nil {
		return false
	}
	if !d.raw.Next() {
		d.err = d.raw.Err()
		return false
	}
	raw := d.raw.Event()
	data := decodeSessionEvent(raw.Data)
	typ := data.Type()
	if raw.Type != "" {
		// Explicit SSE `event:` line wins over JSON peeking when the
		// server actually emits one (rare on this API today).
		typ = raw.Type
	}
	d.cur = StreamEvent{
		Type:       typ,
		Data:       data,
		RawPayload: append([]byte(nil), raw.Data...),
	}
	return true
}

// Event returns the current frame — valid until the next Next call.
func (d *StreamDecoder) Event() StreamEvent {
	return d.cur
}

// Err returns the first non-nil error encountered by the underlying raw
// decoder, or nil at clean EOF.
func (d *StreamDecoder) Err() error {
	if d.err == nil {
		return nil
	}
	if strings.Contains(d.err.Error(), "EOF") && !errors.Is(d.err, ErrNonEOF) {
		return nil
	}
	return d.err
}

// Close closes the underlying SSE decoder / response body.
func (d *StreamDecoder) Close() error {
	return d.raw.Close()
}

// ErrNonEOF is a sentinel used internally to distinguish real errors from
// clean stream closure. Callers shouldn't reach for this directly.
var ErrNonEOF = errors.New("non-eof stream error")
