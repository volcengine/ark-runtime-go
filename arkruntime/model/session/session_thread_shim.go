// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"encoding/json"
	"net/url"

	"github.com/volcengine/ark-runtime-go/arkruntime/model"
	"github.com/volcengine/ark-runtime-go/arkruntime/pkg/apiquery"
)

// This file extends session_shim.go with header-carrying wrappers +
// URLQuery helpers for the endpoints ogen generated schemas for but the
// hand-written Client (sessions.go) drives directly.
//
// The wire response for ListSessionEvents / ListSessionThreadEvents is a
// heterogeneous JSON union (30+ variants) — ogen models it as
// map[string]jx.Raw, which is technically accurate but unusable in caller
// code. We hand-write ListSessionEventsResponse here with a fully typed
// Events slice + custom UnmarshalJSON that dispatches into the concrete
// ManagedAgents*Event variants defined in session_stream.go. The raw
// codegen shape lives on as ListSessionEventsResponseWire (referenced by
// the OpenAPI schema) but never leaks past this shim.

// ---- Public response type — hand-written, no raw-JSON escape hatch ----

// ListSessionEventsResponse is the typed response for
// Client.ListSessionEvents and Client.ListSessionThreadEvents. Every wire
// event is dispatched into its concrete ManagedAgents*Event variant on
// unmarshal — callers consume via a Go type-switch:
//
//	resp, _ := c.ListSessionEvents(ctx, sid, nil)
//	for _, ev := range resp.Events {
//	    switch e := ev.(type) {
//	    case *session.ManagedAgentsAgentMessageEvent:
//	        for _, b := range e.Content { fmt.Print(b.Text) }
//	    case *session.ManagedAgentsSpanOutcomeEvaluationEndEvent:
//	        fmt.Printf("outcome=%s: %s\n", e.Result, e.Explanation)
//	    case *session.ManagedAgentsUnknownSessionEvent:
//	        // SDK doesn't yet type this wire variant — e.RawPayload
//	        // preserves the JSON body.
//	    }
//	}
type ListSessionEventsResponse struct {
	// Events is the page of wire events, each decoded into its concrete
	// ManagedAgents*Event variant. Items whose `type` this SDK version
	// doesn't recognize surface as *ManagedAgentsUnknownSessionEvent.
	Events []ManagedAgentsSessionEvent

	// NextPage is the cursor for the next page of results; empty when the
	// current page is the last.
	NextPage OptString
}

// UnmarshalJSON decodes the raw wire response (with untyped `data[]`)
// into the typed Events slice. Uses the same decodeSessionEvent
// dispatcher that the SSE stream decoder uses, so list-events and
// stream-events produce identical typed structs.
func (r *ListSessionEventsResponse) UnmarshalJSON(b []byte) error {
	var raw struct {
		Data     []json.RawMessage `json:"data"`
		NextPage OptString         `json:"next_page"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	r.NextPage = raw.NextPage
	r.Events = make([]ManagedAgentsSessionEvent, 0, len(raw.Data))
	for _, item := range raw.Data {
		r.Events = append(r.Events, decodeSessionEvent(item))
	}
	return nil
}

// ---- Wrappers for Client.Do ------------------------------------------------

// ListSessionEventsResponseWrapper embeds ListSessionEventsResponse and the
// standard HttpHeader so it satisfies model.Response.
type ListSessionEventsResponseWrapper struct {
	ListSessionEventsResponse
	model.HttpHeader
}

// ListSessionThreadsResponseWrapper embeds ListSessionThreadsResponse.
type ListSessionThreadsResponseWrapper struct {
	ListSessionThreadsResponse
	model.HttpHeader
}

// SessionThreadResponse — carrier for GetSessionThread. Server flattens
// SessionThread to the top level, so we embed the struct directly.
type SessionThreadResponse struct {
	SessionThread
	model.HttpHeader
}

// ---- URL query encoders ----------------------------------------------------

// URLQuerySessionEventsList encodes SessionEventsListEventsParams to
// url.Values. The ogen-generated struct carries `query:"..."` tags on each
// field; apiquery.Marshal picks them up directly.
func URLQuerySessionEventsList(req *SessionEventsListEventsParams) (url.Values, error) {
	if req == nil {
		return url.Values{}, nil
	}
	return apiquery.Marshal(req)
}

// URLQuerySessionEventsStream encodes SessionEventsStreamEventsParams.
func URLQuerySessionEventsStream(req *SessionEventsStreamEventsParams) (url.Values, error) {
	if req == nil {
		return url.Values{}, nil
	}
	return apiquery.Marshal(req)
}

// URLQuerySessionThreadsList encodes SessionThreadsListParams.
func URLQuerySessionThreadsList(req *SessionThreadsListParams) (url.Values, error) {
	if req == nil {
		return url.Values{}, nil
	}
	return apiquery.Marshal(req)
}

// URLQuerySessionThreadEventsList encodes SessionThreadsListEventsParams.
func URLQuerySessionThreadEventsList(req *SessionThreadsListEventsParams) (url.Values, error) {
	if req == nil {
		return url.Values{}, nil
	}
	return apiquery.Marshal(req)
}

// URLQuerySessionThreadEventsStream encodes SessionThreadsStreamEventsParams.
func URLQuerySessionThreadEventsStream(req *SessionThreadsStreamEventsParams) (url.Values, error) {
	if req == nil {
		return url.Values{}, nil
	}
	return apiquery.Marshal(req)
}
