// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/session"
	"github.com/volcengine/ark-runtime-go/arkruntime/utils"
)

const sessionsPrefix = "/sessions"

// ---- Session CRUD ---------------------------------------------------------

// CreateSession creates a new Session (binds an Agent to an Environment).
func (c *Client) CreateSession(
	ctx context.Context,
	body *session.CreateSessionRequest,
	setters ...requestOption,
) (*session.Session, error) {
	if body == nil {
		return nil, errors.New("missing required request body")
	}
	opts := append(setters, withBody(body))
	wrap := &session.SessionResponse{}
	if err := c.Do(ctx, http.MethodPost, c.fullURL(sessionsPrefix), "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Session, nil
}

// GetSession retrieves a Session by ID.
func (c *Client) GetSession(
	ctx context.Context,
	sessionID string,
	setters ...requestOption,
) (*session.Session, error) {
	if sessionID == "" {
		return nil, errors.New("missing required session_id")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", sessionsPrefix, session.PathEscape(sessionID)))
	opts := append(setters, withBody(nil))
	wrap := &session.SessionResponse{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Session, nil
}

// ListSessions lists Sessions with optional filters.
func (c *Client) ListSessions(
	ctx context.Context,
	params *session.SessionsListParams,
	setters ...requestOption,
) (*session.ListSessionsResponse, error) {
	q, qerr := session.URLQuerySessionsList(params)
	if qerr != nil {
		return nil, qerr
	}
	u := c.fullURL(sessionsPrefix)
	if encoded := q.Encode(); encoded != "" {
		u = u + "?" + encoded
	}
	opts := append(setters, withBody(nil))
	wrap := &session.ListSessionsResponseWrapper{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.ListSessionsResponse, nil
}

// UpdateSession updates a Session's title / tags.
func (c *Client) UpdateSession(
	ctx context.Context,
	sessionID string,
	body *session.UpdateSessionRequest,
	setters ...requestOption,
) (*session.Session, error) {
	if sessionID == "" {
		return nil, errors.New("missing required session_id")
	}
	if body == nil {
		return nil, errors.New("missing required request body")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", sessionsPrefix, session.PathEscape(sessionID)))
	opts := append(setters, withBody(body))
	wrap := &session.SessionResponse{}
	if err := c.Do(ctx, http.MethodPost, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Session, nil
}

// DeleteSession deletes a Session (must be idle / terminated).
func (c *Client) DeleteSession(
	ctx context.Context,
	sessionID string,
	setters ...requestOption,
) (*session.DeleteSessionResponse, error) {
	if sessionID == "" {
		return nil, errors.New("missing required session_id")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", sessionsPrefix, session.PathEscape(sessionID)))
	opts := append(setters, withBody(nil))
	wrap := &session.DeleteSessionResponseWrapper{}
	if err := c.Do(ctx, http.MethodDelete, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.DeleteSessionResponse, nil
}

// ---- SessionResource (nested under Session) --------------------------------

func sessionResourcesPath(sessionID string) string {
	return fmt.Sprintf("%s/%s/resources", sessionsPrefix, session.PathEscape(sessionID))
}

// CreateSessionResource mounts a file resource on the given session.
func (c *Client) CreateSessionResource(
	ctx context.Context,
	sessionID string,
	body *session.CreateSessionResourceRequest,
	setters ...requestOption,
) (*session.SessionResource, error) {
	if sessionID == "" {
		return nil, errors.New("missing required session_id")
	}
	if body == nil {
		return nil, errors.New("missing required request body")
	}
	opts := append(setters, withBody(body))
	wrap := &session.SessionResourceResponse{}
	if err := c.Do(ctx, http.MethodPost, c.fullURL(sessionResourcesPath(sessionID)), "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.SessionResource, nil
}

// ListSessionResources lists all resources mounted on the given session.
func (c *Client) ListSessionResources(
	ctx context.Context,
	sessionID string,
	setters ...requestOption,
) (*session.ListSessionResourcesResponse, error) {
	if sessionID == "" {
		return nil, errors.New("missing required session_id")
	}
	opts := append(setters, withBody(nil))
	wrap := &session.ListSessionResourcesResponseWrapper{}
	if err := c.Do(ctx, http.MethodGet, c.fullURL(sessionResourcesPath(sessionID)), "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.ListSessionResourcesResponse, nil
}

// ---- SessionEvent (Send / Status / Stream) --------------------------------

// SendSessionEvents pushes one or more events to the given session.
func (c *Client) SendSessionEvents(
	ctx context.Context,
	sessionID string,
	body *session.SendSessionEventsRequest,
	setters ...requestOption,
) (*session.SendSessionEventsResponse, error) {
	if sessionID == "" {
		return nil, errors.New("missing required session_id")
	}
	if body == nil {
		return nil, errors.New("missing required request body")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s/events", sessionsPrefix, session.PathEscape(sessionID)))
	opts := append(setters, withBody(body))
	wrap := &session.SendSessionEventsResponseWrapper{}
	if err := c.Do(ctx, http.MethodPost, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.SendSessionEventsResponse, nil
}

// StreamSessionEvents opens a text/event-stream long-lived connection to the
// given session's events. The returned *session.StreamDecoder yields typed
// session.StreamEvent frames — each frame's Data is a
// session.ManagedAgentsSessionEvent so callers can dispatch on .GetType()
// (agent.message / span.* / session.*) without re-parsing the wire body.
// The default envelope also exposes Raw() []byte for callers that need to
// Unmarshal the payload into a discriminator-specific type.
//
// The caller MUST Close the decoder when done; otherwise the underlying
// HTTP response body leaks.
func (c *Client) StreamSessionEvents(
	ctx context.Context,
	sessionID string,
	setters ...requestOption,
) (*session.StreamDecoder, error) {
	return c.StreamSessionEventsWithParams(ctx, sessionID, nil, setters...)
}

// StreamSessionEventsWithParams opens a text/event-stream connection with
// query parameters such as event_deltas.
func (c *Client) StreamSessionEventsWithParams(
	ctx context.Context,
	sessionID string,
	params *session.SessionEventsStreamEventsParams,
	setters ...requestOption,
) (*session.StreamDecoder, error) {
	if sessionID == "" {
		return nil, errors.New("missing required session_id")
	}
	q, qerr := session.URLQuerySessionEventsStream(params)
	if qerr != nil {
		return nil, qerr
	}
	u := c.fullURL(fmt.Sprintf("%s/%s/events/stream", sessionsPrefix, session.PathEscape(sessionID)))
	if encoded := q.Encode(); encoded != "" {
		u = u + "?" + encoded
	}

	opts := append(setters, withBody(nil), WithCustomHeader("Accept", "text/event-stream"))
	req, reqErr := c.newRequest(ctx, http.MethodGet, u, "", "", opts...)
	if reqErr != nil {
		return nil, reqErr
	}
	resp, err := c.config.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		// Delegate to the shared error decoder so callers see a typed
		// *model.APIError (with HTTPStatusCode + request_id + code)
		// instead of a bare "stream open failed" string — matches the
		// contract of every non-stream method on this client.
		defer func() { _ = resp.Body.Close() }()
		return nil, c.handleErrorResp(resp)
	}
	return session.NewStreamDecoder(utils.NewEventStreamDecoder(resp.Body)), nil
}

// ListSessionEvents lists the historical events on a session. The `data`
// field carries the raw SessionEvent union (30+ variants); each element is
// returned as `map[string]any` so callers can dispatch on `ev["type"]`.
//
// The response also carries `next_page` for cursor pagination.
func (c *Client) ListSessionEvents(
	ctx context.Context,
	sessionID string,
	params *session.SessionEventsListEventsParams,
	setters ...requestOption,
) (*session.ListSessionEventsResponse, error) {
	if sessionID == "" {
		return nil, errors.New("missing required session_id")
	}
	q, qerr := session.URLQuerySessionEventsList(params)
	if qerr != nil {
		return nil, qerr
	}
	u := c.fullURL(fmt.Sprintf("%s/%s/events", sessionsPrefix, session.PathEscape(sessionID)))
	if encoded := q.Encode(); encoded != "" {
		u = u + "?" + encoded
	}
	opts := append(setters, withBody(nil))
	wrap := &session.ListSessionEventsResponseWrapper{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.ListSessionEventsResponse, nil
}

// ---- SessionThread (nested under Session) ---------------------------------

func sessionThreadsPath(sessionID string) string {
	return fmt.Sprintf("%s/%s/threads", sessionsPrefix, session.PathEscape(sessionID))
}

func sessionThreadPath(sessionID, threadID string) string {
	return fmt.Sprintf("%s/%s/threads/%s",
		sessionsPrefix, session.PathEscape(sessionID), session.PathEscape(threadID))
}

// ListSessionThreads returns every thread in the session (primary +
// subagent-spawned). Order: primary first, then children by spawn time asc.
//
// Note: primary thread is materialized lazily on the first event — callers
// may need to send at least one user.message + wait for status_idle before
// the list is populated.
func (c *Client) ListSessionThreads(
	ctx context.Context,
	sessionID string,
	params *session.SessionThreadsListParams,
	setters ...requestOption,
) (*session.ListSessionThreadsResponse, error) {
	if sessionID == "" {
		return nil, errors.New("missing required session_id")
	}
	q, qerr := session.URLQuerySessionThreadsList(params)
	if qerr != nil {
		return nil, qerr
	}
	u := c.fullURL(sessionThreadsPath(sessionID))
	if encoded := q.Encode(); encoded != "" {
		u = u + "?" + encoded
	}
	opts := append(setters, withBody(nil))
	wrap := &session.ListSessionThreadsResponseWrapper{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.ListSessionThreadsResponse, nil
}

// GetSessionThread retrieves a single thread by ID.
func (c *Client) GetSessionThread(
	ctx context.Context,
	sessionID, threadID string,
	setters ...requestOption,
) (*session.SessionThread, error) {
	if sessionID == "" {
		return nil, errors.New("missing required session_id")
	}
	if threadID == "" {
		return nil, errors.New("missing required thread_id")
	}
	opts := append(setters, withBody(nil))
	wrap := &session.SessionThreadResponse{}
	if err := c.Do(ctx, http.MethodGet, c.fullURL(sessionThreadPath(sessionID, threadID)),
		"", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.SessionThread, nil
}

// ListSessionThreadEvents lists events attributed to a specific thread.
// Query semantics mirror ListSessionEvents (types/limit/order/created_at
// bounds/page).
func (c *Client) ListSessionThreadEvents(
	ctx context.Context,
	sessionID, threadID string,
	params *session.SessionThreadsListEventsParams,
	setters ...requestOption,
) (*session.ListSessionEventsResponse, error) {
	if sessionID == "" {
		return nil, errors.New("missing required session_id")
	}
	if threadID == "" {
		return nil, errors.New("missing required thread_id")
	}
	q, qerr := session.URLQuerySessionThreadEventsList(params)
	if qerr != nil {
		return nil, qerr
	}
	u := c.fullURL(sessionThreadPath(sessionID, threadID) + "/events")
	if encoded := q.Encode(); encoded != "" {
		u = u + "?" + encoded
	}
	opts := append(setters, withBody(nil))
	wrap := &session.ListSessionEventsResponseWrapper{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.ListSessionEventsResponse, nil
}

// StreamSessionThreadEvents opens an SSE long-lived connection scoped to
// a single thread. Same lifecycle as StreamSessionEvents: caller MUST
// Close the returned decoder, no history replay, `data: [DONE]` frame
// terminates the stream.
func (c *Client) StreamSessionThreadEvents(
	ctx context.Context,
	sessionID, threadID string,
	setters ...requestOption,
) (*session.StreamDecoder, error) {
	return c.StreamSessionThreadEventsWithParams(ctx, sessionID, threadID, nil, setters...)
}

// StreamSessionThreadEventsWithParams opens a thread-scoped SSE stream with
// query parameters such as event_deltas.
func (c *Client) StreamSessionThreadEventsWithParams(
	ctx context.Context,
	sessionID, threadID string,
	params *session.SessionThreadsStreamEventsParams,
	setters ...requestOption,
) (*session.StreamDecoder, error) {
	if sessionID == "" {
		return nil, errors.New("missing required session_id")
	}
	if threadID == "" {
		return nil, errors.New("missing required thread_id")
	}
	q, qerr := session.URLQuerySessionThreadEventsStream(params)
	if qerr != nil {
		return nil, qerr
	}
	u := c.fullURL(sessionThreadPath(sessionID, threadID) + "/stream")
	if encoded := q.Encode(); encoded != "" {
		u = u + "?" + encoded
	}
	opts := append(setters, withBody(nil), WithCustomHeader("Accept", "text/event-stream"))
	req, reqErr := c.newRequest(ctx, http.MethodGet, u, "", "", opts...)
	if reqErr != nil {
		return nil, reqErr
	}
	resp, err := c.config.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		// Delegate to the shared error decoder so callers see a typed
		// *model.APIError (with HTTPStatusCode + request_id + code)
		// instead of a bare "stream open failed" string — matches the
		// contract of every non-stream method on this client.
		defer func() { _ = resp.Body.Close() }()
		return nil, c.handleErrorResp(resp)
	}
	return session.NewStreamDecoder(utils.NewEventStreamDecoder(resp.Body)), nil
}
