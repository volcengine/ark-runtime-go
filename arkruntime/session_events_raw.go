// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/session"
)

// SendSessionEventsRaw sends raw session event payloads.
//
// This is primarily used by self-hosted workers, where the event shape is
// already produced by the local runner and must be passed through without
// losing forward-compatible fields.
func (c *Client) SendSessionEventsRaw(
	ctx context.Context,
	sessionID string,
	events []any,
	setters ...requestOption,
) error {
	if sessionID == "" {
		return errors.New("missing required session_id")
	}
	if len(events) == 0 {
		return errors.New("missing required events")
	}
	body := map[string]any{"events": events}
	u := c.fullURL(fmt.Sprintf("%s/%s/events", sessionsPrefix, session.PathEscape(sessionID)))
	wrap := &session.SendSessionEventsResponseWrapper{}
	return c.doControlPlaneRequest(ctx, http.MethodPost, u, wrap, append(setters, withBody(body))...)
}

// SendSessionEventRaw sends one raw session event payload.
func (c *Client) SendSessionEventRaw(
	ctx context.Context,
	sessionID string,
	event any,
	setters ...requestOption,
) error {
	if event == nil {
		return errors.New("missing required event")
	}
	return c.SendSessionEventsRaw(ctx, sessionID, []any{event}, setters...)
}
