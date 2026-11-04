// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"bytes"
	"encoding/json"
	"net/url"

	"github.com/volcengine/ark-runtime-go/arkruntime/internal/listresponse"
	"github.com/volcengine/ark-runtime-go/arkruntime/model"
	"github.com/volcengine/ark-runtime-go/arkruntime/pkg/apiquery"
)

// --- Session ----------------------------------------------------------------

type SessionResponse struct {
	Session
	model.HttpHeader
}

func (s *SessionResponse) UnmarshalJSON(data []byte) error {
	normalized, err := normalizeSessionPayload(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(normalized, &s.Session)
}

func normalizeSessionPayload(data []byte) ([]byte, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	deleteNullRawField(payload, "archived_at")
	for _, field := range []string{"agent", "environment"} {
		raw, ok := payload[field]
		if !ok {
			continue
		}
		if normalized, changed := normalizeResourceArchivedAt(raw); changed {
			payload[field] = normalized
		}
	}
	return json.Marshal(payload)
}

func normalizeResourceArchivedAt(raw json.RawMessage) (json.RawMessage, bool) {
	var resource map[string]json.RawMessage
	if err := json.Unmarshal(raw, &resource); err != nil || resource == nil {
		return raw, false
	}
	if !deleteNullRawField(resource, "archived_at") {
		return raw, false
	}
	normalized, err := json.Marshal(resource)
	if err != nil {
		return raw, false
	}
	return normalized, true
}

func deleteNullRawField(object map[string]json.RawMessage, field string) bool {
	raw, ok := object[field]
	if !ok || !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return false
	}
	delete(object, field)
	return true
}

type ListSessionsResponseWrapper struct {
	ListSessionsResponse
	model.HttpHeader
}

func (s *ListSessionsResponseWrapper) UnmarshalJSON(data []byte) error {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	rawSessions, ok := payload["data"]
	if ok {
		var sessions []json.RawMessage
		if err := json.Unmarshal(rawSessions, &sessions); err != nil {
			return err
		}
		for i, raw := range sessions {
			normalized, err := normalizeSessionPayload(raw)
			if err != nil {
				return err
			}
			sessions[i] = normalized
		}
		normalized, err := json.Marshal(sessions)
		if err != nil {
			return err
		}
		payload["data"] = normalized
	}
	normalized, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return json.Unmarshal(normalized, &s.ListSessionsResponse)
}

type DeleteSessionResponseWrapper struct {
	DeleteSessionResponse
	model.HttpHeader
}

// --- SessionResource --------------------------------------------------------

type SessionResourceResponse struct {
	SessionResource
	model.HttpHeader
}

type ListSessionResourcesResponseWrapper struct {
	ListSessionResourcesResponse
	model.HttpHeader
}

// UnmarshalJSON normalizes legacy null pages without changing generated models.
func (r *ListSessionResourcesResponseWrapper) UnmarshalJSON(data []byte) error {
	return r.ListSessionResourcesResponse.UnmarshalJSON(listresponse.Normalize(data, `{"data":[]}`))
}

// --- SessionEvent -----------------------------------------------------------

type SendSessionEventsResponseWrapper struct {
	SendSessionEventsResponse
	model.HttpHeader
}

// --- URL helpers ------------------------------------------------------------

func URLQuerySessionsList(req *SessionsListParams) (url.Values, error) {
	if req == nil {
		return url.Values{}, nil
	}
	return apiquery.Marshal(req)
}

func PathEscape(s string) string { return url.PathEscape(s) }
