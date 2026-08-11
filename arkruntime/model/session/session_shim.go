// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"net/url"

	"github.com/volcengine/ark-runtime-go/arkruntime/model"
	"github.com/volcengine/ark-runtime-go/arkruntime/pkg/apiquery"
)

// --- Session ----------------------------------------------------------------

type SessionResponse struct {
	Session
	model.HttpHeader
}

type ListSessionsResponseWrapper struct {
	ListSessionsResponse
	model.HttpHeader
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
