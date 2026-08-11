// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"net/url"

	"github.com/volcengine/ark-runtime-go/arkruntime/model"
	"github.com/volcengine/ark-runtime-go/arkruntime/pkg/apiquery"
)

// AgentResponse wraps Agent so it satisfies model.Response.
type AgentResponse struct {
	Agent
	model.HttpHeader
}

// ListAgentsResponseWrapper wraps ListAgentsResponse (also used for versions).
type ListAgentsResponseWrapper struct {
	ListAgentsResponse
	model.HttpHeader
}

// DeleteAgentResponseWrapper wraps DeleteAgentResponse.
type DeleteAgentResponseWrapper struct {
	DeleteAgentResponse
	model.HttpHeader
}

// URLQueryList encodes ListAgents query params.
func URLQueryList(req *AgentsListParams) (url.Values, error) {
	if req == nil {
		return url.Values{}, nil
	}
	return apiquery.Marshal(req)
}

// URLQueryVersions encodes ListAgentVersions query params.
func URLQueryVersions(req *AgentsListVersionsParams) (url.Values, error) {
	if req == nil {
		return url.Values{}, nil
	}
	return apiquery.Marshal(req)
}

// PathEscape hides url.PathEscape at the shim boundary.
func PathEscape(s string) string { return url.PathEscape(s) }
