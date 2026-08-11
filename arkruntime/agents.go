// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/agent"
)

const agentsPrefix = "/agents"

// CreateAgent creates a new Agent.
func (c *Client) CreateAgent(
	ctx context.Context,
	body *agent.CreateAgentRequest,
	setters ...requestOption,
) (*agent.Agent, error) {
	if body == nil {
		return nil, errors.New("missing required request body")
	}
	opts := append(setters, withBody(body))
	wrap := &agent.AgentResponse{}
	if err := c.Do(ctx, http.MethodPost, c.fullURL(agentsPrefix), "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Agent, nil
}

// GetAgent retrieves an Agent by ID.
func (c *Client) GetAgent(ctx context.Context, agentID string, setters ...requestOption) (*agent.Agent, error) {
	if agentID == "" {
		return nil, errors.New("missing required agent_id")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", agentsPrefix, agent.PathEscape(agentID)))
	opts := append(setters, withBody(nil))
	wrap := &agent.AgentResponse{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Agent, nil
}

// ListAgents lists Agents with optional query filters.
func (c *Client) ListAgents(
	ctx context.Context,
	params *agent.AgentsListParams,
	setters ...requestOption,
) (*agent.ListAgentsResponse, error) {
	q, qerr := agent.URLQueryList(params)
	if qerr != nil {
		return nil, qerr
	}
	u := c.fullURL(agentsPrefix)
	if encoded := q.Encode(); encoded != "" {
		u = u + "?" + encoded
	}
	opts := append(setters, withBody(nil))
	wrap := &agent.ListAgentsResponseWrapper{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.ListAgentsResponse, nil
}

// UpdateAgent updates an Agent (optimistic concurrency via body.Version).
func (c *Client) UpdateAgent(
	ctx context.Context,
	agentID string,
	body *agent.UpdateAgentRequest,
	setters ...requestOption,
) (*agent.Agent, error) {
	if agentID == "" {
		return nil, errors.New("missing required agent_id")
	}
	if body == nil {
		return nil, errors.New("missing required request body")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", agentsPrefix, agent.PathEscape(agentID)))
	opts := append(setters, withBody(body))
	wrap := &agent.AgentResponse{}
	if err := c.Do(ctx, http.MethodPost, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Agent, nil
}

// DeleteAgent deletes an Agent.
func (c *Client) DeleteAgent(
	ctx context.Context,
	agentID string,
	setters ...requestOption,
) (*agent.DeleteAgentResponse, error) {
	if agentID == "" {
		return nil, errors.New("missing required agent_id")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", agentsPrefix, agent.PathEscape(agentID)))
	opts := append(setters, withBody(nil))
	wrap := &agent.DeleteAgentResponseWrapper{}
	if err := c.Do(ctx, http.MethodDelete, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.DeleteAgentResponse, nil
}

// ListAgentVersions lists all historical versions of the given Agent.
func (c *Client) ListAgentVersions(
	ctx context.Context,
	agentID string,
	params *agent.AgentsListVersionsParams,
	setters ...requestOption,
) (*agent.ListAgentsResponse, error) {
	if agentID == "" {
		return nil, errors.New("missing required agent_id")
	}
	q, qerr := agent.URLQueryVersions(params)
	if qerr != nil {
		return nil, qerr
	}
	u := c.fullURL(fmt.Sprintf("%s/%s/versions", agentsPrefix, agent.PathEscape(agentID)))
	if encoded := q.Encode(); encoded != "" {
		u = u + "?" + encoded
	}
	opts := append(setters, withBody(nil))
	wrap := &agent.ListAgentsResponseWrapper{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.ListAgentsResponse, nil
}
