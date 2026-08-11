// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/environment"
)

const environmentsPrefix = "/environments"

// CreateEnvironment creates a new Environment.
func (c *Client) CreateEnvironment(
	ctx context.Context,
	body *environment.CreateEnvironmentRequest,
	setters ...requestOption,
) (*environment.Environment, error) {
	if body == nil {
		return nil, errors.New("missing required request body")
	}
	opts := append(setters, withBody(body))
	wrap := &environment.EnvironmentResponse{}
	if err := c.Do(ctx, http.MethodPost, c.fullURL(environmentsPrefix), "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Environment, nil
}

// GetEnvironment retrieves an Environment by ID.
func (c *Client) GetEnvironment(
	ctx context.Context,
	environmentID string,
	setters ...requestOption,
) (*environment.Environment, error) {
	if environmentID == "" {
		return nil, errors.New("missing required environment_id")
	}
	opts := append(setters, withBody(nil))
	u := c.fullURL(fmt.Sprintf("%s/%s", environmentsPrefix, environment.PathEscape(environmentID)))
	wrap := &environment.EnvironmentResponse{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Environment, nil
}

// ListEnvironments lists Environments with optional query filters.
func (c *Client) ListEnvironments(
	ctx context.Context,
	params *environment.EnvironmentsListParams,
	setters ...requestOption,
) (*environment.ListEnvironmentsResponse, error) {
	q, qerr := environment.URLQuery(params)
	if qerr != nil {
		return nil, qerr
	}
	u := c.fullURL(environmentsPrefix)
	if encoded := q.Encode(); encoded != "" {
		u = u + "?" + encoded
	}
	opts := append(setters, withBody(nil))
	wrap := &environment.ListEnvironmentsResponseWrapper{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.ListEnvironmentsResponse, nil
}

// UpdateEnvironment updates an existing Environment.
func (c *Client) UpdateEnvironment(
	ctx context.Context,
	environmentID string,
	body *environment.UpdateEnvironmentRequest,
	setters ...requestOption,
) (*environment.Environment, error) {
	if environmentID == "" {
		return nil, errors.New("missing required environment_id")
	}
	if body == nil {
		return nil, errors.New("missing required request body")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", environmentsPrefix, environment.PathEscape(environmentID)))
	opts := append(setters, withBody(body))
	wrap := &environment.EnvironmentResponse{}
	if err := c.Do(ctx, http.MethodPost, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Environment, nil
}

// DeleteEnvironment deletes an Environment.
func (c *Client) DeleteEnvironment(
	ctx context.Context,
	environmentID string,
	setters ...requestOption,
) (*environment.DeleteEnvironmentResponse, error) {
	if environmentID == "" {
		return nil, errors.New("missing required environment_id")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", environmentsPrefix, environment.PathEscape(environmentID)))
	opts := append(setters, withBody(nil))
	wrap := &environment.DeleteEnvironmentResponseWrapper{}
	if err := c.Do(ctx, http.MethodDelete, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.DeleteEnvironmentResponse, nil
}
