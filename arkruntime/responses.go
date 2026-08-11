// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/responses"
	"github.com/volcengine/ark-runtime-go/arkruntime/utils"
)

// CreateResponses Creates a model response.
//
// Defensively clears body.Stream so callers that reuse a single
// *ResponsesRequest across CreateResponsesStream and CreateResponses
// don't carry the streaming flag into a non-streaming request — which
// would otherwise make the server emit SSE and break this method's
// JSON decoder.
func (c *Client) CreateResponses(ctx context.Context, body *responses.ResponsesRequest, opts ...requestOption) (res *responses.ResponseWithHeader, err error) {
	body.Stream = responses.NewOptBool(false)
	path := "/responses"
	opts = append(opts, withBody(body))
	// preprocess input multi modal files
	if err := c.preprocessResponseInput(ctx, &body.Input); err != nil {
		return nil, err
	}
	res = &responses.ResponseWithHeader{}
	err = c.Do(ctx, http.MethodPost, c.fullURL(path), resourceTypeEndpoint, body.Model, res, opts...)
	return
}

// CreateResponsesStream Creates a model response.
func (c *Client) CreateResponsesStream(ctx context.Context, body *responses.ResponsesRequest, opts ...requestOption) (stream *utils.ResponsesStreamReader, err error) {
	body.Stream = responses.NewOptBool(true)
	opts = append(opts, withBody(body))
	// preprocess input multi modal files
	if err := c.preprocessResponseInput(ctx, &body.Input); err != nil {
		return nil, err
	}
	path := "/responses"
	return c.ResponsesRequestStreamDo(ctx, http.MethodPost, c.fullURL(path), resourceTypeEndpoint, body.Model, opts...)
}

// GetResponses retrieves a model response with the given ID.
func (c *Client) GetResponses(ctx context.Context, params *responses.ResponsesRetrieveParams, opts ...requestOption) (res *responses.ResponseWithHeader, err error) {
	if params == nil || params.ResponseId == "" {
		err = errors.New("missing required response_id parameter")
		return
	}
	q, qerr := responses.RetrieveURLQuery(params)
	if qerr != nil {
		return nil, qerr
	}
	u := c.fullURL(fmt.Sprintf("/responses/%s", params.ResponseId))
	if encoded := q.Encode(); encoded != "" {
		u = u + "?" + encoded
	}
	res = &responses.ResponseWithHeader{}
	err = c.Do(ctx, http.MethodGet, u, "", "", res, opts...)
	return
}

// DeleteResponse deletes a model response with the given ID.
func (c *Client) DeleteResponse(ctx context.Context, params *responses.ResponsesRemoveParams, opts ...requestOption) (err error) {
	opts = append(opts, WithCustomHeader("Accept", ""))
	if params == nil || params.ResponseId == "" {
		err = errors.New("missing required response_id parameter")
		return
	}
	path := fmt.Sprintf("/responses/%s", params.ResponseId)
	err = c.Do(ctx, http.MethodDelete, c.fullURL(path), "", "", nil, opts...)
	return
}

// ListResponseInputItems returns a list of input items for a given response.
func (c *Client) ListResponseInputItems(ctx context.Context, params *responses.ResponsesListInputItemsParams, opts ...requestOption) (res *responses.ListInputItemsResponseWithHeader, err error) {
	if params == nil || params.ResponseId == "" {
		err = errors.New("missing required response_id parameter")
		return
	}
	q, qerr := responses.ListInputItemsURLQuery(params)
	if qerr != nil {
		return nil, qerr
	}
	u := c.fullURL(fmt.Sprintf("/responses/%s/input_items", params.ResponseId))
	if encoded := q.Encode(); encoded != "" {
		u = u + "?" + encoded
	}
	res = &responses.ListInputItemsResponseWithHeader{}
	err = c.Do(ctx, http.MethodGet, u, "", "", res, opts...)
	return
}
