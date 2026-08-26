// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/volcengine/ark-runtime-go/arkruntime/model"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/environment"
	"github.com/volcengine/ark-runtime-go/arkruntime/utils"
)

const (
	environmentWorkWorkerIDHeader = "Ark-Worker-ID"
)

// PollWork polls one work item from an Environment work queue.
func (c *Client) PollWork(
	ctx context.Context,
	body *environment.PollWorkRequest,
	setters ...requestOption,
) (*environment.WorkItem, error) {
	if body == nil {
		return nil, errors.New("missing required request body")
	}
	if body.EnvironmentID == "" {
		return nil, errors.New("missing required environment_id")
	}
	q := url.Values{}
	if body.WorkerID != "" {
		setters = append(setters, WithCustomHeader(environmentWorkWorkerIDHeader, body.WorkerID))
	}
	if body.BlockMS > 0 {
		q.Set("block_ms", strconv.Itoa(body.BlockMS))
	}
	if body.ReclaimOlderThanMS > 0 {
		q.Set("reclaim_older_than_ms", strconv.Itoa(body.ReclaimOlderThanMS))
	}
	u := c.fullURL(fmt.Sprintf("%s/%s/work/poll", environmentsPrefix, environment.PathEscape(body.EnvironmentID)))
	if encoded := q.Encode(); encoded != "" {
		u += "?" + encoded
	}

	opts := append(setters, withBody(nil))
	wrap := &environment.WorkItemResponse{}
	if err := c.doControlPlaneRequest(ctx, http.MethodGet, u, wrap, opts...); err != nil {
		return nil, err
	}
	if wrap.ID == "" {
		return nil, nil
	}
	return &wrap.WorkItem, nil
}

// AckWork acknowledges one claimed work item.
func (c *Client) AckWork(
	ctx context.Context,
	body *environment.AckWorkRequest,
	setters ...requestOption,
) error {
	if body == nil {
		return errors.New("missing required request body")
	}
	if body.EnvironmentID == "" {
		return errors.New("missing required environment_id")
	}
	if body.WorkID == "" {
		return errors.New("missing required work_id")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s/work/%s/ack",
		environmentsPrefix,
		environment.PathEscape(body.EnvironmentID),
		environment.PathEscape(body.WorkID),
	))
	if workerID, ok := body.WorkerID.Get(); ok {
		setters = append(setters, WithCustomHeader(environmentWorkWorkerIDHeader, workerID))
	}
	wrap := &environment.WorkItemResponse{}
	return c.doControlPlaneRequest(ctx, http.MethodPost, u, wrap, append(setters, withBody(nil))...)
}

// HeartbeatWork refreshes a claimed work lease.
func (c *Client) HeartbeatWork(
	ctx context.Context,
	body *environment.HeartbeatWorkRequest,
	setters ...requestOption,
) (*environment.HeartbeatWorkResponse, error) {
	if body == nil {
		return nil, errors.New("missing required request body")
	}
	if body.EnvironmentID == "" {
		return nil, errors.New("missing required environment_id")
	}
	if body.WorkID == "" {
		return nil, errors.New("missing required work_id")
	}
	q := url.Values{}
	if desiredTTLSeconds, ok := body.DesiredTTLSeconds.Get(); ok && desiredTTLSeconds > 0 {
		q.Set("desired_ttl_seconds", strconv.FormatInt(desiredTTLSeconds, 10))
	}
	if expectedLastHeartbeat, ok := body.ExpectedLastHeartbeat.Get(); ok && expectedLastHeartbeat != "" {
		q.Set("expected_last_heartbeat", expectedLastHeartbeat)
	}
	u := c.fullURL(fmt.Sprintf("%s/%s/work/%s/heartbeat",
		environmentsPrefix,
		environment.PathEscape(body.EnvironmentID),
		environment.PathEscape(body.WorkID),
	))
	if encoded := q.Encode(); encoded != "" {
		u += "?" + encoded
	}
	wrap := &environment.HeartbeatWorkResponseWrapper{}
	if err := c.doControlPlaneRequest(ctx, http.MethodPost, u, wrap, append(setters, withBody(nil))...); err != nil {
		return nil, err
	}
	return &wrap.HeartbeatWorkResponse, nil
}

// StopWork releases or stops one claimed work item.
func (c *Client) StopWork(
	ctx context.Context,
	body *environment.StopWorkRequest,
	setters ...requestOption,
) error {
	if body == nil {
		return errors.New("missing required request body")
	}
	if body.EnvironmentID == "" {
		return errors.New("missing required environment_id")
	}
	if body.WorkID == "" {
		return errors.New("missing required work_id")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s/work/%s/stop",
		environmentsPrefix,
		environment.PathEscape(body.EnvironmentID),
		environment.PathEscape(body.WorkID),
	))
	wrap := &environment.WorkItemResponse{}
	return c.doControlPlaneRequest(ctx, http.MethodPost, u, wrap, append(setters, withBody(stopWorkBody(body)))...)
}

type stopWorkRequestBody struct {
	Force *bool `json:"force,omitempty"`
}

func stopWorkBody(body *environment.StopWorkRequest) any {
	force, ok := body.Force.Get()
	if !ok {
		return nil
	}
	return stopWorkRequestBody{Force: &force}
}

func (c *Client) doControlPlaneRequest(
	ctx context.Context,
	method string,
	u string,
	v model.Response,
	setters ...requestOption,
) error {
	return utils.Retry(
		ctx,
		utils.RetryPolicy{
			MaxAttempts:    c.config.RetryTimes,
			InitialBackoff: model.ErrorRetryBaseDelay,
			MaxBackoff:     model.ErrorRetryMaxDelay,
		},
		func() bool { return true },
		func() error {
			req, reqErr := c.newRequest(ctx, method, u, "", "", setters...)
			if reqErr != nil {
				return reqErr
			}
			return c.sendControlPlaneRequest(req, v)
		},
		nil,
		needRetryError,
	)
}

func (c *Client) sendControlPlaneRequest(req *http.Request, v model.Response) error {
	requestID := req.Header.Get(model.ClientRequestHeader)
	req.Header.Set("Accept", "application/json")
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	res, err := c.config.HTTPClient.Do(req)
	if err != nil {
		return model.NewRequestError(http.StatusInternalServerError, err, requestID)
	}
	defer res.Body.Close() //nolint:errcheck // response body close errors are non-actionable

	if v != nil {
		v.SetHeader(res.Header)
	}
	if isFailureStatusCode(res) {
		return c.handleErrorResp(res)
	}
	if err := decodeResponse(res.Body, v); err != nil && !errors.Is(err, io.EOF) {
		return model.NewRequestError(res.StatusCode, err, requestID)
	}
	return nil
}
