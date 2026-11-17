// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	sdkmodel "github.com/volcengine/ark-runtime-go/arkruntime/model"
	"github.com/volcengine/ark-runtime-go/arkruntime/utils"
)

type sdkCallOptions struct {
	Timeout        time.Duration
	IdempotencyKey string
	Headers        http.Header
}

type sdkOperation[T any] func(context.Context, *arkruntime.Client, []arkruntime.RequestOption) (T, error)

// requestSDK keeps request deadlines and domain error mapping in the adapter;
// request construction, response decoding and HTTP execution belong to the SDK.
func requestSDK[T any](c *Client, ctx context.Context, method, path string, options sdkCallOptions, operation sdkOperation[T]) (T, error) {
	result, cancel, err := callSDK(c, ctx, method, path, options, operation)
	if cancel != nil {
		defer cancel()
	}
	return result, err
}

func callSDK[T any](c *Client, ctx context.Context, method, path string, options sdkCallOptions, operation sdkOperation[T]) (T, context.CancelFunc, error) {
	var zero T
	timeout := options.Timeout
	if timeout == 0 {
		timeout = c.requestTimeout
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	if err := requestCtx.Err(); err != nil {
		cancel()
		return zero, nil, &requestNotSentError{cause: err}
	}
	scope := requestScope(path)
	if c.sessionScope != nil {
		scope = *c.sessionScope
	}
	req := (&http.Request{Method: method, Header: make(http.Header)}).WithContext(requestCtx)
	req.Header.Set("X-Client-Request-Id", utils.GenRequestId())
	if options.IdempotencyKey != "" {
		req.Header.Set("Idempotency-Key", options.IdempotencyKey)
	}
	for name, values := range options.Headers {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
	if err := c.applyModelHeaders(req, path, scope); err != nil {
		cancel()
		return zero, nil, &requestNotSentError{cause: err}
	}
	var setters []arkruntime.RequestOption
	for name := range req.Header {
		setters = append(setters, arkruntime.WithCustomHeader(name, req.Header.Get(name)))
	}
	result, err := operation(requestCtx, c.sdk, setters)
	if err == nil {
		return result, cancel, nil
	}
	status, code, requestID := sdkErrorStatus(err)
	mapped := safeSDKError(err, status, code, requestID, requestCtx.Err())
	cancel()
	return zero, nil, mapped
}

func sdkErrorStatus(err error) (int, string, string) {
	var api *sdkmodel.APIError
	if errors.As(err, &api) {
		return api.HTTPStatusCode, api.Code, api.RequestId
	}
	var request *sdkmodel.RequestError
	// RequestError may carry an SDK-synthesized 500 for a transport failure.
	// Preserve the status; Runtime keeps 5xx and timeouts uncertain.
	if errors.As(err, &request) {
		return request.HTTPStatusCode, "", request.RequestId
	}
	return 0, "", ""
}

func safeSDKError(err error, status int, code, requestID string, contextErr error) error {
	var mapped error
	if status >= 300 {
		mapped = &APIError{Status: status, Code: code, RequestID: requestID}
	}
	if contextErr != nil {
		return errors.Join(mapped, contextErr)
	}
	if mapped != nil {
		return mapped
	}
	var unsent *requestNotSentError
	if errors.As(err, &unsent) {
		return unsent
	}
	return errors.New("ma request failed")
}

// sdkBody validates an adapter-owned request projection using the SDK schema.
func sdkBody[T any](value any) (*T, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, &requestNotSentError{cause: fmt.Errorf("encode ma request: %w", err)}
	}
	var body T
	if err := json.Unmarshal(data, &body); err != nil {
		return nil, &requestNotSentError{cause: errors.New("invalid ma SDK request")}
	}
	return &body, nil
}

// sdkProjection maps an SDK resource into the adapter's domain projection.
func sdkProjection(value any, result any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return errors.New("invalid ma SDK response")
	}
	if err := json.Unmarshal(data, result); err != nil {
		return errors.New("invalid ma SDK response")
	}
	return nil
}

// requestNotSentError preserves a sanitized cause for failures before transport.
type requestNotSentError struct{ cause error }

func (e *requestNotSentError) Error() string        { return e.cause.Error() }
func (e *requestNotSentError) Unwrap() error        { return e.cause }
func (e *requestNotSentError) RequestNotSent() bool { return true }
func (e *requestNotSentError) GoString() string     { return e.Error() }
