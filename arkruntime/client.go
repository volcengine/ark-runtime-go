// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/volcengine/volcengine-go-sdk/service/ark"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
	"github.com/volcengine/volcengine-go-sdk/volcengine/credentials"
	"github.com/volcengine/volcengine-go-sdk/volcengine/session"
	"github.com/volcengine/volcengine-go-sdk/volcengine/volcengineerr"

	"github.com/volcengine/ark-runtime-go/arkruntime/model"
	"github.com/volcengine/ark-runtime-go/arkruntime/utils"
)

type Client struct {
	config clientConfig

	requestBuilder utils.RequestBuilder

	arkClient               *ark.ARK
	resourceStsTokens       sync.Map
	rwLock                  *sync.RWMutex
	advisoryRefreshTimeout  int
	mandatoryRefreshTimeout int

	batchHTTPClient      *http.Client
	sessionStreamClient  *http.Client
	modelBreakerProvider *utils.ModelBreakerProvider
}

func NewClientWithApiKey(apiKey string, setters ...ConfigOption) *Client {
	config := NewClientConfig(apiKey, "", "", setters...)
	return newClientWithConfig(config)
}

func NewClientWithAkSk(ak, sk string, setters ...ConfigOption) *Client {
	config := NewClientConfig("", ak, sk, setters...)
	return newClientWithConfig(config)
}

// HTTPClient returns the configured HTTP client.
func (c *Client) HTTPClient() *http.Client {
	if c == nil || c.config.HTTPClient == nil {
		return http.DefaultClient
	}
	return c.config.HTTPClient
}

func newSessionStreamClient(client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	streamClient := *client
	streamClient.Timeout = 0
	return &streamClient
}

// NewVolcClient constructs a client targeting the Volcengine cloud
// (ark.cn-beijing.volces.com). Reads ARK_API_KEY for api-key auth and
// VOLC_ACCESSKEY/VOLC_SECRETKEY for AK/SK auth, in that preference order.
// Pass extra ConfigOptions (e.g. WithBaseUrl, WithTimeout) to override.
func NewVolcClient(setters ...ConfigOption) *Client {
	return newClientForCloud(CloudVolc, setters...)
}

// NewVolcClientWithApiKey is a Volc-targeted variant of NewClientWithApiKey.
func NewVolcClientWithApiKey(apiKey string, setters ...ConfigOption) *Client {
	return NewClientWithApiKey(apiKey, append([]ConfigOption{withCloud(CloudVolc)}, setters...)...)
}

// NewVolcClientWithAkSk is a Volc-targeted variant of NewClientWithAkSk.
func NewVolcClientWithAkSk(ak, sk string, setters ...ConfigOption) *Client {
	return NewClientWithAkSk(ak, sk, append([]ConfigOption{withCloud(CloudVolc)}, setters...)...)
}

// NewByteplusClient constructs a client targeting the Byteplus cloud
// (ark.ap-southeast.bytepluses.com). Reads ARK_API_KEY for api-key auth
// and BYTEPLUS_ACCESSKEY/BYTEPLUS_SECRETKEY for AK/SK auth, in that
// preference order.
func NewByteplusClient(setters ...ConfigOption) *Client {
	return newClientForCloud(CloudByteplus, setters...)
}

// NewByteplusClientWithApiKey is a Byteplus-targeted variant of
// NewClientWithApiKey.
func NewByteplusClientWithApiKey(apiKey string, setters ...ConfigOption) *Client {
	return NewClientWithApiKey(apiKey, append([]ConfigOption{withCloud(CloudByteplus)}, setters...)...)
}

// NewByteplusClientWithAkSk is a Byteplus-targeted variant of
// NewClientWithAkSk.
func NewByteplusClientWithAkSk(ak, sk string, setters ...ConfigOption) *Client {
	return NewClientWithAkSk(ak, sk, append([]ConfigOption{withCloud(CloudByteplus)}, setters...)...)
}

// newClientForCloud picks creds from the cloud's env vars: prefer api-key
// if ARK_API_KEY is set, else fall back to the cloud-specific AK/SK pair.
// Multi-cloud users can avoid env-var ambiguity by passing creds explicitly
// to the typed AkSk/ApiKey constructors instead.
func newClientForCloud(cloud Cloud, setters ...ConfigOption) *Client {
	preset := cloudPresets[cloud]
	if apiKey := os.Getenv("ARK_API_KEY"); apiKey != "" {
		return NewClientWithApiKey(apiKey, append([]ConfigOption{withCloud(cloud)}, setters...)...)
	}
	return NewClientWithAkSk(os.Getenv(preset.akEnv), os.Getenv(preset.skEnv), append([]ConfigOption{withCloud(cloud)}, setters...)...)
}

// NewClientWithConfig creates new API client for specified config.
func newClientWithConfig(config clientConfig) *Client {
	var arkClient *ark.ARK
	arkConfig := volcengine.NewConfig().
		WithCredentials(credentials.NewStaticCredentials(config.ak, config.sk, "")).
		WithRegion(config.region)

	sess, _ := session.NewSession(arkConfig)
	arkClient = ark.New(sess)

	return &Client{
		config:                  config,
		requestBuilder:          utils.NewRequestBuilder(),
		arkClient:               arkClient,
		resourceStsTokens:       sync.Map{},
		rwLock:                  &sync.RWMutex{},
		advisoryRefreshTimeout:  model.DefaultAdvisoryRefreshTimeout,
		mandatoryRefreshTimeout: model.DefaultMandatoryRefreshTimeout,
		batchHTTPClient:         newBatchHTTPClient(config.batchMaxParallel),
		sessionStreamClient:     newSessionStreamClient(config.HTTPClient),
		modelBreakerProvider:    utils.NewModelBreakerProvider(),
	}
}

func (c *Client) GetEndpointStsToken(ctx context.Context, endpointId string) (string, error) {
	return c.GetResourceStsToken(ctx, resourceTypeEndpoint, endpointId, "")
}

func (c *Client) GetResourceStsToken(ctx context.Context, resourceType string, resourceId string, projectName string) (string, error) {
	err := c.refresh(ctx, resourceType, resourceId, projectName)
	if err != nil {
		return "", err
	}

	token, ok := c.resourceStsTokens.Load(fmt.Sprintf(stsTokenKeyPattern, resourceType, resourceId))
	if ok {
		return token.(tokenInfo).token, nil
	}
	return "", nil
}

func (c *Client) refresh(ctx context.Context, resourceType string, resourceId string, projectName string) error {
	if !c.needRefresh(resourceType, resourceId, c.advisoryRefreshTimeout) {
		return nil
	}

	if c.rwLock.TryLock() {
		defer c.rwLock.Unlock()
		if !c.needRefresh(resourceType, resourceId, c.advisoryRefreshTimeout) {
			return nil
		}

		isMandatoryRefresh := c.needRefresh(resourceType, resourceId, c.mandatoryRefreshTimeout)
		return c.protectedRefresh(ctx, resourceType, resourceId, projectName, isMandatoryRefresh)
	} else if c.needRefresh(resourceType, resourceId, c.mandatoryRefreshTimeout) {
		c.rwLock.Lock()
		defer c.rwLock.Unlock()
		if !c.needRefresh(resourceType, resourceId, c.mandatoryRefreshTimeout) {
			return nil
		}
		return c.protectedRefresh(ctx, resourceType, resourceId, projectName, true)
	}
	return nil
}

func (c *Client) needRefresh(resourceType string, resourceId string, refreshIn int) bool {
	delta := c.advisoryRefreshTimeout
	if refreshIn > 0 {
		delta = refreshIn
	}

	token, ok := c.resourceStsTokens.Load(fmt.Sprintf(stsTokenKeyPattern, resourceType, resourceId))
	if ok {
		return token.(tokenInfo).expiredTime-time.Now().Unix() < int64(delta)
	}
	return true
}

func (c *Client) protectedRefresh(ctx context.Context, resourceType string, resourceId string, projectName string, isMandatory bool) error {
	input := ark.GetApiKeyInput{
		DurationSeconds: volcengine.Int32(model.DefaultStsTimeout),
		ResourceIds:     []*string{volcengine.String(resourceId)},
		ResourceType:    volcengine.String(resourceType),
	}

	if projectName != "" {
		input.ProjectName = volcengine.String(projectName)
	}

	resp, err := c.arkClient.GetApiKeyWithContext(ctx, &input)
	if err != nil {
		if isMandatory {
			return err
		} else {
			return nil
		}
	}
	c.resourceStsTokens.Store(fmt.Sprintf(stsTokenKeyPattern, resourceType, resourceId), tokenInfo{*resp.ApiKey, int64(*resp.ExpiredTime)})
	return nil
}

type requestOptions struct {
	body      interface{}
	extraBody map[string]interface{}
	header    http.Header
	query     url.Values
}

type requestOption func(*requestOptions)

func withBody(body interface{}) requestOption {
	return func(args *requestOptions) {
		args.body = body
	}
}

func withContentType(contentType string) requestOption {
	return func(args *requestOptions) {
		args.header.Set("Content-Type", contentType)
	}
}

func WithProjectName(project string) requestOption {
	return func(args *requestOptions) {
		args.header.Set("X-Project-Name", project)
	}
}

func WithCustomHeader(key, value string) requestOption {
	return func(args *requestOptions) {
		args.header.Set(key, value)
	}
}

func WithCustomHeaders(m map[string]string) requestOption {
	return func(args *requestOptions) {
		for k, v := range m {
			args.header.Set(k, v)
		}
	}
}

// WithQuery returns a requestOption that sets the query value for the
// given key, overwriting any prior value. If args.query has not been
// initialized yet (zero-value requestOptions), this lazily creates it
// so the option is usable from tests and callers that construct
// requestOptions directly.
func WithQuery(key, value string) requestOption {
	return func(args *requestOptions) {
		if args.query == nil {
			args.query = url.Values{}
		}
		args.query.Set(key, value)
	}
}

func (c *Client) newRequest(ctx context.Context, method, url, _, resourceId string, setters ...requestOption) (*http.Request, *model.RequestError) {
	// Default Options
	args := &requestOptions{
		body:   nil,
		header: make(http.Header),
	}
	args.query = make(map[string][]string)

	requestID := utils.GenRequestId()
	args.header.Set(model.ClientRequestHeader, requestID)

	// parse resource type by resourceId
	// - endpoint: ep-*
	// - presetendpoint: ep-m-* or modelID such as doubao-pro-32k-240525
	resourceType := c.getResourceTypeById(resourceId)

	for _, setter := range setters {
		setter(args)
	}

	mergedBody, err := mergeExtraBody(args.body, args.extraBody)
	if err != nil {
		return nil, model.NewRequestError(http.StatusBadRequest, err, requestID)
	}
	args.body = mergedBody

	errH := c.setCommonHeaders(ctx, args, resourceType, resourceId)
	if errH != nil {
		return nil, errH
	}

	// add query args
	if len(args.query) > 0 {
		url = url + "?" + args.query.Encode()
	}

	req, err := c.requestBuilder.Build(ctx, method, url, args.body, args.header)
	if err != nil {
		return nil, model.NewRequestError(http.StatusBadRequest, err, requestID)
	}

	return req, nil
}

func (c *Client) sendRequest(client *http.Client, req *http.Request, v model.Response) error {
	requestID := req.Header.Get(model.ClientRequestHeader)
	req.Header.Set("Accept", "application/json")

	// Check whether Content-Type is already set, Upload Files API requires
	// Content-Type == multipart/form-data
	contentType := req.Header.Get("Content-Type")
	if contentType == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	res, err := client.Do(req)
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

	err = decodeResponse(res.Body, v)
	if err != nil {
		err = &model.RequestError{
			HTTPStatusCode: res.StatusCode,
			Err:            err,
			RequestId:      requestID,
		}
	}
	return err
}

func (c *Client) Do(ctx context.Context, method, url, resourceType, resourceId string, v model.Response, setters ...requestOption) (err error) {
	err = utils.Retry(
		ctx,
		utils.RetryPolicy{
			MaxAttempts:    c.config.RetryTimes,
			InitialBackoff: model.ErrorRetryBaseDelay,
			MaxBackoff:     model.ErrorRetryMaxDelay,
		},
		func() bool { return true },
		func() error {
			req, innerErr := c.newRequest(ctx, method, url, resourceType, resourceId, setters...)
			if innerErr != nil {
				return innerErr
			}

			return c.sendRequest(c.config.HTTPClient, req, v)
		},
		nil,
		needRetryError,
	)
	return
}

func (c *Client) DoBatch(ctx context.Context, method, url, resourceType, resourceId string, v model.Response, setters ...requestOption) error {
	breaker := c.modelBreakerProvider.GetOrCreateBreaker(resourceId)

	for {
		breaker.Wait()

		select {
		case <-ctx.Done(): // whole context finish
			return ctx.Err()
		default:
		}

		err := func() error {
			req, er := c.newRequest(ctx, method, url, resourceType, resourceId, setters...)
			if er != nil {
				return er
			}

			return c.sendRequest(c.batchHTTPClient, req, v)
		}()

		// no error: just return on this try
		if err == nil {
			return nil
		}

		// no need to retry error
		if !needRetryError(err) {
			return err
		}

		retryAfter := c.getRetryAfter(v)
		if retryAfter > 0 {
			breaker.Reset(time.Duration(retryAfter) * time.Second)
		}

		time.Sleep(time.Duration(500+rand.Intn(1001)) * time.Millisecond)
	}
}

func sendChatGenStream(client *Client, httpClient *http.Client, req *http.Request) (*utils.ChatGenStreamReader, error) {
	requestID := req.Header.Get(model.ClientRequestHeader)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Connection", "keep-alive")

	resp, err := httpClient.Do(req) //nolint:bodyclose // body is closed in stream.Close()
	if err != nil {
		return &utils.ChatGenStreamReader{}, model.NewRequestError(http.StatusInternalServerError, err, requestID)
	}
	if isFailureStatusCode(resp) {
		return &utils.ChatGenStreamReader{}, client.handleErrorResp(resp)
	}
	return &utils.ChatGenStreamReader{
		ChatCompletionStreamReader: utils.ChatCompletionStreamReader{
			EmptyMessagesLimit: client.config.EmptyMessagesLimit,
			Reader:             bufio.NewReader(resp.Body),
			Response:           resp,
			ErrAccumulator:     utils.NewErrorAccumulator(),
			Unmarshaler:        &utils.JSONUnmarshaler{},
			HttpHeader:         model.HttpHeader(resp.Header),
		},
	}, nil
}

// sendImageGenerationStream wires the gen-typed ImageGenerationStreamReader.
// Mirrors sendChatGenStream — same Accept: text/event-stream handshake; the
// reader unmarshals each `data:` frame into images.ImageGenerationStreamEvent.
func sendImageGenerationStream(client *Client, httpClient *http.Client, req *http.Request) (*utils.ImageGenerationStreamReader, error) {
	requestID := req.Header.Get(model.ClientRequestHeader)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Connection", "keep-alive")

	resp, err := httpClient.Do(req) //nolint:bodyclose // body is closed in stream.Close()
	if err != nil {
		return &utils.ImageGenerationStreamReader{}, model.NewRequestError(http.StatusInternalServerError, err, requestID)
	}
	if isFailureStatusCode(resp) {
		return &utils.ImageGenerationStreamReader{}, client.handleErrorResp(resp)
	}
	return &utils.ImageGenerationStreamReader{
		ChatCompletionStreamReader: utils.ChatCompletionStreamReader{
			EmptyMessagesLimit: client.config.EmptyMessagesLimit,
			Reader:             bufio.NewReader(resp.Body),
			Response:           resp,
			ErrAccumulator:     utils.NewErrorAccumulator(),
			Unmarshaler:        &utils.JSONUnmarshaler{},
			HttpHeader:         model.HttpHeader(resp.Header),
		},
	}, nil
}

func sendCreateResponsesRequestStream(client *Client, httpClient *http.Client, req *http.Request) (*utils.ResponsesStreamReader, error) {
	requestID := req.Header.Get(model.ClientRequestHeader)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Connection", "keep-alive")

	resp, err := httpClient.Do(req) //nolint:bodyclose // body is closed in stream.Close()
	if err != nil {
		return &utils.ResponsesStreamReader{}, model.NewRequestError(http.StatusInternalServerError, err, requestID)
	}
	if isFailureStatusCode(resp) {
		return &utils.ResponsesStreamReader{}, client.handleErrorResp(resp)
	}
	return &utils.ResponsesStreamReader{
		ChatCompletionStreamReader: utils.ChatCompletionStreamReader{
			EmptyMessagesLimit: client.config.EmptyMessagesLimit,
			Response:           resp,
			ErrAccumulator:     utils.NewErrorAccumulator(),
			Unmarshaler:        &utils.JSONUnmarshaler{},
			HttpHeader:         model.HttpHeader(resp.Header),
		},
		Decoder: utils.NewEventStreamDecoder(resp.Body),
	}, nil
}

// ImageGenerationStreamRequestDo executes an /images/generations request
// with stream=true and returns a gen-typed reader.
func (c *Client) ImageGenerationStreamRequestDo(ctx context.Context, method, url, resourceId string, setters ...requestOption) (streamReader *utils.ImageGenerationStreamReader, err error) {
	err = utils.Retry(
		ctx,
		utils.RetryPolicy{
			MaxAttempts:    c.config.RetryTimes,
			InitialBackoff: model.ErrorRetryBaseDelay,
			MaxBackoff:     model.ErrorRetryMaxDelay,
		},
		func() bool { return true },
		func() error {
			req, innerErr := c.newRequest(ctx, method, url, resourceTypeEndpoint, resourceId, setters...)
			if innerErr != nil {
				return innerErr
			}

			streamReader, err = sendImageGenerationStream(c, c.config.HTTPClient, req)
			return err
		},
		nil,
		needRetryError,
	)
	return
}

// ChatGenStreamRequestDo executes a chat-completions stream request and
// returns a gen-typed reader.
func (c *Client) ChatGenStreamRequestDo(ctx context.Context, method, url, resourceId string, setters ...requestOption) (streamReader *utils.ChatGenStreamReader, err error) {
	err = utils.Retry(
		ctx,
		utils.RetryPolicy{
			MaxAttempts:    c.config.RetryTimes,
			InitialBackoff: model.ErrorRetryBaseDelay,
			MaxBackoff:     model.ErrorRetryMaxDelay,
		},
		func() bool { return true },
		func() error {
			req, innerErr := c.newRequest(ctx, method, url, resourceTypeEndpoint, resourceId, setters...)
			if innerErr != nil {
				return innerErr
			}

			streamReader, err = sendChatGenStream(c, c.config.HTTPClient, req)
			return err
		},
		nil,
		needRetryError,
	)

	return
}

// ResponsesRequestStreamDo executes a request.
func (c *Client) ResponsesRequestStreamDo(ctx context.Context, method, url, resourceType, resourceId string, setters ...requestOption) (resp *utils.ResponsesStreamReader, err error) {
	err = utils.Retry(
		ctx,
		utils.RetryPolicy{
			MaxAttempts:    c.config.RetryTimes,
			InitialBackoff: model.ErrorRetryBaseDelay,
			MaxBackoff:     model.ErrorRetryMaxDelay,
		},
		func() bool { return true },
		func() error {
			req, innerErr := c.newRequest(ctx, method, url, resourceType, resourceId, setters...)
			if innerErr != nil {
				return innerErr
			}
			resp, err = sendCreateResponsesRequestStream(c, c.config.HTTPClient, req)
			return err
		},
		nil,
		needRetryError,
	)
	return
}

func (c *Client) setCommonHeaders(ctx context.Context, args *requestOptions, resourceType string, resourceId string) *model.RequestError {
	requestID := args.header.Get(model.ClientRequestHeader)
	if len(c.config.apiKey) > 0 {
		args.header.Set("Authorization", fmt.Sprintf("Bearer %s", c.config.apiKey))
	} else {
		if resourceTypeEndpoint == resourceType && !strings.HasPrefix(resourceId, "ep-") {
			return model.NewRequestError(http.StatusBadRequest, model.ErrBodyWithoutEndpoint, requestID)
		}

		projectName := args.header.Get("X-Project-Name")
		if resourceTypePresetEndpoint == resourceType && projectName == "" {
			return model.NewRequestError(http.StatusBadRequest, model.ErrBodyWithoutProjectName, requestID)
		}

		token, err := c.GetResourceStsToken(ctx, resourceType, resourceId, projectName)
		if err != nil {
			if volcErr, ok := err.(volcengineerr.RequestFailure); ok {
				return model.NewRequestError(volcErr.StatusCode(), fmt.Errorf("failed to get resource sts token. err=%w", volcErr), volcErr.RequestID())
			}
			return model.NewRequestError(http.StatusInternalServerError, fmt.Errorf("failed to get resource sts token. err=%w", err), requestID)
		}
		args.header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	}
	return nil
}

func (c *Client) getResourceTypeById(resourceId string) string {
	switch {
	case strings.HasPrefix(resourceId, "ep-m-"):
		return resourceTypePresetEndpoint
	case strings.HasPrefix(resourceId, "ep-"):
		return resourceTypeEndpoint
	default:
		return resourceTypePresetEndpoint
	}
}

func isFailureStatusCode(resp *http.Response) bool {
	return resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusBadRequest
}

func needRetryError(err error) bool {
	apiErr := &model.APIError{}
	reqErr := &model.RequestError{}
	if errors.As(err, &apiErr) {
		return apiErr.HTTPStatusCode >= http.StatusInternalServerError || apiErr.HTTPStatusCode == http.StatusTooManyRequests
	} else if errors.Is(err, io.EOF) {
		return true
	} else if errors.As(err, &reqErr) {
		return reqErr.HTTPStatusCode >= http.StatusInternalServerError
	}
	return false
}

func decodeResponse(body io.Reader, v interface{}) error {
	if v == nil {
		return nil
	}

	switch o := v.(type) {
	case *string:
		return decodeString(body, o)
	default:
		return json.NewDecoder(body).Decode(v)
	}
}

func decodeString(body io.Reader, output *string) error {
	b, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	*output = string(b)
	return nil
}

func (c *Client) fullURL(suffix string) string {
	return fmt.Sprintf("%s%s", c.config.BaseURL, suffix)
}

func (c *Client) handleErrorResp(resp *http.Response) error {
	requestID := responseRequestID(resp)
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return model.NewRequestError(
			resp.StatusCode,
			fmt.Errorf("read error response body: %w", readErr),
			requestID,
		)
	}

	var errRes model.ErrorResponse
	if err := json.Unmarshal(body, &errRes); err == nil && errRes.Error != nil {
		return setAPIErrorResponseMetadata(errRes.Error, resp.StatusCode, requestID)
	}

	// Some services return the error object directly instead of wrapping it in
	// an {"error": ...} envelope. Preserve its structured fields when possible.
	var apiErr model.APIError
	if err := json.Unmarshal(body, &apiErr); err == nil &&
		(apiErr.Message != "" || apiErr.Code != "" || apiErr.Type != "") {
		return setAPIErrorResponseMetadata(&apiErr, resp.StatusCode, requestID)
	}

	bodyText := strings.TrimSpace(string(body))
	if bodyText == "" {
		return model.NewRequestError(
			resp.StatusCode,
			errors.New("unexpected error response: empty body"),
			requestID,
		)
	}
	return model.NewRequestError(
		resp.StatusCode,
		fmt.Errorf("unexpected error response body: %s", bodyText),
		requestID,
	)
}

func responseRequestID(resp *http.Response) string {
	if requestID := resp.Header.Get(model.ServerRequestHeader); requestID != "" {
		return requestID
	}
	if requestID := resp.Header.Get(model.ClientRequestHeader); requestID != "" {
		return requestID
	}
	if resp.Request != nil {
		return resp.Request.Header.Get(model.ClientRequestHeader)
	}
	return ""
}

func setAPIErrorResponseMetadata(apiErr *model.APIError, statusCode int, requestID string) error {
	apiErr.HTTPStatusCode = statusCode
	if requestID != "" {
		apiErr.RequestId = requestID
	}
	return apiErr
}

func (c *Client) getRetryAfter(v model.Response) int64 {
	header := v.GetHeader()
	retryAfter := header[model.RetryAfterHeader]
	if len(retryAfter) == 0 || retryAfter[0] == "" {
		return 0
	}
	retryAfterInterval, err := strconv.ParseInt(retryAfter[0], 10, 64)
	if err != nil {
		return 0
	}
	return retryAfterInterval
}

func (c *Client) isAPIKeyAuthentication() bool { //nolint:unused // reserved for future auth branching
	return c.config.apiKey != ""
}
