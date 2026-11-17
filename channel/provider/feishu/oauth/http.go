// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package oauth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxResponseBytes = 1 << 20

// New 校验配置并冻结应用身份；不会发起网络请求。
func New(config Config) (*Client, error) {
	if strings.TrimSpace(config.AppID) == "" || config.AppSecret == "" || strings.Contains(config.AppID, ":") {
		return nil, failure(ErrorConfiguration, OutcomeRejected, 0)
	}
	accounts, err := baseURL(config.AccountsBaseURL, "https://accounts.feishu.cn")
	if err != nil {
		return nil, err
	}
	open, err := baseURL(config.OpenBaseURL, "https://open.feishu.cn")
	if err != nil {
		return nil, err
	}
	client := http.Client{Timeout: 30 * time.Second}
	if config.HTTPClient != nil {
		client = *config.HTTPClient
		if client.Timeout == 0 {
			client.Timeout = 30 * time.Second
		}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	clock := config.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Client{appID: config.AppID, appSecret: config.AppSecret, accountsBaseURL: accounts, openBaseURL: open, httpClient: &client, clock: clock}, nil
}

func baseURL(value, fallback string) (string, error) {
	if value == "" {
		value = fallback
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || parsed.Scheme != "https" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || strings.Contains(value, "#") || parsed.Opaque != "" {
		return "", failure(ErrorConfiguration, OutcomeRejected, 0)
	}
	return strings.TrimRight(value, "/"), nil
}

func failure(kind ErrorKind, outcome Outcome, status int) *Error {
	return &Error{Kind: kind, Outcome: outcome, HTTPStatus: status}
}

func (c *Client) request(ctx context.Context, method, endpoint string, form url.Values, credential string, basic bool) (map[string]json.RawMessage, int, error) {
	var body io.Reader
	contentType := ""
	if form != nil {
		body = strings.NewReader(form.Encode())
		contentType = "application/x-www-form-urlencoded"
	}
	return c.requestBody(ctx, method, endpoint, body, contentType, credential, basic)
}

func (c *Client) requestBody(ctx context.Context, method, endpoint string, body io.Reader, contentType, credential string, basic bool) (map[string]json.RawMessage, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, &Error{Kind: ErrorCancelled, Outcome: OutcomeUnknown, cause: err}
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, 0, failure(ErrorConfiguration, OutcomeRejected, 0)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if basic {
		request.SetBasicAuth(c.appID, c.appSecret)
	} else if credential != "" {
		request.Header.Set("Authorization", "Bearer "+credential)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, 0, &Error{Kind: ErrorCancelled, Outcome: OutcomeUnknown, cause: ctx.Err()}
		}
		return nil, 0, failure(ErrorNetwork, OutcomeUnknown, 0)
	}
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			return nil, response.StatusCode, &Error{Kind: ErrorCancelled, Outcome: OutcomeUnknown, HTTPStatus: response.StatusCode, cause: ctx.Err()}
		}
		return nil, response.StatusCode, failure(ErrorNetwork, OutcomeUnknown, response.StatusCode)
	}
	if response.StatusCode >= 500 {
		return nil, response.StatusCode, failure(ErrorUpstream, OutcomeUnknown, response.StatusCode)
	}
	if len(raw) > maxResponseBytes || !validJSON(raw) {
		return nil, response.StatusCode, failure(ErrorInvalidResponse, OutcomeUnknown, response.StatusCode)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil || payload == nil {
		return nil, response.StatusCode, failure(ErrorInvalidResponse, OutcomeUnknown, response.StatusCode)
	}
	if raw, present := payload["code"]; present {
		var code int
		if string(raw) == "null" || json.Unmarshal(raw, &code) != nil {
			return nil, response.StatusCode, failure(ErrorInvalidResponse, OutcomeUnknown, response.StatusCode)
		}
	}
	return payload, response.StatusCode, nil
}

func responseError(payload map[string]json.RawMessage, status int) error {
	code := 0
	if raw, ok := payload["code"]; ok {
		if string(raw) == "null" || json.Unmarshal(raw, &code) != nil {
			return failure(ErrorInvalidResponse, OutcomeUnknown, status)
		}
	}
	symbol := stringField(payload, "error")
	if raw, ok := payload["error"]; ok && (symbol == "" || string(raw) == "null") {
		return failure(ErrorInvalidResponse, OutcomeUnknown, status)
	}
	if status >= 200 && status < 300 && code == 0 && symbol == "" {
		return nil
	}
	kind, outcome := ErrorUpstream, OutcomeUnknown
	switch symbol {
	case "invalid_grant":
		kind, outcome = ErrorReauthRequired, OutcomeRejected
	case "invalid_client", "unauthorized_client":
		kind, outcome = ErrorConfiguration, OutcomeRejected
	case "invalid_scope", "access_denied":
		kind, outcome = ErrorPermission, OutcomeRejected
	}
	switch code {
	case 20001, 20002, 20009, 20024, 20036, 20048, 20063, 20067, 20069, 20070, 20074:
		kind, outcome = ErrorConfiguration, OutcomeRejected
	case 20008, 20010, 20066, 20068:
		kind, outcome = ErrorPermission, OutcomeRejected
	case 20026, 20037, 20064, 20073:
		kind, outcome = ErrorReauthRequired, OutcomeRejected
	}
	if status == 429 {
		kind, outcome = ErrorRateLimit, OutcomeRejected
	}
	return &Error{Kind: kind, Outcome: outcome, HTTPStatus: status, Code: code}
}

func stringField(payload map[string]json.RawMessage, key string) string {
	var value string
	_ = json.Unmarshal(payload[key], &value)
	return value
}
func secondsField(payload map[string]json.RawMessage, key string) (time.Duration, bool) {
	var seconds int64
	if json.Unmarshal(payload[key], &seconds) != nil || seconds <= 0 || seconds > int64((1<<63-1)/time.Second) {
		return 0, false
	}
	return time.Duration(seconds) * time.Second, true
}

// String 返回不含应用凭据和地址的安全视图。
func (c Client) String() string { return "FeishuOAuthClient{redacted}" }

// GoString 返回 Go 格式的安全视图。
func (c Client) GoString() string { return c.String() }

// String 返回不含应用凭据和地址的配置视图。
func (c Config) String() string { return "FeishuOAuthConfig{redacted}" }

// GoString 返回 Go 格式的安全配置视图。
func (c Config) GoString() string { return c.String() }

// GoString 返回不含原始响应及凭据的错误视图。
func (e *Error) GoString() string { return e.Error() }

func safeErrorText(e *Error) string {
	// 只格式化受控枚举；未知或由调用方构造的 Kind 不回显。
	kind := "unknown"
	switch e.Kind {
	case ErrorNetwork, ErrorUpstream, ErrorInvalidResponse, ErrorReauthRequired, ErrorConfiguration, ErrorPermission, ErrorRateLimit, ErrorCancelled:
		kind = string(e.Kind)
	}
	return fmt.Sprintf("feishu oauth: %s (http %d)", kind, e.HTTPStatus)
}
