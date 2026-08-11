// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"net/http"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/tokenization"
)

// CreateTokenization tokenizes one or more text inputs and returns the token
// IDs and offsets produced by the model's tokenizer. Request and response
// types live in the tokenization package (regenerated from
// openapi/tokenization/openapi.yaml).
func (c *Client) CreateTokenization(
	ctx context.Context,
	request *tokenization.TokenizationRequest,
	setters ...requestOption,
) (res *tokenization.TokenizationResponseWithHeader, err error) {
	path := "/tokenization"
	setters = append(setters, withBody(request))
	res = &tokenization.TokenizationResponseWithHeader{}
	err = c.Do(ctx, http.MethodPost, c.fullURL(path), resourceTypeEndpoint, request.Model, res, setters...)
	return
}
