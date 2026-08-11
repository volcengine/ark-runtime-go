// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"net/http"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/embedding"
)

// CreateEmbeddings returns an EmbeddingResponse with one Embedding per
// element of request.Input.
func (c *Client) CreateEmbeddings(
	ctx context.Context,
	request *embedding.EmbeddingRequest,
	setters ...requestOption,
) (res *embedding.EmbeddingResponseWithHeader, err error) {
	path := "/embeddings"
	setters = append(setters, withBody(request))
	res = &embedding.EmbeddingResponseWithHeader{}
	err = c.Do(ctx, http.MethodPost, c.fullURL(path), resourceTypeEndpoint, request.Model, res, setters...)
	return
}
