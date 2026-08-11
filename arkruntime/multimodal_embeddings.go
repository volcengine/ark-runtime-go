// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"net/http"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/multimodalembedding"
)

// CreateMultiModalEmbeddings returns a multimodal embedding response for the
// supplied input items.
func (c *Client) CreateMultiModalEmbeddings(
	ctx context.Context,
	request *multimodalembedding.MultiModalEmbeddingRequest,
	setters ...requestOption,
) (res *multimodalembedding.MultiModalEmbeddingResponseWithHeader, err error) {
	path := "/embeddings/multimodal"
	setters = append(setters, withBody(request))
	res = &multimodalembedding.MultiModalEmbeddingResponseWithHeader{}
	err = c.Do(ctx, http.MethodPost, c.fullURL(path), resourceTypeEndpoint, request.Model, res, setters...)
	return
}
