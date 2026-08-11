// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"net/http"

	"github.com/volcengine/ark-runtime-go/arkruntime/model"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/chat"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/embedding"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/multimodalembedding"
)

const (
	batchChatCompletionsSuffix      = "/batch/chat/completions"
	batchEmbeddingsSuffix           = "/batch/embeddings"
	batchMultiModalEmbeddingsSuffix = "/batch/embeddings/multimodal"
)

func newBatchHTTPClient(maxParallel int) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxConnsPerHost: maxParallel,
		},
	}
}

// CreateBatchChatCompletion performs a parallel synchronous chat completion
// against the batch endpoint. Request/response shapes match
// CreateChatCompletion; only the URL prefix and the client's connection pool
// differ. Streaming is not supported — a streaming request returns
// model.ErrChatCompletionStreamNotSupported.
func (c *Client) CreateBatchChatCompletion(
	ctx context.Context,
	request *chat.ChatCompletionRequest,
	setters ...requestOption,
) (res *chat.ChatCompletionResponseWithHeader, err error) {
	if v, ok := request.Stream.Get(); ok && v {
		err = model.ErrChatCompletionStreamNotSupported
		return
	}
	setters = append(setters, withBody(request))
	res = &chat.ChatCompletionResponseWithHeader{}
	err = c.DoBatch(ctx, http.MethodPost, c.fullURL(batchChatCompletionsSuffix), resourceTypeEndpoint, request.Model, res, setters...)
	return
}

// CreateBatchEmbeddings performs a parallel synchronous embeddings call
// against the batch endpoint. Mirrors CreateEmbeddings.
func (c *Client) CreateBatchEmbeddings(
	ctx context.Context,
	request *embedding.EmbeddingRequest,
	setters ...requestOption,
) (res *embedding.EmbeddingResponseWithHeader, err error) {
	setters = append(setters, withBody(request))
	res = &embedding.EmbeddingResponseWithHeader{}
	err = c.DoBatch(ctx, http.MethodPost, c.fullURL(batchEmbeddingsSuffix), resourceTypeEndpoint, request.Model, res, setters...)
	return
}

// CreateBatchMultiModalEmbeddings performs a parallel synchronous multimodal
// embeddings call against the batch endpoint. Mirrors
// CreateMultiModalEmbeddings.
func (c *Client) CreateBatchMultiModalEmbeddings(
	ctx context.Context,
	request *multimodalembedding.MultiModalEmbeddingRequest,
	setters ...requestOption,
) (res *multimodalembedding.MultiModalEmbeddingResponseWithHeader, err error) {
	setters = append(setters, withBody(request))
	res = &multimodalembedding.MultiModalEmbeddingResponseWithHeader{}
	err = c.DoBatch(ctx, http.MethodPost, c.fullURL(batchMultiModalEmbeddingsSuffix), resourceTypeEndpoint, request.Model, res, setters...)
	return
}

// Deprecated: use `arkruntime.WithBatchMaxParallel` option in `arkruntime.NewClientXXX` instead.
func (c *Client) StartBatchWorker(workerNum int) {
	if transport, ok := c.batchHTTPClient.Transport.(*http.Transport); ok {
		transport.MaxConnsPerHost = workerNum
	}
}

// Deprecated: no need to stop batch worker.
func (c *Client) StopBatchWorker() {
}
