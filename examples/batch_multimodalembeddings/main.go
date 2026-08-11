// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/multimodalembedding"
)

/**
 * Authentication
 * If you authorize your endpoint using an API key, you can set your api key to environment variable "ARK_API_KEY"
 * client := arkruntime.NewClientWithApiKey(os.Getenv("ARK_API_KEY"))
 *
 * Batch multimodal embeddings use the same request/response shapes as
 * /embeddings/multimodal, but are dispatched against
 * /batch/embeddings/multimodal with a high-concurrency HTTP client and a
 * per-model breaker that honours Retry-After headers.
 */

func main() {
	client := arkruntime.NewClientWithApiKey(
		os.Getenv("ARK_API_KEY"),
		arkruntime.WithBatchMaxParallel(3000),
	)

	const total = 20
	requests := mockRequests("doubao-embedding-vision-250615", total)

	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()

	var wg sync.WaitGroup
	for req := range requests {
		wg.Add(1)
		go func(req *multimodalembedding.MultiModalEmbeddingRequest) {
			defer wg.Done()

			reqCtx, reqCancel := context.WithTimeout(ctx, 10*time.Minute)
			defer reqCancel()

			resp, err := client.CreateBatchMultiModalEmbeddings(reqCtx, req)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return
			}
			fmt.Println(mustMarshalJSON(resp))
		}(req)
	}
	wg.Wait()
}

func mockRequests(model string, count int) <-chan *multimodalembedding.MultiModalEmbeddingRequest {
	out := make(chan *multimodalembedding.MultiModalEmbeddingRequest)
	go func() {
		defer close(out)
		for i := 0; i < count; i++ {
			out <- &multimodalembedding.MultiModalEmbeddingRequest{
				Model: model,
				Input: []multimodalembedding.EmbeddingInput{
					{
						Type: multimodalembedding.EmbeddingInputTypeImageURL,
						ImageURL: multimodalembedding.NewOptImageURL(multimodalembedding.ImageURL{
							URL: "https://ark-project.tos-cn-beijing.volces.com/images/view.jpeg",
						}),
					},
				},
			}
		}
	}()
	return out
}

func mustMarshalJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
