// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/multimodalembedding"
)

/**
 * Authentication
 * If you authorize your endpoint using an API key, you can set your api key to environment variable "ARK_API_KEY"
 * client := arkruntime.NewVolcClientWithApiKey(os.Getenv("ARK_API_KEY"))
 */

func main() {
	client := arkruntime.NewVolcClientWithApiKey(
		os.Getenv("ARK_API_KEY"),
	)
	ctx := context.Background()

	fmt.Println("----- sparse embeddings request -----")
	req := &multimodalembedding.MultiModalEmbeddingRequest{
		Model: "doubao-embedding-vision-251215",
		Input: []multimodalembedding.EmbeddingInput{
			{
				Type: multimodalembedding.EmbeddingInputTypeText,
				Text: multimodalembedding.NewOptString("花椰菜又称菜花、花菜，是一种常见的蔬菜。"),
			},
		},
		SparseEmbedding: multimodalembedding.NewOptSparseEmbeddingConfig(multimodalembedding.SparseEmbeddingConfig{
			Type: multimodalembedding.NewOptSparseEmbeddingMode(multimodalembedding.SparseEmbeddingModeEnabled),
		}),
	}

	resp, err := client.CreateMultiModalEmbeddings(ctx, req)
	if err != nil {
		fmt.Printf("sparse embeddings error: %v\n", err)
		return
	}

	s, _ := json.Marshal(resp.Data)
	fmt.Println(string(s))
}
