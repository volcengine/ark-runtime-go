// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/embedding"
)

/**
 * Authentication
 * If you authorize your endpoint using an API key, you can set your api key to environment variable "ARK_API_KEY"
 * client := arkruntime.NewClientWithApiKey(os.Getenv("ARK_API_KEY"))
 */

func main() {
	client := arkruntime.NewClientWithApiKey(
		os.Getenv("ARK_API_KEY"),
	)
	ctx := context.Background()

	fmt.Println("----- embeddings request -----")
	req := &embedding.EmbeddingRequest{
		Model: "${YOUR_ENDPOINT_ID}",
		Input: embedding.NewStringArrayEmbeddingInput([]string{
			"花椰菜又称菜花、花菜，是一种常见的蔬菜。",
		}),
	}

	resp, err := client.CreateEmbeddings(ctx, req)
	if err != nil {
		fmt.Printf("embeddings error: %v\n", err)
		return
	}

	s, _ := json.Marshal(resp)
	fmt.Println(string(s))
}
