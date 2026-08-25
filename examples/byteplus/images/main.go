// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/images"
)

/**
 * Authentication
 * If you authorize your endpoint using an API key, set your api key to the
 * environment variable "ARK_API_KEY":
 *   client := arkruntime.NewByteplusClientWithApiKey(os.Getenv("ARK_API_KEY"))
 * Note: API keys do not refresh — pick one with no expiration.
 */
func main() {
	client := arkruntime.NewByteplusClientWithApiKey(os.Getenv("ARK_API_KEY"))
	ctx := context.Background()
	seedreamModel := envOrDefault("SEEDREAM_MODEL", "dola-seedream-5-0-pro-260628")

	fmt.Println("----- [Seedream] generate images (response format: url) -----")
	req := &images.CreateImageGenerationRequest{
		Model:          seedreamModel,
		Prompt:         "龙与地下城女骑士背景是起伏的平原，目光从镜头转向平原",
		ResponseFormat: images.NewOptResponseFormat(images.ResponseFormatURL),
		Seed:           images.NewOptInt64(1234567890),
		Watermark:      images.NewOptBool(true),
		Size:           images.NewOptString("1024x1024"),
	}

	resp, err := client.GenerateImages(ctx, req)
	if err != nil {
		fmt.Printf("generate images error: %v\n", err)
		return
	}
	if resp.Error.IsSet() {
		fmt.Printf("Error Code: %s\n", resp.Error.Value.Code)
		fmt.Printf("Error Message: %s\n", resp.Error.Value.Message)
		return
	}
	fmt.Printf("Model: %s\n", resp.Model)
	if len(resp.Data) > 0 {
		fmt.Printf("Image URL: %s\n", resp.Data[0].URL.Value)
	}
	if resp.Usage.IsSet() {
		fmt.Printf("Generated Images: %d\n", resp.Usage.Value.GeneratedImages)
	}
	fmt.Printf("Created: %d\n", resp.Created)

}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
