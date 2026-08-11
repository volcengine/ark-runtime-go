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
 *   client := arkruntime.NewClientWithApiKey(os.Getenv("ARK_API_KEY"))
 * Note: API keys do not refresh — pick one with no expiration.
 */
func main() {
	client := arkruntime.NewClientWithApiKey(os.Getenv("ARK_API_KEY"))
	ctx := context.Background()
	modelEp := os.Getenv("ENDPOINT_ID")

	fmt.Println("----- [Seedream] generate images (response format: url) -----")
	req := &images.CreateImageGenerationRequest{
		Model:          modelEp,
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

	fmt.Println("----- [Seededit] generate images (with input image) -----")
	editReq := &images.CreateImageGenerationRequest{
		Model:          modelEp,
		Prompt:         "把背景换成黄昏的沙漠",
		Image:          []string{"YOUR_IMAGE_URL_HERE"},
		ResponseFormat: images.NewOptResponseFormat(images.ResponseFormatURL),
		Seed:           images.NewOptInt64(1234567890),
		Watermark:      images.NewOptBool(true),
		Size:           images.NewOptString("adaptive"),
	}
	if resp, err = client.GenerateImages(ctx, editReq); err != nil {
		fmt.Printf("generate images (edit) error: %v\n", err)
		return
	}
	if resp.Error.IsSet() {
		fmt.Printf("Error: %s — %s\n", resp.Error.Value.Code, resp.Error.Value.Message)
		return
	}
	if len(resp.Data) > 0 {
		fmt.Printf("Edited Image URL: %s\n", resp.Data[0].URL.Value)
	}

	fmt.Println("----- [Seedream] sequential image generation -----")
	seqReq := &images.CreateImageGenerationRequest{
		Model:                       modelEp,
		Prompt:                      "星球大战, 场面壮观, 需要描述3个连续场面",
		ResponseFormat:              images.NewOptResponseFormat(images.ResponseFormatURL),
		Seed:                        images.NewOptInt64(1234567890),
		Watermark:                   images.NewOptBool(true),
		Size:                        images.NewOptString("1024x1024"),
		SequentialImageGeneration:   images.NewOptSequentialImageGenerationMode(images.SequentialImageGenerationModeAuto),
		SequentialImageGenerationOptions: images.NewOptSequentialImageGenerationOptions(
			images.SequentialImageGenerationOptions{
				MaxImages: images.NewOptInt32(3),
			},
		),
	}
	if resp, err = client.GenerateImages(ctx, seqReq); err != nil {
		fmt.Printf("generate images (sequential) error: %v\n", err)
		return
	}
	for i, item := range resp.Data {
		fmt.Printf("[%d] size=%s url=%s\n", i, item.Size.Value, item.URL.Value)
	}
}
