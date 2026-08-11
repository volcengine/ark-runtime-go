// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/contentgeneration"
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
	// imageURL := os.Getenv("IMAGE_URL")

	fmt.Println("----- create content generation task -----")
	createReq := &contentgeneration.CreateContentGenerationTaskRequest{
		Model: modelEp,
		Content: []contentgeneration.ContentItem{
			{
				Type: contentgeneration.ContentTypeText,
				Text: contentgeneration.NewOptString("龙与地下城女骑士背景是起伏的平原，目光从镜头转向平原"),
			},
			/*
				{
					Type: contentgeneration.ContentTypeImageURL,
					ImageURL: contentgeneration.NewOptImageURL(contentgeneration.ImageURL{
						URL: imageURL,
					}),
					// Role: contentgeneration.NewOptString("first_frame"),
				},
			*/
		},
		// ServiceTier is not accepted by every model (e.g. seedance 2.0
		// in t2v rejects any explicit tier). Leave it unset by default;
		// the "flex" example below shows how to opt-in when a model
		// supports it.
		ExecutionExpiresAfter: contentgeneration.NewOptInt64(3600),
		// CallbackUrl: contentgeneration.NewOptString("CALLBACK_URL"),
	}

	createResp, err := client.CreateContentGenerationTask(ctx, createReq)
	if err != nil {
		fmt.Printf("create content generation error: %v\n", err)
		return
	}
	fmt.Printf("Task Created with ID: %s\n", createResp.ID)

	time.Sleep(1 * time.Minute)
	fmt.Println("----- get content generation task -----")
	taskID := createResp.ID

	getResp, err := client.GetContentGenerationTask(ctx, taskID)
	if err != nil {
		fmt.Printf("get content generation task error: %v\n", err)
		return
	}

	fmt.Printf("Task ID: %s\n", getResp.ID)
	fmt.Printf("Model: %s\n", getResp.Model)
	fmt.Printf("Status: %s\n", getResp.Status)
	if c, ok := getResp.Content.Get(); ok {
		if v, ok := c.VideoURL.Get(); ok {
			fmt.Printf("Video URL: %s\n", v)
		}
	}
	if u, ok := getResp.Usage.Get(); ok {
		fmt.Printf("Completion Tokens: %d\n", u.CompletionTokens)
	}
	if v, ok := getResp.CreatedAt.Get(); ok {
		fmt.Printf("Created At: %d\n", v)
	}
	if v, ok := getResp.UpdatedAt.Get(); ok {
		fmt.Printf("Updated At: %d\n", v)
	}
	if v, ok := getResp.Seed.Get(); ok {
		fmt.Printf("Seed: %d\n", v)
	}
	if v, ok := getResp.RevisedPrompt.Get(); ok {
		fmt.Printf("RevisedPrompt: %s\n", v)
	}
	if v, ok := getResp.ServiceTier.Get(); ok {
		fmt.Printf("ServiceTier: %s\n", v)
	}
	if v, ok := getResp.ExecutionExpiresAfter.Get(); ok {
		fmt.Printf("ExecutionExpiresAfter: %d\n", v)
	}
	if e, ok := getResp.Error.Get(); ok {
		fmt.Printf("Error Code: %s\n", e.Code)
		fmt.Printf("Error Message: %s\n", e.Message)
	}

	fmt.Println("----- list content generation task -----")

	pageNum := int32(1)
	pageSize := int32(10)
	status := contentgeneration.TaskStatusSucceeded
	tier := "default"
	listReq := &arkruntime.ListContentGenerationTasksRequest{
		PageNum:     &pageNum,
		PageSize:    &pageSize,
		Status:      &status,
		ServiceTier: &tier,
		// TaskIDs:     []string{"cgt-example-1", "cgt-example-2"},
		// Model:       &modelEp,
	}
	listResp, err := client.ListContentGenerationTasks(ctx, listReq)
	if err != nil {
		fmt.Printf("failed to list content generation tasks: %v\n", err)
	} else {
		fmt.Printf("ListContentGenerationTasks returned %d results\n", listResp.Total)
		for _, item := range listResp.Items {
			if v, ok := item.ServiceTier.Get(); ok {
				fmt.Printf("List Item ServiceTier: %s\n", v)
			}
			if v, ok := item.ExecutionExpiresAfter.Get(); ok {
				fmt.Printf("List Item ExecutionExpiresAfter: %d\n", v)
			}
		}
	}

	fmt.Println("----- delete content generation task -----")
	if err := client.DeleteContentGenerationTask(ctx, taskID); err != nil {
		fmt.Printf("delete content generation task error: %v\n", err)
	} else {
		fmt.Println("successfully deleted task id: ", taskID)
	}

}
