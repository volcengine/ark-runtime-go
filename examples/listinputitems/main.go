// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// List-input-items example: creates a response, then paginates through
// its input items via ListResponseInputItems.
//
// Run with:
//
//	ARK_API_KEY=... go run ./listinputitems
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/responses"
)

const defaultLimit int32 = 10

func main() {
	client := arkruntime.NewClientWithApiKey(os.Getenv("ARK_API_KEY"))
	ctx := context.Background()

	resp, err := client.CreateResponses(ctx, &responses.ResponsesRequest{
		Model: "doubao-seed-2-1-pro",
		Input: responses.NewStringResponsesInput("hello"),
	})
	if err != nil {
		fmt.Printf("create error: %v\n", err)
		return
	}
	fmt.Printf("created response: id=%s\n", resp.ID)

	params := &responses.ResponsesListInputItemsParams{
		ResponseId: resp.ID,
		Limit:      responses.NewOptInt32(defaultLimit),
	}

	page, err := client.ListResponseInputItems(ctx, params)
	if err != nil {
		fmt.Printf("list error: %v\n", err)
		return
	}
	for _, item := range page.Data {
		fmt.Printf("%+v\n", item)
	}

	hasMore, _ := page.HasMore.Get()
	if !hasMore {
		return
	}

	params.Before = responses.NewOptString(page.FirstID)
	page, err = client.ListResponseInputItems(ctx, params)
	if err != nil {
		fmt.Printf("list error: %v\n", err)
		return
	}
	for _, item := range page.Data {
		fmt.Printf("%+v\n", item)
	}
}
