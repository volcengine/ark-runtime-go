// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Agent lifecycle example — Create → Get → List → Update → ListVersions → Delete.
//
// Runs against the outward /api/v3/agents endpoint. Requires:
//
//	export ARK_API_KEY=...
//	export ARK_MODEL_ID=doubao-seed-2-1-pro-260628   # or whatever you have access to
//	go run examples/agents/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/agent"
)

func main() {
	apiKey := os.Getenv("ARK_API_KEY")
	if apiKey == "" {
		log.Fatal("set ARK_API_KEY")
	}
	modelID := os.Getenv("ARK_MODEL_ID")
	if modelID == "" {
		modelID = "${YOUR_MODEL_ID}"
	}

	client := arkruntime.NewVolcClientWithApiKey(apiKey)
	ctx := context.Background()

	// 1. Create
	name := fmt.Sprintf("example-agent-%d", time.Now().UnixNano())
	created, err := client.CreateAgent(ctx, &agent.CreateAgentRequest{
		Name:        name,
		Model:       agent.ModelConfig{ID: modelID},
		Description: agent.NewOptString("created by ark-runtime-go example"),
	})
	if err != nil {
		log.Fatalf("create agent: %v", err)
	}
	fmt.Printf("created:    id=%s version=%d name=%s\n", created.ID, created.Version, created.Name)

	// Register cleanup up-front so we don't leak an agent if a later step fails.
	defer func() {
		if _, err := client.DeleteAgent(context.Background(), created.ID); err != nil {
			log.Printf("cleanup delete_agent(%s): %v", created.ID, err)
		} else {
			fmt.Printf("deleted:    id=%s\n", created.ID)
		}
	}()

	// 2. Get
	got, err := client.GetAgent(ctx, created.ID)
	if err != nil {
		log.Fatalf("get agent: %v", err)
	}
	fmt.Printf("get:        id=%s name=%s\n", got.ID, got.Name)

	// 3. List — takes limit / page / created_at_gte / created_at_lte.
	listed, err := client.ListAgents(ctx, &agent.AgentsListParams{
		Limit: agent.NewOptInt32(5),
	})
	if err != nil {
		log.Fatalf("list agents: %v", err)
	}
	fmt.Printf("list:       %d items, next_page=%q\n", len(listed.Data), listed.NextPage.Value)

	// 4. Update — bumps version. Requires the previous version for optimistic
	//    concurrency control.
	updated, err := client.UpdateAgent(ctx, created.ID, &agent.UpdateAgentRequest{
		Version:     created.Version,
		Description: agent.NewOptString("updated by ark-runtime-go example"),
	})
	if err != nil {
		log.Fatalf("update agent: %v", err)
	}
	fmt.Printf("updated:    id=%s version=%d (was %d)\n", updated.ID, updated.Version, created.Version)

	// 5. List versions — should see at least v1 (create) + v2 (update).
	versions, err := client.ListAgentVersions(ctx, created.ID, &agent.AgentsListVersionsParams{
		Limit: agent.NewOptInt32(10),
	})
	if err != nil {
		log.Fatalf("list agent versions: %v", err)
	}
	fmt.Printf("versions:   %d items\n", len(versions.Data))
}
