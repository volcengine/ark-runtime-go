// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// MemoryStore + Memory lifecycle example.
//
// A MemoryStore is a namespace of Memory documents keyed by path. This example
// covers the full CRUD on both levels:
//
//	POST   /api/v3/memory_stores                         (CreateMemoryStore)
//	GET    /api/v3/memory_stores/:store_id               (GetMemoryStore)
//	GET    /api/v3/memory_stores                         (ListMemoryStores)
//	POST   /api/v3/memory_stores/:store_id               (UpdateMemoryStore)
//	POST   /api/v3/memory_stores/:store_id/memories      (CreateMemory)
//	GET    /api/v3/memory_stores/:store_id/memories/:id  (GetMemory)
//	GET    /api/v3/memory_stores/:store_id/memories      (ListMemories)
//	POST   /api/v3/memory_stores/:store_id/memories/:id  (UpdateMemory)
//	DELETE /api/v3/memory_stores/:store_id/memories/:id  (DeleteMemory)
//	DELETE /api/v3/memory_stores/:store_id               (DeleteMemoryStore)
//
//	export ARK_API_KEY=...
//	go run examples/memory_stores/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/memory"
)

func main() {
	apiKey := os.Getenv("ARK_API_KEY")
	if apiKey == "" {
		log.Fatal("set ARK_API_KEY")
	}

	client := arkruntime.NewByteplusClientWithApiKey(apiKey)
	ctx := context.Background()

	// 1. Create a memory store.
	storeName := fmt.Sprintf("example-store-%d", time.Now().UnixNano())
	store, err := client.CreateMemoryStore(ctx, &memory.CreateMemoryStoreRequest{
		Name: storeName,
	})
	if err != nil {
		log.Fatalf("create memory store: %v", err)
	}
	fmt.Printf("store:      id=%s name=%s\n", store.ID, store.Name)

	defer func() {
		if _, err := client.DeleteMemoryStore(context.Background(), store.ID); err != nil {
			log.Printf("cleanup delete_memory_store(%s): %v", store.ID, err)
		} else {
			fmt.Printf("store:      deleted id=%s\n", store.ID)
		}
	}()

	// 2. Create a memory doc inside it.
	path := fmt.Sprintf("/example/note-%d.md", time.Now().UnixNano())
	mem, err := client.CreateMemory(ctx, store.ID, &memory.CreateMemoryRequest{
		Path:    path,
		Content: "hello from ark-runtime-go example",
	})
	if err != nil {
		log.Fatalf("create memory: %v", err)
	}
	fmt.Printf("memory:     id=%s path=%s sha256=%s\n", mem.ID, mem.Path, mem.ContentSHA256)

	// 3. Get + list.
	got, err := client.GetMemory(ctx, store.ID, mem.ID)
	if err != nil {
		log.Fatalf("get memory: %v", err)
	}
	fmt.Printf("get:        id=%s path=%s\n", got.ID, got.Path)

	list, err := client.ListMemories(ctx, store.ID, &memory.MemoriesListParams{
		Limit: memory.NewOptInt32(10),
	})
	if err != nil {
		log.Fatalf("list memories: %v", err)
	}
	fmt.Printf("list:       %d items in store\n", len(list.Data))

	// 4. Update — the SHA256 should change after new content.
	if _, err := client.UpdateMemory(ctx, store.ID, mem.ID, &memory.UpdateMemoryRequest{
		Content: memory.NewOptString("updated content"),
	}); err != nil {
		log.Fatalf("update memory: %v", err)
	}
	got2, err := client.GetMemory(ctx, store.ID, mem.ID)
	if err != nil {
		log.Fatalf("re-get memory: %v", err)
	}
	fmt.Printf("updated:    id=%s new_sha256=%s (was %s)\n", got2.ID, got2.ContentSHA256, mem.ContentSHA256)

	// 5. Delete memory (store is cleaned up via defer above).
	if _, err := client.DeleteMemory(ctx, store.ID, mem.ID); err != nil {
		log.Fatalf("delete memory: %v", err)
	}
	fmt.Printf("memory:     deleted id=%s\n", mem.ID)
}
