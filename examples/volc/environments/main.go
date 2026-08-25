// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Environment lifecycle example — Create → Get → List → Update → Delete.
//
// An Environment is the sandbox (network + filesystem policy) an Agent runs
// inside during a Session. This example uses the cloud environment with
// unrestricted networking; production usage will typically restrict either.
//
//	export ARK_API_KEY=...
//	go run examples/environments/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/environment"
)

func main() {
	apiKey := os.Getenv("ARK_API_KEY")
	if apiKey == "" {
		log.Fatal("set ARK_API_KEY")
	}

	client := arkruntime.NewVolcClientWithApiKey(apiKey)
	ctx := context.Background()

	// 1. Create — cloud + unrestricted network.
	name := fmt.Sprintf("example-env-%d", time.Now().UnixNano())
	created, err := client.CreateEnvironment(ctx, &environment.CreateEnvironmentRequest{
		Name: name,
		Config: environment.NewOptEnvConfig(environment.EnvConfig{
			Type: environment.EnvConfigTypeCloud,
			Networking: environment.NewOptNetworkingConfig(environment.NetworkingConfig{
				Type: environment.NetworkingTypeUnrestricted,
			}),
		}),
	})
	if err != nil {
		log.Fatalf("create environment: %v", err)
	}
	fmt.Printf("created:    id=%s name=%s\n", created.ID, created.Name)

	defer func() {
		if _, err := client.DeleteEnvironment(context.Background(), created.ID); err != nil {
			log.Printf("cleanup delete_environment(%s): %v", created.ID, err)
		} else {
			fmt.Printf("deleted:    id=%s\n", created.ID)
		}
	}()

	// 2. Get
	got, err := client.GetEnvironment(ctx, created.ID)
	if err != nil {
		log.Fatalf("get environment: %v", err)
	}
	fmt.Printf("get:        id=%s name=%s type=%v\n", got.ID, got.Name, got.Type)

	// 3. List
	listed, err := client.ListEnvironments(ctx, &environment.EnvironmentsListParams{
		Limit: environment.NewOptInt32(5),
	})
	if err != nil {
		log.Fatalf("list environments: %v", err)
	}
	fmt.Printf("list:       %d items, next_page=%q\n", len(listed.Data), listed.NextPage.Value)

	// 4. Update — attach a description.
	updated, err := client.UpdateEnvironment(ctx, created.ID, &environment.UpdateEnvironmentRequest{
		Description: environment.NewOptString("updated by ark-runtime-go example"),
	})
	if err != nil {
		log.Fatalf("update environment: %v", err)
	}
	fmt.Printf("updated:    id=%s description=%q\n", updated.ID, updated.Description.Value)
}
