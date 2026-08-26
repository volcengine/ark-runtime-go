// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Self-hosted worker runner.
//
// Required:
//
//	export ARK_API_KEY=...
//	export MA_ENVIRONMENT_ID=env_xxx
//
// Run from examples/:
//
//	go run ./self_hosted_worker
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/lib/environments"
)

func main() {
	apiKey := os.Getenv("ARK_API_KEY")
	baseURL := os.Getenv("ARK_BASE_URL")
	envID := os.Getenv("MA_ENVIRONMENT_ID")

	if apiKey == "" || envID == "" {
		log.Fatal("ARK_API_KEY and MA_ENVIRONMENT_ID are required")
	}

	clientOptions := make([]arkruntime.ConfigOption, 0, 1)
	if baseURL != "" {
		clientOptions = append(clientOptions, arkruntime.WithBaseUrl(baseURL))
	}
	client := arkruntime.NewClientWithApiKey(apiKey, clientOptions...)

	worker := environments.NewEnvironmentWorkerForClient(client, environments.EnvironmentWorkerOptions{
		EnvironmentID: envID,
		Workdir:       ".",
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := worker.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
