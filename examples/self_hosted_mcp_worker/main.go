// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Self-hosted worker with client-side MCP tools.
//
// Required:
//
//	export ARK_API_KEY=...
//	export MA_ENVIRONMENT_ID=env_xxx
//
// Run the bundled MCP server from this directory:
//
//	go run . -- go run ./server
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/lib/environments"
	arkmcp "github.com/volcengine/ark-runtime-go/mcp"
)

func main() {
	apiKey := mustEnv("ARK_API_KEY")
	environmentID := mustEnv("MA_ENVIRONMENT_ID")
	commandArgs := mcpCommandArgs(os.Args[1:])
	if len(commandArgs) == 0 {
		log.Fatal("MCP server command is required; example: go run . -- go run ./server")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	command := exec.CommandContext(ctx, commandArgs[0], commandArgs[1:]...)
	command.Env = environmentWithout(os.Environ(), "ARK_API_KEY")
	mcpClient := mcpsdk.NewClient(&mcpsdk.Implementation{
		Name:    "ark-self-hosted-worker-example",
		Version: "1.0.0",
	}, nil)
	mcpSession, err := mcpClient.Connect(ctx, &mcpsdk.CommandTransport{Command: command}, nil)
	if err != nil {
		log.Fatalf("connect MCP server: %v", err)
	}
	defer func() {
		if closeErr := mcpSession.Close(); closeErr != nil {
			log.Printf("close MCP session: %v", closeErr)
		}
	}()

	tools := make([]*mcpsdk.Tool, 0)
	for tool, listErr := range mcpSession.Tools(ctx, nil) {
		if listErr != nil {
			log.Fatalf("list MCP tools: %v", listErr)
		}
		tools = append(tools, tool)
	}
	declarations, err := arkmcp.CustomToolItems(tools)
	if err != nil {
		log.Fatalf("convert MCP tool declarations: %v", err)
	}
	for _, declaration := range declarations {
		raw, marshalErr := declaration.MarshalJSON()
		if marshalErr != nil {
			log.Fatalf("marshal MCP tool declaration: %v", marshalErr)
		}
		fmt.Printf("Agent custom tool: %s\n", raw)
	}

	customTools, err := arkmcp.NewTools(tools, mcpSession)
	if err != nil {
		log.Fatalf("create MCP worker tools: %v", err)
	}
	clientOptions := make([]arkruntime.ConfigOption, 0, 1)
	if baseURL := os.Getenv("ARK_BASE_URL"); baseURL != "" {
		clientOptions = append(clientOptions, arkruntime.WithBaseUrl(baseURL))
	}
	client := arkruntime.NewClientWithApiKey(apiKey, clientOptions...)
	worker := environments.NewEnvironmentWorkerForClient(client, environments.EnvironmentWorkerOptions{
		EnvironmentID: environmentID,
		Workdir:       ".",
		CustomTools:   customTools,
	})
	if err := worker.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func mcpCommandArgs(args []string) []string {
	if len(args) > 0 && args[0] == "--" {
		return args[1:]
	}
	return args
}

func environmentWithout(values []string, names ...string) []string {
	removed := make(map[string]struct{}, len(names))
	for _, name := range names {
		removed[name] = struct{}{}
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		name, _, _ := strings.Cut(value, "=")
		if _, ok := removed[name]; !ok {
			out = append(out, value)
		}
	}
	return out
}

func mustEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("%s is required", name)
	}
	return value
}
