// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/channel"
	"github.com/volcengine/ark-runtime-go/channel/ma"
	"github.com/volcengine/ark-runtime-go/channel/provider/feishu"
	"github.com/volcengine/ark-runtime-go/channel/secret"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
)

func run() error {
	if os.Getenv("ARK_API_KEY") == "" || os.Getenv("FEISHU_APP_SECRET") == "" {
		return errors.New("set ARK_API_KEY and FEISHU_APP_SECRET")
	}
	path := os.Getenv("CHANNEL_DATABASE")
	if path == "" {
		path = "channel.db"
	}
	if len(os.Args) == 2 && os.Args[1] == "install" {
		return embedded.Install(path, embedded.Options{})
	}
	store, err := embedded.Open(path, embedded.Options{})
	if err != nil {
		return err
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Print(err)
		}
	}()
	agent, err := ma.New(arkruntime.NewClientWithApiKey(os.Getenv("ARK_API_KEY"), arkruntime.WithRetryTimes(0)), ma.Options{})
	if err != nil {
		return err
	}
	binding := channel.Binding{ID: "primary", AppID: os.Getenv("FEISHU_APP_ID"), AgentID: os.Getenv("ARK_AGENT_ID"), AgentVersion: "v1", EnvironmentID: os.Getenv("ARK_ENVIRONMENT_ID"), GroupTrigger: "mention", GroupReplyMode: "thread"}
	provider, err := feishu.New(feishu.Config{Binding: binding, AppSecretRef: "env:FEISHU_APP_SECRET", SecretResolver: secret.EnvFileResolver{}})
	if err != nil {
		return err
	}
	runtime, err := channel.NewRuntime(store, []channel.Connection{{Binding: binding, MA: agent, Provider: provider}}, channel.Options{})
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	return runtime.Run(ctx)
}
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
