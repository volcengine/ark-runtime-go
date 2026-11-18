// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/volcengine/ark-runtime-go/arkruntime"

	"github.com/volcengine/ark-runtime-go/channel"
	"github.com/volcengine/ark-runtime-go/channel/ma"
	"github.com/volcengine/ark-runtime-go/channel/provider/feishu"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
)

type resolver struct{}

func (resolver) Resolve(string) (string, error) { return "fixture-secret", nil }

func TestRuntimeRejectsProviderBoundToDifferentApplication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "channel.db")
	if err := embedded.Install(path, embedded.Options{}); err != nil {
		t.Fatal(err)
	}
	st, err := embedded.Open(path, embedded.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Error(err)
		}
	}()
	b := channel.Binding{ID: "local", AppID: "app-a", AgentID: "agent", AgentVersion: "v1", EnvironmentID: "env"}
	p, err := feishu.New(feishu.Config{Binding: b, AppSecretRef: "env:APP", SecretResolver: resolver{}})
	if err != nil {
		t.Fatal(err)
	}
	agent, err := ma.New(arkruntime.NewClientWithApiKey("fixture"), ma.Options{})
	if err != nil {
		t.Fatal(err)
	}
	b.AppID = "app-b"
	_, err = channel.NewRuntime(st, []channel.Connection{{Binding: b, MA: agent, Provider: p}}, channel.Options{})
	if err == nil {
		t.Fatal("accepted mismatched provider credentials and runtime binding")
	}
}

func TestUserAuthorizationRequiresStableNamespace(t *testing.T) {
	_, err := feishu.NewWithContext(context.Background(), feishu.Config{Binding: channel.Binding{ID: "local", AppID: "app", Identity: channel.IdentityPolicy{P2P: "user"}}, AppSecretRef: "env:APP", SecretResolver: resolver{}, UserAuthorization: &feishu.UserAuthorization{}})
	if err == nil {
		t.Fatal("user authorization accepted without a stable namespace")
	}
}
