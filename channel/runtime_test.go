// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package channel_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime"

	"github.com/volcengine/ark-runtime-go/channel"
	"github.com/volcengine/ark-runtime-go/channel/ma"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
)

type localProvider struct{ deliveries chan channel.Delivery }

func (p *localProvider) Start(ctx context.Context, accept func(context.Context, channel.Message) error) error {
	if err := accept(ctx, channel.Message{BindingID: "local", ProviderEventID: "event-1", MessageID: "message-1", TenantKey: "tenant", ChatID: "chat", ChatType: "p2p", SenderID: "user", Text: "hello", ReceivedAt: time.Now()}); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}
func (p *localProvider) Deliver(_ context.Context, d channel.Delivery) (string, error) {
	p.deliveries <- d
	return "reply-1", nil
}

func TestRuntimeRunsCustomProviderAndHookWithOpaqueStore(t *testing.T) {
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
	agent, err := ma.New(arkruntime.NewClientWithApiKey("fixture"), ma.Options{})
	if err != nil {
		t.Fatal(err)
	}
	p := &localProvider{deliveries: make(chan channel.Delivery, 1)}
	r, err := channel.NewRuntime(st, []channel.Connection{{Binding: channel.Binding{ID: "local", AppID: "app", AgentID: "agent", AgentVersion: "v1", EnvironmentID: "env", GroupTrigger: "mention", GroupReplyMode: "thread"}, MA: agent, Provider: channel.NewProvider(p)}}, channel.Options{PollInterval: time.Millisecond, Hooks: channel.Hooks{BeforeTrigger: func(context.Context, channel.HookMessage) (channel.TriggerDecision, error) {
		return channel.TriggerDecision{Allow: false, Summary: "handled locally"}, nil
	}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	select {
	case d := <-p.deliveries:
		if d.Text != "handled locally" || d.ReplyTarget.ChatID != "chat" {
			t.Fatalf("unexpected delivery: %+v", d)
		}
	case <-ctx.Done():
		t.Fatal("delivery timed out")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeRejectsFinalStreamingForUnsupportedCustomProvider(t *testing.T) {
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
	agent, err := ma.New(arkruntime.NewClientWithApiKey("fixture"), ma.Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = channel.NewRuntime(st, []channel.Connection{{Binding: channel.Binding{ID: "local", AppID: "app", AgentID: "agent", AgentVersion: "v1", EnvironmentID: "env", StreamFinalAnswer: true}, MA: agent, Provider: channel.NewProvider(&localProvider{})}}, channel.Options{})
	if err == nil {
		t.Fatal("unsupported final-stream configuration accepted and would discard the reply")
	}
}
