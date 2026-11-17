// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type budgetContextProvider struct{ *runtimeProvider }

func (budgetContextProvider) ChannelProvider() string { return "feishu" }
func (budgetContextProvider) ResolveMessageContext(context.Context, core.Message) (core.MessageContext, error) {
	return core.MessageContext{}, nil
}

func TestRuntimeChannelContextBudgetFailsOnlyTurn(t *testing.T) {
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	provider := budgetContextProvider{&runtimeProvider{}}
	remote := &conformance.MAServer{}
	runtime := mustRuntime(t, store, runtimeBinding(), newMAForHarness(t, remote), provider, testRuntimeOptions())
	message := runtimeMessage()
	message.Metadata = &core.MessageMetadata{RootMessageID: strings.Repeat("x", 5000)}
	result := mustEnqueue(t, runtime, "binding", message)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	defer func() { cancel(); require.NoError(t, <-done) }()
	require.Eventually(t, func() bool {
		turn, err := store.GetTurn(context.Background(), result.Turn.ID)
		return err == nil && turn.Status == core.TurnFailed && turn.Outcome.ErrorKind == "context_budget_exceeded"
	}, time.Second, 5*time.Millisecond)
	remote.Lock()
	require.Zero(t, remote.SendCount)
	remote.Unlock()
	message = runtimeMessage()
	message.Text, message.ChatID, message.ProviderEventID, message.MessageID = "/new", "other-chat", "other-event", "other-message"
	second := mustEnqueue(t, runtime, "binding", message)
	require.Eventually(t, func() bool {
		turn, err := store.GetTurn(context.Background(), second.Turn.ID)
		return err == nil && turn.Status == core.TurnDone
	}, time.Second, 5*time.Millisecond)
}
