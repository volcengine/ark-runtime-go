// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type contextSnapshotStore struct {
	Store
	snapshot *SessionSnapshot
}

func (s contextSnapshotStore) GetSessionSnapshot(context.Context, string) (*SessionSnapshot, error) {
	return s.snapshot, nil
}

type contextProviderFixture struct{ Provider }

func (contextProviderFixture) ChannelProvider() string { return "feishu" }
func (contextProviderFixture) ResolveMessageContext(context.Context, Message) (MessageContext, error) {
	return MessageContext{ChatMode: "group"}, nil
}

func TestChannelContextUsesActualFrozenSessionIdentity(t *testing.T) {
	binding := Binding{ID: "binding", AppID: "app", AgentID: "agent", AgentVersion: "1", Identity: IdentityPolicy{P2P: IdentityUser}}
	snapshot := NewSessionSnapshot("route", 1, CreateSessionSpec{Binding: binding, CreationKey: "create", LookupKey: "lookup", ChatType: "p2p", EnvironmentOverride: &EnvironmentOverride{Type: "cloud", Env: map[string]string{
		EnvLarkSuiteAppID: "app", EnvLarkSuiteDefaultAs: "user", EnvLarkSuiteStrictMode: "user",
	}}})
	options, err := (RuntimeOptions{}).Normalize()
	require.NoError(t, err)
	r := &Runtime{store: contextSnapshotStore{snapshot: &snapshot}, options: options}
	j := &job{Turn: Turn{ConversationID: "route", Session: TurnSession{SnapshotID: snapshot.ID, Generation: 1}}, msg: Message{ChatType: "p2p", SenderID: "ou_actual"}}
	current := binding
	current.Identity.P2P = IdentityBot
	got, err := r.channelContext(t.Context(), j, Connection{Binding: current, Provider: contextProviderFixture{}}, false)
	require.NoError(t, err)
	require.Contains(t, got, `execution_identity="user"`)
	require.Contains(t, got, `strict_mode="user"`)
	require.Contains(t, got, `sender_open_id="ou_actual"`)
	require.Contains(t, got, `chat_mode="p2p"`)
	require.NotContains(t, got, `execution_identity="bot"`)
	current.AppID = "other-app"
	_, err = r.channelContext(t.Context(), j, Connection{Binding: current, Provider: contextProviderFixture{}}, false)
	require.ErrorIs(t, err, ErrConflict)
}
