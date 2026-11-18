// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package conformance_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func TestMAServerListsSuccessfulCreationForRecovery(t *testing.T) {
	fixture := &conformance.MAServer{}
	server := httptest.NewServer(fixture)
	defer server.Close()
	client, err := ma.NewClient(server.URL, "fixture-key", ma.ClientOptions{})
	require.NoError(t, err)
	created, err := client.CreateSession(context.Background(), core.CreateSessionSpec{
		Binding: conformance.SampleBinding(), CreationKey: "create-success", LookupKey: "lookup-success",
	})
	require.NoError(t, err)
	found, err := client.ListSessions(context.Background(), "lookup-success", "")
	require.NoError(t, err)
	require.Len(t, found.Sessions, 1, "成功创建的远端资源也必须可查账，不能只模拟5xx后已创建")
	require.Equal(t, created.ID, found.Sessions[0].ID)
}

func TestMAServerOnlyListsAcceptedCreations(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		persist bool
		want    int
	}{
		{"unknown accepted", 503, true, 1},
		{"unknown absent", 503, false, 0},
		{"rejected", 400, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := &conformance.MAServer{CreateCode: tc.status, CreatePersistThenError: tc.persist}
			server := httptest.NewServer(fixture)
			defer server.Close()
			client, err := ma.NewClient(server.URL, "fixture-key", ma.ClientOptions{})
			require.NoError(t, err)
			_, err = client.CreateSession(context.Background(), core.CreateSessionSpec{
				Binding: conformance.SampleBinding(), CreationKey: "create", LookupKey: "lookup",
			})
			require.Error(t, err)
			found, err := client.ListSessions(context.Background(), "lookup", "")
			require.NoError(t, err)
			require.Len(t, found.Sessions, tc.want)
		})
	}
}

func TestMAServerRejectedCreationCannotRewriteAcceptedProof(t *testing.T) {
	fixture := &conformance.MAServer{}
	server := httptest.NewServer(fixture)
	defer server.Close()
	client, err := ma.NewClient(server.URL, "fixture-key", ma.ClientOptions{})
	require.NoError(t, err)
	binding := conformance.SampleBinding()
	created, err := client.CreateSession(context.Background(), core.CreateSessionSpec{
		Binding: binding, CreationKey: "create-a", LookupKey: "lookup-a",
	})
	require.NoError(t, err)
	fixture.Lock()
	fixture.CreateCode = 503
	fixture.Unlock()
	binding.VaultIDs = []string{"foreign-vault"}
	_, err = client.CreateSession(context.Background(), core.CreateSessionSpec{
		Binding: binding, CreationKey: "create-b", LookupKey: "lookup-b",
	})
	require.Error(t, err)
	found, err := client.ListSessions(context.Background(), "lookup-a", "")
	require.NoError(t, err)
	require.Len(t, found.Sessions, 1)
	require.Equal(t, created.ID, found.Sessions[0].ID)
	require.Equal(t, "lookup-a", found.Sessions[0].Tags["channel_session_key"])
	require.Equal(t, []string{"vault"}, found.Sessions[0].VaultIDs)
	loaded, err := client.GetSession(context.Background(), created.ID)
	require.NoError(t, err)
	require.Equal(t, "lookup-a", loaded.Tags["channel_session_key"])
	require.Equal(t, []string{"vault"}, loaded.VaultIDs)
}
