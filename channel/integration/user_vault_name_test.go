// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestNewPersonalVaultDisplayNameUsesOpenID(t *testing.T) {
	s := newUserStore(t)
	b := userBinding()
	client := &userVaultMA{MAClient: newMAForHarness(t, &conformance.MAServer{EnvironmentType: "cloud"})}
	runtime, err := core.NewRuntime(s, []core.Connection{{Binding: b, MAClient: client, Provider: &runtimeProvider{}, UserAuthorizer: &userAuthFixture{ready: true}}}, testRuntimeOptions())
	require.NoError(t, err)
	message := runtimeMessage()
	message.SenderID = "ou_display-user"
	mustEnqueue(t, runtime, b.ID, message)
	runUntilCleanup(t, runtime)
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.creates == 1
	}, time.Second, time.Millisecond)
	client.mu.Lock()
	defer client.mu.Unlock()
	require.Equal(t, "Channel User · ou_display-user", client.displayName)
}
