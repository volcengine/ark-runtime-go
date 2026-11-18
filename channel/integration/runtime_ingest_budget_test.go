// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
)

func TestRuntimeReportsDurableCapacityRejection(t *testing.T) {
	store, err := openEmbeddedFixture(filepath.Join(t.TempDir(), "budget.db"), embedded.Options{MaxChannelOpenTurns: 1, MaxRejectedMessages: 4, NoSync: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	observer := &recordingObserver{}
	options := testRuntimeOptions()
	options.Observer = observer
	runtime := mustRuntime(t, store, runtimeBinding(), newMAForHarness(t, &conformance.MAServer{}), &runtimeProvider{}, options)
	first, err := runtime.Enqueue(t.Context(), "binding", runtimeMessage())
	require.NoError(t, err)
	require.NotNil(t, first.Turn)
	observer.mu.Lock()
	observer.items = nil
	observer.mu.Unlock()
	message := runtimeMessage()
	message.MessageID = "rejected"
	message.ProviderEventID = "rejected-event"
	for _, duplicate := range []bool{false, true} {
		r, err := runtime.Enqueue(t.Context(), "binding", message)
		require.NoError(t, err)
		require.True(t, r.Rejected)
		require.Equal(t, duplicate, r.Duplicate)
		require.Nil(t, r.Turn)
		require.NotNil(t, r.Reply)
	}
	observer.mu.Lock()
	items := append([]core.Observation(nil), observer.items...)
	observer.mu.Unlock()
	require.Len(t, items, 2, "容量诊断不能误报stop或accepted")
	for _, item := range items {
		require.Equal(t, core.EventIngress, item.Name)
		require.Equal(t, "rejected", item.Status)
		require.Equal(t, "queue_full", item.ErrorKind)
		require.Empty(t, item.TurnID)
		require.Equal(t, core.RouteFor(message), item.ConversationID)
	}
	e, err := store.Export(t.Context())
	require.NoError(t, err)
	require.Len(t, e.Turns, 1)
	require.Len(t, e.Controls, 1)
	require.Len(t, e.Deliveries, 1)
	require.Equal(t, core.RejectControlKind, e.Controls[0].Kind)
}
