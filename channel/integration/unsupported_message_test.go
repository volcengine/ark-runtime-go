// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
)

func TestUnsupportedNoticeSurvivesRestartWithoutAgentExecution(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	first, err := openEmbeddedFixture(path, embedded.Options{})
	require.NoError(t, err)
	message := runtimeMessage()
	message.Text, message.Unsupported = "", true
	ingested, err := first.Ingest(context.Background(), core.NewBindingVersion(runtimeBinding()), message)
	require.NoError(t, err)
	require.NoError(t, first.Close())
	store, err := openEmbeddedStore(path, embedded.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	runtime := newRuntimeWithStore(t, store, f, p)
	runUntilCleanup(t, runtime)
	require.Eventually(t, func() bool {
		export, readErr := store.Export(context.Background())
		if readErr != nil {
			return false
		}
		for _, d := range export.Deliveries {
			if d.ID == ingested.Reply.ID {
				return d.Status == core.DeliveryDone
			}
		}
		return false
	}, time.Second, time.Millisecond)
	sent := p.delivered()
	require.Len(t, sent, 1)
	require.Contains(t, sent[0].Body.Text, "暂不支持")
	require.Equal(t, message.MessageID, sent[0].ReplyTarget.MessageID)
	f.Lock()
	defer f.Unlock()
	require.Zero(t, f.CreateCount)
	require.Zero(t, f.SendCount)
	require.Zero(t, f.InterruptCount)
}

func TestUnsupportedMessageCannotCarryExecutableContent(t *testing.T) {
	for _, mutate := range []func(*core.Message){
		func(m *core.Message) { m.Text = "/stop" },
		func(m *core.Message) { m.Attachments = []core.Attachment{{Kind: "file", Key: "file"}} },
		func(m *core.Message) { m.RecalledMessageID = "previous-message" },
	} {
		m := runtimeMessage()
		m.Text, m.Unsupported = "", true
		mutate(&m)
		require.ErrorIs(t, core.ValidateMessage(runtimeBinding(), m, 1000), core.ErrInvalid)
	}
}
