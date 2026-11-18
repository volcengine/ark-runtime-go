// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type notSentError struct{}

func (notSentError) Error() string        { return "request setup unavailable" }
func (notSentError) RequestNotSent() bool { return true }

type unsentCreationMA struct {
	core.MAClient
	calls atomic.Int32
}

func (m *unsentCreationMA) CreateSession(ctx context.Context, spec core.CreateSessionSpec) (core.Session, error) {
	if m.calls.Add(1) == 1 {
		return core.Session{}, errors.Join(errors.New("retryable local failure"), notSentError{})
	}
	return m.MAClient.CreateSession(ctx, spec)
}
func TestRuntimeKnownUnsentCreateRetriesFrozenRequest(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	client := &unsentCreationMA{MAClient: newMAForHarness(t, f)}
	runtime := mustRuntime(t, store, runtimeBinding(), client, p, testRuntimeOptions())
	m := runtimeMessage()
	m.Text = "/new"
	mustEnqueue(t, runtime, "binding", m)
	runUntilCleanup(t, runtime)
	state := waitLatest(t, store, core.TurnDone, core.DeliveryDone, 3*time.Second)
	require.NotEmpty(t, state.turn.Session.SnapshotID)
	require.Equal(t, int32(2), client.calls.Load())
	f.Lock()
	defer f.Unlock()
	require.Equal(t, 1, f.CreateCount)
	require.Zero(t, f.ListSessionCount)
}

type cancelledUnsentMA struct {
	core.MAClient
	cancel context.CancelFunc
}

func (m cancelledUnsentMA) CreateSession(context.Context, core.CreateSessionSpec) (core.Session, error) {
	m.cancel()
	return core.Session{}, notSentError{}
}
func TestKnownUnsentCreationPersistsProofBeforeShutdown(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := cancelledUnsentMA{MAClient: newMAForHarness(t, f), cancel: cancel}
	first := mustRuntime(t, store, runtimeBinding(), client, p, testRuntimeOptions())
	message := runtimeMessage()
	message.Text = "/new"
	accepted := mustEnqueue(t, first, "binding", message)
	require.NoError(t, first.Run(ctx))
	saved, err := store.GetTurn(context.Background(), accepted.Turn.ID)
	require.NoError(t, err)
	require.Zero(t, saved.Submit.WriteAttempts, "shutdown must not turn known-unsent into unknown")
	second := mustRuntime(t, store, runtimeBinding(), newMAForHarness(t, f), p, testRuntimeOptions())
	runUntilCleanup(t, second)
	waitLatest(t, store, core.TurnDone, core.DeliveryDone, time.Second)
	f.Lock()
	defer f.Unlock()
	require.Equal(t, 1, f.CreateCount)
	require.Zero(t, f.ListSessionCount)
}
