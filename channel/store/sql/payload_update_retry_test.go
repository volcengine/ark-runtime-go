// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestSQLiteUpdateTurnFailedReplayReturnsNoRolledBackValue(t *testing.T) {
	ctx := t.Context()
	clock := conformance.NewClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c := newPayloadTestCodec(t)
	s, err := openInstalledSQLFixture(ctx, openSQLite(t, filepath.Join(t.TempDir(), "update-retry.db")), SQLite, Options{Clock: clock.Now, PayloadCodec: c, PayloadNamespace: "deployment", RequirePayloadEncryption: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	_, err = s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("update"))
	require.NoError(t, err)
	lease, err := s.LeaseTurn(ctx, conformance.Claim("worker"))
	require.NoError(t, err)
	require.NotNil(t, lease)
	fired := false
	c.beforeSeal = func() {
		if fired {
			return
		}
		fired = true
		clock.Advance(2 * time.Minute)
	}
	got, err := s.UpdateTurn(ctx, lease.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "creation"}, Input: core.FrozenInput{ContextFrozen: true, Context: "frozen"}})
	require.True(t, fired)
	require.ErrorIs(t, err, core.ErrLeaseLost)
	require.Equal(t, core.Turn{}, got, "a failed replay returns no stale success value from the rolled-back attempt")
	persisted, err := s.GetTurn(ctx, lease.Turn.ID)
	require.NoError(t, err)
	require.Equal(t, lease.Turn.Phase, persisted.Phase)
	require.False(t, persisted.Input.ContextFrozen)
}
