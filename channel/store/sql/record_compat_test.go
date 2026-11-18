// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestSQLRecordUpdatesPreserveFutureFields(t *testing.T) {
	db := openSQLite(t, filepath.Join(t.TempDir(), "future.db"))
	s, err := openInstalledSQLFixture(t.Context(), db, SQLite, Options{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	_, err = s.Ingest(t.Context(), conformance.SampleVersion(), conformance.SampleMessage("future"))
	require.NoError(t, err)
	leased, err := s.LeaseTurn(t.Context(), conformance.Claim("current-worker"))
	require.NoError(t, err)
	var raw string
	require.NoError(t, db.QueryRow("SELECT data FROM channel_turns WHERE id=?", leased.Turn.ID).Scan(&raw))
	var future map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &future))
	future["future_record"] = map[string]any{"release": "next"}
	session := future["session"].(map[string]any)
	session["future_session"] = map[string]any{"proof": "preserve"}
	data, err := json.Marshal(future)
	require.NoError(t, err)
	_, err = db.Exec("UPDATE channel_turns SET data=? WHERE id=?", string(data), leased.Turn.ID)
	require.NoError(t, err)
	_, err = s.RenewTurn(t.Context(), leased.Turn.Lease, time.Minute)
	require.NoError(t, err)
	_, err = s.UpdateTurn(t.Context(), leased.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate})
	require.NoError(t, err)
	require.NoError(t, db.QueryRow("SELECT data FROM channel_turns WHERE id=?", leased.Turn.ID).Scan(&raw))
	require.Contains(t, raw, "future_record")
	require.Contains(t, raw, "future_session")
	require.Contains(t, raw, "preserve")
	turn, err := s.GetTurn(t.Context(), leased.Turn.ID)
	require.NoError(t, err)
	require.Equal(t, core.PhaseCreate, turn.Phase)
}
