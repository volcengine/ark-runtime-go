// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestUserAuthWithoutWorkKind(t *testing.T) {
	codec := &conformance.UserAuthTestCodec{}
	factory := func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		db := lifecycleDB(t)
		opts := Options{Clock: c.Clock, PayloadCodec: codec, PayloadNamespace: "without-kind", RequirePayloadEncryption: true}
		require.NoError(t, Install(context.Background(), db, SQLite, opts))
		removeFixtureWorkKind(t, db)
		s, err := open(context.Background(), db, SQLite, opts)
		require.NoError(t, err)
		return s
	}
	t.Run("ingest", func(t *testing.T) { conformance.RunUserAuthIngest(t, factory) })
	t.Run("gate", func(t *testing.T) { conformance.RunUserAuthStore(t, factory) })
	t.Run("wait", func(t *testing.T) { conformance.RunUserAuthWait(t, factory) })
	t.Run("payload", func(t *testing.T) { conformance.RunUserAuthPayload(t, codec, factory) })
	t.Run("race", func(t *testing.T) { conformance.RunUserAuthRace(t, factory) })
	t.Run("refresh", func(t *testing.T) { conformance.RunUserAuthRefresh(t, factory) })
	t.Run("uncertain", func(t *testing.T) { conformance.RunUserAuthUncertainFailure(t, factory) })
}

func TestUserAuthUnknownRemoteRemainsBarrier(t *testing.T) {
	for _, protocol := range []string{"header", "kind"} {
		for _, resolved := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%v", protocol, resolved), func(t *testing.T) {
				ctx := context.Background()
				db := lifecycleDB(t)
				s, err := openInstalledSQLFixture(ctx, db, SQLite, Options{})
				require.NoError(t, err)
				defer func() { require.NoError(t, s.Close()) }()
				p := core.UserPrincipal{Owner: "owner", Project: "project", ChannelID: "channel", Epoch: 1, AppID: "app", TenantKey: "tenant", OpenID: "user"}
				raw := map[string]any{"id": "unknown", "owner": p.Owner, "kind": "input", "resolved": resolved}
				switch protocol {
				case "header":
					raw["_work_kind"] = "future"
				default:
					raw["kind"] = "future"
				}
				data, err := json.Marshal(raw)
				require.NoError(t, err)
				_, err = db.Exec("INSERT INTO channel_remote_operations(id,owner,turn_id,session_id,resolved,eligible_at,data) VALUES (?,?,?,'',?,0,?)", "unknown", p.Owner, "unknown-turn", resolved, string(data))
				require.NoError(t, err)
				_, err = s.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "worker", TTL: time.Minute})
				require.ErrorIs(t, err, core.ErrRemoteBarrier)
				var after string
				require.NoError(t, db.QueryRow("SELECT data FROM channel_remote_operations WHERE id='unknown'").Scan(&after))
				require.Equal(t, string(data), after)
			})
		}
	}
}
