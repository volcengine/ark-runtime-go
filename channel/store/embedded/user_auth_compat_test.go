// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestUserAuthUnknownRemoteRemainsBarrier(t *testing.T) {
	for _, protocol := range []string{"header", "kind"} {
		for _, resolved := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%v", protocol, resolved), func(t *testing.T) {
				s, err := openInstalledFixture(filepath.Join(t.TempDir(), "auth.db"), Options{})
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
				require.NoError(t, s.db.Update(func(tx *bbolt.Tx) error { return tx.Bucket(bucketRemoteOperations).Put([]byte("unknown"), data) }))
				_, err = s.BeginUserAuth(t.Context(), core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "worker", TTL: time.Minute})
				require.ErrorIs(t, err, core.ErrRemoteBarrier)
				require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
					require.Equal(t, data, tx.Bucket(bucketRemoteOperations).Get([]byte("unknown")))
					return nil
				}))
			})
		}
	}
}
