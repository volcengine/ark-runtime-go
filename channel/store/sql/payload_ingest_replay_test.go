// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type replayIngressResolver struct{ binding core.Binding }

func (r replayIngressResolver) ResolveIngest(context.Context, core.IngestScope) (core.ResolvedIngest, error) {
	return core.ResolvedIngest{Connection: core.Connection{Binding: r.binding}, AdmitUntil: time.Now().Add(time.Hour)}, nil
}
func (replayIngressResolver) ResolveForTurn(context.Context, core.Turn, *core.SessionSnapshot) (core.Connection, error) {
	return core.Connection{}, core.ErrRuntimeConfig
}
func (replayIngressResolver) ResolveForDelivery(context.Context, core.Delivery) (core.Connection, error) {
	return core.Connection{}, core.ErrRuntimeConfig
}

type replayObserver func(core.Observation)

func (o replayObserver) Observe(_ context.Context, v core.Observation) { o(v) }

func payloadIngestResultTracksCommittedBranch(t *testing.T, open func(*testing.T, *payloadTestCodec) *store) {
	t.Helper()
	for _, accepted := range []bool{true, false} {
		name := "accepted_to_rejected"
		if accepted {
			name = "rejected_to_accepted"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			c := newPayloadTestCodec(t)
			s := open(t, c)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			version := conformance.SampleVersion()
			if accepted {
				_, err := s.Ingest(ctx, version, conformance.SampleMessage("initial-occupant"))
				require.NoError(t, err)
			}
			fired := false
			c.beforeSeal = func() {
				if fired {
					return
				}
				fired = true
				if accepted {
					lease, err := s.LeaseTurn(ctx, conformance.Claim("release-capacity"))
					require.NoError(t, err)
					require.NotNil(t, lease)
					require.NoError(t, s.SettleTurn(ctx, lease.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, nil))
					return
				}
				occupying := conformance.SampleMessage("new-occupant")
				occupying.ChatID = "another-chat"
				result, err := s.Ingest(ctx, version, occupying)
				require.NoError(t, err)
				require.NotNil(t, result.Turn)
			}
			var observations []core.Observation
			runtime, err := core.NewRuntime(s, nil, core.RuntimeOptions{Resolver: replayIngressResolver{binding: version.Content}, Observer: replayObserver(func(v core.Observation) { observations = append(observations, v) })})
			require.NoError(t, err)
			message := conformance.SampleMessage("replayed")
			result, err := runtime.EnqueueScoped(ctx, core.IngestScope{BindingID: version.BindingID}, message)
			require.NoError(t, err, "only the final committed branch may be hydrated")
			require.True(t, fired, "capacity must change during lock-free Seal")
			require.False(t, result.Duplicate)
			require.Equal(t, !accepted, result.Rejected)
			require.Nil(t, result.Stopped)
			require.Equal(t, message.Text, result.Message.Message.Text)
			require.Len(t, observations, 1)
			require.Equal(t, core.EventIngress, observations[0].Name)
			if accepted {
				require.NotNil(t, result.Turn)
				require.Nil(t, result.Control)
				require.Nil(t, result.Reply)
				require.Equal(t, "accepted", observations[0].Status)
				require.Equal(t, result.Turn.ID, observations[0].TurnID)
			} else {
				require.Nil(t, result.Turn, "the rolled-back accepted Turn must not escape")
				require.NotNil(t, result.Control)
				require.NotNil(t, result.Reply)
				require.Equal(t, "rejected", observations[0].Status)
				require.Equal(t, "queue_full", observations[0].ErrorKind)
				require.Empty(t, observations[0].TurnID)
			}
			exported, err := s.Export(ctx)
			require.NoError(t, err)
			require.Len(t, exported.Messages, 2)
			if accepted {
				require.Len(t, exported.Turns, 2)
				require.Empty(t, exported.Controls)
				require.Empty(t, exported.Deliveries)
			} else {
				require.Len(t, exported.Turns, 1)
				require.Len(t, exported.Controls, 1)
				require.Len(t, exported.Deliveries, 1)
				require.Equal(t, result.Reply.ID, exported.Deliveries[0].ID)
			}
			duplicate, err := s.Ingest(ctx, version, message)
			require.NoError(t, err)
			require.True(t, duplicate.Duplicate)
			require.Equal(t, result.Rejected, duplicate.Rejected)
			if accepted {
				require.Equal(t, result.Turn.ID, duplicate.Turn.ID)
			} else {
				require.Nil(t, duplicate.Turn)
				require.Equal(t, result.Reply.ID, duplicate.Reply.ID)
			}
		})
	}
}

func TestSQLitePayloadIngestResultTracksCommittedBranch(t *testing.T) {
	payloadIngestResultTracksCommittedBranch(t, func(t *testing.T, c *payloadTestCodec) *store {
		db := openSQLite(t, filepath.Join(t.TempDir(), "replay.db"))
		s, err := openInstalledSQLFixture(t.Context(), db, SQLite, Options{PayloadCodec: c, PayloadNamespace: "deployment", RequirePayloadEncryption: true, MaxChannelOpenTurns: 1, MaxRejectedMessages: 8})
		require.NoError(t, err)
		return s
	})
}
