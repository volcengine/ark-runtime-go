// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
)

func TestUserAuthReopenAndCiphertextAtRest(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "auth.db")
	clock := conformance.NewClock(time.Now().UTC())
	c := &conformance.UserAuthTestCodec{}
	opts := Options{Clock: clock.Now, PayloadCodec: c, PayloadNamespace: "test", RequirePayloadEncryption: true}
	s, e := openInstalledFixture(path, opts)
	require.NoError(t, e)
	p := core.UserPrincipal{Owner: "owner", Project: "project", ChannelID: "channel", Epoch: 1, AppID: "app", TenantKey: "tenant", OpenID: "user"}
	b := conformance.SampleBinding()
	b.Owner = p.Owner
	b.Project = p.Project
	_, e = s.Ingest(ctx, core.NewBindingVersion(b), conformance.SampleMessage("reopen"))
	require.NoError(t, e)
	l, e := s.LeaseTurn(ctx, conformance.Claim("worker"))
	require.NoError(t, e)
	r, e := s.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "auth", TTL: time.Minute})
	require.NoError(t, e)
	envelope, e := c.Seal(ctx, codec.Scope{Namespace: "test", Owner: p.Owner, Project: p.Project, RecordKind: "user_auth", RecordID: p.Key(), Field: "device", PayloadVersion: 1, LogicalSchema: 1}, []byte("device-token-at-rest-sentinel"))
	require.NoError(t, e)
	ciphertext, e := json.Marshal(envelope)
	require.NoError(t, e)
	deadline := clock.Now().Add(time.Minute)
	r, e = s.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthPending, FlowID: "flow", ExpiresAt: deadline, Secret: &core.UserAuthSecret{Version: 1, KeyID: "test", Ciphertext: ciphertext}})
	require.NoError(t, e)
	prompt := core.NewDelivery(core.DeliverySource{Kind: core.SourceProgress, ID: l.Turn.ID}, 1, "user-auth", b.ID, core.ReplyTarget{Mode: "reply", ChatID: "chat", MessageID: "msg"}, core.TextBody("https://example.test/?code=url-at-rest-sentinel"))
	require.NoError(t, s.WaitForUserAuth(ctx, l.Turn.Lease, core.UserAuthWait{Lease: r.Lease, Revision: r.Revision, Principal: p, FlowID: "flow", DeadlineAt: deadline, Until: clock.Now().Add(time.Second), Prompt: &prompt}))
	require.NoError(t, s.Close())
	disk, e := os.ReadFile(path)
	require.NoError(t, e)
	require.NotContains(t, string(disk), "device-token-at-rest-sentinel")
	require.NotContains(t, string(disk), "url-at-rest-sentinel")
	s, e = open(path, opts)
	require.NoError(t, e)
	defer func() { require.NoError(t, s.Close()) }()
	calls := c.Calls.Load()
	c.Fail.Store(true)
	saved, e := s.GetUserAuth(ctx, p)
	require.NoError(t, e)
	require.Equal(t, r.Revision+1, saved.Revision)
	require.Equal(t, ciphertext, saved.Secret.Ciphertext)
	require.Equal(t, calls, c.Calls.Load())
	c.Fail.Store(false)
	turn, e := s.GetTurn(ctx, l.Turn.ID)
	require.NoError(t, e)
	require.True(t, deadline.Equal(turn.AuthWait.DeadlineAt))
	require.Equal(t, core.WorkKindUserAuth, turn.WorkKind)
}
