// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
)

// Removing Interaction payload protection must expose the sentinels or break restart recovery.
func TestEncryptedInteractionLifecycle(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "interaction.db")
	codec := newPayloadTestCodec(t)
	options := Options{PayloadCodec: codec, PayloadNamespace: "interaction", RequirePayloadEncryption: true}
	db := openSQLite(t, path)
	s, err := openInstalledSQLFixture(ctx, db, SQLite, options)
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	_, err = s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("approval"))
	require.NoError(t, err)
	lease, err := s.LeaseTurn(ctx, conformance.Claim("worker"))
	require.NoError(t, err)
	input := core.Interaction{ID: core.InteractionID(lease.Turn.ID, "tool"), TurnID: lease.Turn.ID, Kind: core.InteractionApproval, Status: core.InteractionPending, ToolUseID: "tool", ApproverID: "user", ConfirmEventID: "confirm", BindingID: lease.Turn.BindingID, AppID: "app", TenantKey: "tenant", ExpiresAt: time.Now().Add(time.Hour), InputPreview: "PRIVATE_APPROVAL_PREVIEW_SENTINEL"}
	stored, err := s.PutInteraction(ctx, lease.Turn.Lease, input)
	require.NoError(t, err)
	require.Equal(t, input.InputPreview, stored.InputPreview)
	readRaw := func() string {
		var raw string
		require.NoError(t, db.QueryRow("SELECT data FROM channel_interactions WHERE id=?", input.ID).Scan(&raw))
		return raw
	}
	require.NotContains(t, readRaw(), input.InputPreview)
	stored.Prompted = true
	stored, err = s.PutInteraction(ctx, lease.Turn.Lease, stored)
	require.NoError(t, err)
	require.True(t, stored.Prompted)
	require.Equal(t, input.InputPreview, stored.InputPreview)
	changed := input
	changed.InputPreview = "different"
	_, err = s.PutInteraction(ctx, lease.Turn.Lease, changed)
	require.ErrorIs(t, err, core.ErrConflict)
	decision := core.InteractionDecision{DeciderID: "user", DenyReason: "PRIVATE_DENY_REASON_SENTINEL"}
	stored, err = s.DecideInteraction(ctx, input.ID, decision)
	require.NoError(t, err)
	require.Equal(t, core.InteractionDenied, stored.Status)
	require.Equal(t, decision.DenyReason, stored.DenyReason)
	require.Equal(t, input.InputPreview, stored.InputPreview)
	require.NotContains(t, readRaw(), decision.DenyReason)
	require.NotContains(t, readRaw(), input.InputPreview)
	exported, err := s.Export(ctx)
	require.NoError(t, err)
	require.Len(t, exported.Interactions, 1)
	require.Equal(t, decision.DenyReason, exported.Interactions[0].DenyReason)
	require.NoError(t, s.Close())
	db = openSQLite(t, path)
	s, err = open(ctx, db, SQLite, options)
	require.NoError(t, err)
	got, err := s.GetInteraction(ctx, input.ID)
	require.NoError(t, err)
	require.Equal(t, input.InputPreview, got.InputPreview)
	require.Equal(t, decision.DenyReason, got.DenyReason)
	again, err := s.DecideInteraction(ctx, input.ID, core.InteractionDecision{DeciderID: "user"})
	require.NoError(t, err)
	require.Equal(t, decision.DenyReason, again.DenyReason)
	// The persisted identity must still be checked after reopening.
	var wire map[string]any
	require.NoError(t, json.Unmarshal([]byte(readRaw()), &wire))
	wire["_payloads"].(map[string]any)["owner"] = "forged-owner"
	bad, err := json.Marshal(wire)
	require.NoError(t, err)
	_, err = db.Exec("UPDATE channel_interactions SET data=? WHERE id=?", string(bad), input.ID)
	require.NoError(t, err)
	_, err = s.GetInteraction(ctx, input.ID)
	require.Error(t, err)
}

type interactionReadCodec struct {
	codec.Codec
	failReason bool
}

func (c *interactionReadCodec) Open(ctx context.Context, scope codec.Scope, envelope codec.Envelope) ([]byte, error) {
	if c.failReason && scope.RecordKind == "interaction" && strings.HasPrefix(scope.Field, "reason:") {
		return nil, errors.New("fixture reason unavailable")
	}
	return c.Codec.Open(ctx, scope, envelope)
}
func TestEncryptedInteractionReadFailureReturnsNoData(t *testing.T) {
	for _, mode := range []string{"plaintext", "partial-open"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			path := filepath.Join(t.TempDir(), "failure.db")
			c := &interactionReadCodec{Codec: newPayloadTestCodec(t)}
			db := openSQLite(t, path)
			s, err := openInstalledSQLFixture(ctx, db, SQLite, Options{PayloadCodec: c, PayloadNamespace: "ix-fail", RequirePayloadEncryption: true})
			require.NoError(t, err)
			defer func() { require.NoError(t, s.Close()) }()
			_, err = s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("failure"))
			require.NoError(t, err)
			lease, err := s.LeaseTurn(ctx, conformance.Claim("worker"))
			require.NoError(t, err)
			input := core.Interaction{ID: core.InteractionID(lease.Turn.ID, "tool"), TurnID: lease.Turn.ID, Kind: core.InteractionApproval, Status: core.InteractionPending, ToolUseID: "tool", ApproverID: "user", ConfirmEventID: "confirm", BindingID: lease.Turn.BindingID, AppID: "app", TenantKey: "tenant", ExpiresAt: time.Now().Add(time.Hour), InputPreview: "PRIVATE_PARTIAL_PREVIEW_SENTINEL"}
			_, err = s.PutInteraction(ctx, lease.Turn.Lease, input)
			require.NoError(t, err)
			if mode == "partial-open" {
				c.failReason = true
			} else {
				raw, err := core.EncodeStored(input)
				require.NoError(t, err)
				_, err = db.Exec("UPDATE channel_interactions SET data=? WHERE id=?", string(raw), input.ID)
				require.NoError(t, err)
			}
			got, err := s.GetInteraction(ctx, input.ID)
			require.Error(t, err)
			require.Nil(t, got, "failed reads must not return raw or partly decrypted sensitive fields")
			exported, err := s.Export(ctx)
			require.Error(t, err)
			require.Equal(t, core.Export{}, exported)
		})
	}
}
