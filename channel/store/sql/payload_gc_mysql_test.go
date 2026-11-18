// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

//go:build mysqlintegration

package sqlstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
)

func TestMySQLPayloadMaintenanceGate(t *testing.T) {
	ctx := t.Context()
	db := mysqlTestDB(t)
	s, err := openInstalledSQLFixture(ctx, db, MySQL, Options{
		TablePrefix:  fmt.Sprintf("gc%d_", time.Now().UnixNano()),
		PayloadCodec: newPayloadTestCodec(t), PayloadNamespace: "gc", RequirePayloadEncryption: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()

	// A writer holding a shared gate must not serialize unrelated encrypted writers.
	writer, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = writer.Rollback() }()
	var mode string
	require.NoError(t, writer.QueryRowContext(ctx, "SELECT v FROM "+s.t.meta+" WHERE k='payload_mode' LOCK IN SHARE MODE").Scan(&mode))
	bounded, cancel := context.WithTimeout(ctx, time.Second)
	_, err = s.Ingest(bounded, conformance.SampleVersion(), conformance.SampleMessage("live"))
	cancel()
	require.NoError(t, err)

	// Collection must wait until the writer is done before it scans any roots.
	bounded, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
	err = s.prunePayloads(bounded)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, writer.Rollback())
	require.NoError(t, s.prunePayloads(ctx))
	duplicate, err := s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("live"))
	require.NoError(t, err)
	require.True(t, duplicate.Duplicate)

	// The exclusive maintenance gate blocks writers before domain rows are touched.
	collector, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = collector.Rollback() }()
	require.NoError(t, collector.QueryRowContext(ctx, "SELECT v FROM "+s.t.meta+" WHERE k='payload_mode' FOR UPDATE").Scan(&mode))
	bounded, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
	_, err = s.Ingest(bounded, conformance.SampleVersion(), conformance.SampleMessage("blocked"))
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, collector.Rollback())
	_, err = s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("blocked"))
	require.NoError(t, err)
}

type interactionPrepareCodec struct {
	codec.Codec
	beforeSeal func(codec.Scope)
}

func (c *interactionPrepareCodec) Seal(ctx context.Context, scope codec.Scope, plain []byte) (codec.Envelope, error) {
	if c.beforeSeal != nil {
		c.beforeSeal(scope)
	}
	return c.Codec.Seal(ctx, scope, plain)
}

func TestMySQLSettlementPreservesCommittedInteractionDecision(t *testing.T) {
	ctx := t.Context()
	db := mysqlTestDB(t)
	c := &interactionPrepareCodec{Codec: newPayloadTestCodec(t)}
	prefix := fmt.Sprintf("ixsettle%d_", time.Now().UnixNano())
	s, err := openInstalledSQLFixture(ctx, db, MySQL, Options{TablePrefix: prefix, PayloadCodec: c, PayloadNamespace: "ixsettle", RequirePayloadEncryption: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	_, err = s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("settle"))
	require.NoError(t, err)
	lease, err := s.LeaseTurn(ctx, conformance.Claim("worker"))
	require.NoError(t, err)
	input := core.Interaction{ID: core.InteractionID(lease.Turn.ID, "tool"), TurnID: lease.Turn.ID, Kind: core.InteractionApproval, Status: core.InteractionPending, ToolUseID: "tool", ApproverID: "user", ConfirmEventID: "confirm", BindingID: lease.Turn.BindingID, AppID: "app", TenantKey: "tenant", ExpiresAt: time.Now().Add(time.Hour), InputPreview: "private"}
	_, err = s.PutInteraction(ctx, lease.Turn.Lease, input)
	require.NoError(t, err)
	prepared := make(chan struct{}, 2)
	allow, expire := make(chan struct{}), make(chan struct{})
	release := func(ch chan struct{}) {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
	defer release(allow)
	defer release(expire)
	c.beforeSeal = func(scope codec.Scope) {
		switch scope.Field {
		case "reason:allowed":
			prepared <- struct{}{}
			<-allow
		case "reason:expired":
			prepared <- struct{}{}
			<-expire
		}
	}
	decided, settled := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := s.DecideInteraction(ctx, input.ID, core.InteractionDecision{Allow: true, DeciderID: "user"})
		decided <- err
	}()
	go func() {
		settled <- s.SettleTurn(ctx, lease.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, nil)
	}()
	for range 2 {
		select {
		case <-prepared:
		case <-time.After(5 * time.Second):
			t.Fatal("both writers must prepare outside their transactions")
		}
	}
	locked, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = locked.Rollback() }()
	var raw string
	require.NoError(t, locked.QueryRowContext(ctx, "SELECT data FROM "+s.t.interactions+" WHERE id=? FOR UPDATE", input.ID).Scan(&raw))
	waitForLocks := func(want int) {
		require.Eventually(t, func() bool {
			var n int
			err := db.QueryRowContext(ctx, "SELECT count(DISTINCT w.REQUESTING_ENGINE_TRANSACTION_ID) FROM performance_schema.data_lock_waits w JOIN performance_schema.data_locks l ON l.ENGINE_LOCK_ID=w.REQUESTING_ENGINE_LOCK_ID WHERE l.OBJECT_NAME LIKE ?", prefix+"%").Scan(&n)
			return err == nil && n >= want
		}, 5*time.Second, 10*time.Millisecond)
	}
	release(allow)
	waitForLocks(1) // Callback queues for the decision row first.
	release(expire)
	waitForLocks(2)
	require.NoError(t, locked.Rollback())
	require.NoError(t, <-decided)
	require.NoError(t, <-settled)
	got, err := s.GetInteraction(ctx, input.ID)
	require.NoError(t, err)
	require.Equal(t, core.InteractionAllowed, got.Status)
}

func TestMySQLEncryptedOppositeInteractionDecisionsAreSerialized(t *testing.T) {
	ctx := t.Context()
	db := mysqlTestDB(t)
	c := newPayloadTestCodec(t)
	prefix := fmt.Sprintf("ixcas%d_", time.Now().UnixNano())
	s, err := openInstalledSQLFixture(ctx, db, MySQL, Options{TablePrefix: prefix, PayloadCodec: c, PayloadNamespace: "ixcas", RequirePayloadEncryption: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	_, err = s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("cas"))
	require.NoError(t, err)
	lease, err := s.LeaseTurn(ctx, conformance.Claim("worker"))
	require.NoError(t, err)
	input := core.Interaction{ID: core.InteractionID(lease.Turn.ID, "tool"), TurnID: lease.Turn.ID, Kind: core.InteractionApproval, Status: core.InteractionPending, ToolUseID: "tool", ApproverID: "user", ConfirmEventID: "confirm", BindingID: lease.Turn.BindingID, AppID: "app", TenantKey: "tenant", ExpiresAt: time.Now().Add(time.Hour), InputPreview: "private"}
	_, err = s.PutInteraction(ctx, lease.Turn.Lease, input)
	require.NoError(t, err)

	// Both decisions prepare their different immutable reason payloads outside
	// transactions. Hold the decision row before either replay can proceed.
	prepared, release := make(chan struct{}, 2), make(chan struct{})
	c.beforeSeal = func() { prepared <- struct{}{}; <-release }
	results := make(chan error, 2)
	for _, allow := range []bool{true, false} {
		go func(allow bool) {
			_, err := s.DecideInteraction(ctx, input.ID, core.InteractionDecision{DeciderID: "user", Allow: allow, DenyReason: "reason"})
			results <- err
		}(allow)
	}
	for range 2 {
		select {
		case <-prepared:
		case <-time.After(5 * time.Second):
			t.Fatal("decisions did not reach transaction-free preparation")
		}
	}
	locked, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = locked.Rollback() }()
	var raw string
	require.NoError(t, locked.QueryRowContext(ctx, "SELECT data FROM "+s.t.interactions+" WHERE id=? FOR UPDATE", input.ID).Scan(&raw))
	close(release)
	require.Eventually(t, func() bool {
		var n int
		err := db.QueryRowContext(ctx, "SELECT count(DISTINCT w.REQUESTING_ENGINE_TRANSACTION_ID) FROM performance_schema.data_lock_waits w JOIN performance_schema.data_locks l ON l.ENGINE_LOCK_ID=w.REQUESTING_ENGINE_LOCK_ID WHERE l.OBJECT_NAME LIKE ?", prefix+"%").Scan(&n)
		return err == nil && (n >= 2 || len(results) > 0)
	}, 5*time.Second, 10*time.Millisecond, "both replays must be waiting before the row is unlocked")
	require.NoError(t, locked.Rollback())
	winners := 0
	for range 2 {
		err := <-results
		if err == nil {
			winners++
		} else {
			require.ErrorIs(t, err, core.ErrConflict)
		}
	}
	require.Equal(t, 1, winners, "opposite decisions cannot both succeed")
}
