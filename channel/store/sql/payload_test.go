// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

type payloadTestCodec struct {
	seals      atomic.Int64
	opens      atomic.Int64
	beforeSeal func()
	aead       cipher.AEAD
}

func newPayloadTestCodec(t *testing.T) *payloadTestCodec {
	t.Helper()
	key := make([]byte, 32)
	_, e := rand.Read(key)
	require.NoError(t, e)
	b, e := aes.NewCipher(key)
	require.NoError(t, e)
	a, e := cipher.NewGCM(b)
	require.NoError(t, e)
	return &payloadTestCodec{aead: a}
}
func (c *payloadTestCodec) Seal(_ context.Context, s codec.Scope, p []byte) (codec.Envelope, error) {
	c.seals.Add(1)
	if c.beforeSeal != nil {
		c.beforeSeal()
	}
	n := make([]byte, c.aead.NonceSize())
	if _, e := rand.Read(n); e != nil {
		return codec.Envelope{}, e
	}
	aad, e := codec.CanonicalScope(s)
	if e != nil {
		return codec.Envelope{}, e
	}
	return codec.Envelope{Version: 1, KeyID: "test", Ciphertext: c.aead.Seal(n, n, p, aad)}, nil
}
func (c *payloadTestCodec) Open(_ context.Context, s codec.Scope, e codec.Envelope) ([]byte, error) {
	c.opens.Add(1)
	aad, err := codec.CanonicalScope(s)
	if err != nil {
		return nil, err
	}
	return c.aead.Open(nil, e.Ciphertext[:c.aead.NonceSize()], e.Ciphertext[c.aead.NonceSize():], aad)
}
func TestSQLitePayloadEncryptionAndMetadataRenewal(t *testing.T) {
	ctx := context.Background()
	db := openSQLite(t, filepath.Join(t.TempDir(), "protected.db"))
	c := newPayloadTestCodec(t)
	s, e := openInstalledSQLFixture(ctx, db, SQLite, Options{PayloadCodec: c, PayloadNamespace: "deployment", RequirePayloadEncryption: true})
	require.NoError(t, e)
	defer func() { require.NoError(t, s.Close()) }()
	c.beforeSeal = func() {
		callCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_, err := db.ExecContext(callCtx, "UPDATE channel_meta SET v=v WHERE k='payload_mode'")
		require.NoError(t, err, "Seal must run without a SQLite write transaction")
	}
	m := conformance.SampleMessage("protected")
	m.Text = "sensitive-inbound-sentinel"
	r, e := s.Ingest(ctx, conformance.SampleVersion(), m)
	require.NoError(t, e)
	var raw string
	require.NoError(t, db.QueryRow("SELECT data FROM channel_messages WHERE id=?", r.Message.ID).Scan(&raw))
	require.NotContains(t, raw, m.Text)
	require.Contains(t, raw, "_payloads")
	claimed, e := s.LeaseTurn(ctx, conformance.Claim("worker"))
	require.NoError(t, e)
	require.Equal(t, m.Text, claimed.Message.Message.Text)
	beforeSeal, beforeOpen := c.seals.Load(), c.opens.Load()
	_, e = s.RenewTurn(ctx, claimed.Turn.Lease, time.Minute)
	require.NoError(t, e)
	require.Equal(t, beforeSeal, c.seals.Load())
	require.Equal(t, beforeOpen, c.opens.Load(), "metadata renewal must not decrypt unrelated input")
	duplicate, e := s.Ingest(ctx, conformance.SampleVersion(), m)
	require.NoError(t, e)
	require.True(t, duplicate.Duplicate)
	require.Equal(t, m.Text, duplicate.Message.Message.Text)
	require.Equal(t, core.ContentHash(m), duplicate.Message.ContentHash)
}
func TestSQLiteRequiredPayloadCodecCannotSilentlyUsePlaintext(t *testing.T) {
	db := openSQLite(t, filepath.Join(t.TempDir(), "missing.db"))
	defer func() { require.NoError(t, db.Close()) }()
	_, e := openInstalledSQLFixture(context.Background(), db, SQLite, Options{RequirePayloadEncryption: true, PayloadNamespace: "deployment"})
	require.ErrorIs(t, e, core.ErrInvalid)
}

func TestSQLiteEncryptedStoreConformance(t *testing.T) {
	conformance.RunStorePayload(t, func(t *testing.T, cfg conformance.StoreConfig) conformance.TestStore {
		db := openSQLite(t, filepath.Join(t.TempDir(), "store.db"))
		s, e := openInstalledSQLFixture(context.Background(), db, SQLite, Options{Clock: cfg.Clock, MaxOpenTurns: cfg.MaxOpenTurns, MaxMessageBytes: cfg.MaxMessageBytes, QueueTTL: cfg.QueueTTL, PayloadCodec: newPayloadTestCodec(t), PayloadNamespace: "deployment", RequirePayloadEncryption: true})
		require.NoError(t, e)
		return s
	})
}

func TestSQLitePayloadFrozenInputSnapshotAndDelivery(t *testing.T) {
	ctx := context.Background()
	db := openSQLite(t, filepath.Join(t.TempDir(), "all-fields.db"))
	c := newPayloadTestCodec(t)
	s, e := openInstalledSQLFixture(ctx, db, SQLite, Options{PayloadCodec: c, PayloadNamespace: "deployment", RequirePayloadEncryption: true})
	require.NoError(t, e)
	defer func() { require.NoError(t, s.Close()) }()
	_, e = s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("frozen"))
	require.NoError(t, e)
	lease, e := s.LeaseTurn(ctx, conformance.Claim("worker"))
	require.NoError(t, e)
	input := core.FrozenInput{ContextFrozen: true, Context: "frozen-context-sentinel", BlocksFrozen: true, Blocks: []core.InputBlock{{Type: "document", FileID: "file-id", Title: "frozen-filename-sentinel"}}, MountsFrozen: true, Mounts: []core.MountedFile{{FileID: "file-id", ActualPath: "file:///mnt/session/uploads/mount-path-sentinel"}}}
	turn, e := s.UpdateTurn(ctx, lease.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "creation"}, Input: input})
	require.NoError(t, e)
	spec := core.CreateSessionSpec{Binding: conformance.SampleBinding(), CreationKey: "creation", LookupKey: "lookup", EnvironmentOverride: &core.EnvironmentOverride{Type: "cloud", Env: map[string]string{"TOKEN": "environment-secret-sentinel"}}}
	original := core.NewSessionSnapshot(turn.ConversationID, 1, spec)
	snapshot, e := s.FreezeSession(ctx, turn.Lease, original)
	require.NoError(t, e)
	require.Equal(t, original.RequestHash, snapshot.RequestHash)
	got, e := s.GetSessionSnapshot(ctx, snapshot.ID)
	require.NoError(t, e)
	require.True(t, got.Valid())
	require.Equal(t, spec.EnvironmentOverride, got.Spec.EnvironmentOverride)
	gotTurn, e := s.GetTurn(ctx, turn.ID)
	require.NoError(t, e)
	require.Equal(t, input.Context, gotTurn.Input.Context)
	require.Equal(t, input.Blocks, gotTurn.Input.Blocks)
	require.Equal(t, input.Mounts, gotTurn.Input.Mounts)
	beforeSeal := c.seals.Load()
	_, e = s.UpdateTurn(ctx, turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Input: input})
	require.NoError(t, e)
	require.Equal(t, beforeSeal, c.seals.Load(), "same frozen input must reuse immutable payloads")
	_, e = s.UpdateTurn(ctx, turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Input: core.FrozenInput{ContextFrozen: true, Context: "different"}})
	require.ErrorIs(t, e, core.ErrConflict)
	source := core.DeliverySource{Kind: core.SourceTurn, ID: turn.ID}
	d := core.NewDelivery(source, 0, "answer", turn.BindingID, core.ReplyTarget{ChatID: "chat"}, core.TextBody("answer-secret-sentinel"))
	d.Snapshot = turn.Snapshot
	require.NoError(t, s.SettleTurn(ctx, turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{d}))
	delivery, e := s.LeaseDelivery(ctx, "delivery", time.Minute)
	require.NoError(t, e)
	require.Equal(t, d.Body, delivery.Body)
	beforeSeal, beforeOpen := c.seals.Load(), c.opens.Load()
	_, e = s.RenewDelivery(ctx, delivery.Lease, time.Minute)
	require.NoError(t, e)
	require.Equal(t, beforeSeal, c.seals.Load())
	require.Equal(t, beforeOpen, c.opens.Load())
	for _, table := range []string{"channel_turns", "channel_session_snapshots", "channel_deliveries", "channel_sensitive_payloads"} {
		rows, err := db.Query("SELECT data FROM " + table)
		require.NoError(t, err)
		for rows.Next() {
			var raw string
			require.NoError(t, rows.Scan(&raw))
			for _, secret := range []string{input.Context, input.Blocks[0].Title, input.Mounts[0].ActualPath, "environment-secret-sentinel", d.Body.Text} {
				require.NotContains(t, raw, secret)
			}
		}
		require.NoError(t, rows.Close())
	}
}

func TestSQLitePayloadRequiredReadRejectsInsertedPlaintext(t *testing.T) {
	ctx := context.Background()
	db := openSQLite(t, filepath.Join(t.TempDir(), "fail-closed.db"))
	s, e := openInstalledSQLFixture(ctx, db, SQLite, Options{PayloadCodec: newPayloadTestCodec(t), PayloadNamespace: "deployment", RequirePayloadEncryption: true})
	require.NoError(t, e)
	defer func() { require.NoError(t, s.Close()) }()
	m := conformance.SampleMessage("fail-closed")
	first, e := s.Ingest(ctx, conformance.SampleVersion(), m)
	require.NoError(t, e)
	var meta map[string]any
	var raw string
	require.NoError(t, db.QueryRow("SELECT data FROM channel_turns WHERE id=?", first.Turn.ID).Scan(&raw))
	require.NoError(t, json.Unmarshal([]byte(raw), &meta))
	delete(meta, "_payloads")
	corrupt, e := json.Marshal(meta)
	require.NoError(t, e)
	_, e = db.Exec("UPDATE channel_turns SET data=? WHERE id=?", string(corrupt), first.Turn.ID)
	require.NoError(t, e)
	_, e = s.GetTurn(ctx, first.Turn.ID)
	require.Error(t, e, "required codec must never return an unprotected persisted record")
}

func TestSQLitePayloadReferenceAndCiphertextTamperingFailClosed(t *testing.T) {
	ctx := context.Background()
	db := openSQLite(t, filepath.Join(t.TempDir(), "tamper.db"))
	s, e := openInstalledSQLFixture(ctx, db, SQLite, Options{PayloadCodec: newPayloadTestCodec(t), PayloadNamespace: "deployment", RequirePayloadEncryption: true})
	require.NoError(t, e)
	defer func() { require.NoError(t, s.Close()) }()
	m := conformance.SampleMessage("tamper")
	first, e := s.Ingest(ctx, conformance.SampleVersion(), m)
	require.NoError(t, e)
	var id, raw string
	require.NoError(t, db.QueryRow("SELECT id,data FROM channel_sensitive_payloads LIMIT 1").Scan(&id, &raw))
	var record payload.Record
	require.NoError(t, json.Unmarshal([]byte(raw), &record))
	record.Envelope.Ciphertext[len(record.Envelope.Ciphertext)-1] ^= 1
	bad, e := json.Marshal(record)
	require.NoError(t, e)
	_, e = db.Exec("UPDATE channel_sensitive_payloads SET data=? WHERE id=?", string(bad), id)
	require.NoError(t, e)
	_, e = s.Ingest(ctx, conformance.SampleVersion(), m)
	require.Error(t, e, "duplicate must not ACK a failed authenticated open")
	_, e = db.Exec("UPDATE channel_sensitive_payloads SET data=? WHERE id=?", raw, id)
	require.NoError(t, e)
	require.NoError(t, db.QueryRow("SELECT data FROM channel_messages WHERE id=?", first.Message.ID).Scan(&raw))
	var meta core.InboundMessage
	require.NoError(t, json.Unmarshal([]byte(raw), &meta))
	delete(meta.Payloads.Refs, "content")
	bad, e = json.Marshal(meta)
	require.NoError(t, e)
	_, e = db.Exec("UPDATE channel_messages SET data=? WHERE id=?", string(bad), first.Message.ID)
	require.NoError(t, e)
	_, e = s.Ingest(ctx, conformance.SampleVersion(), m)
	require.Error(t, e, "missing required reference must not become empty successful input")
}

type rotatingPayloadCodec struct {
	keys   map[string]*payloadTestCodec
	active string
}

func (c *rotatingPayloadCodec) Seal(ctx context.Context, s codec.Scope, p []byte) (codec.Envelope, error) {
	e, err := c.keys[c.active].Seal(ctx, s, p)
	e.KeyID = c.active
	return e, err
}
func (c *rotatingPayloadCodec) Open(ctx context.Context, s codec.Scope, e codec.Envelope) ([]byte, error) {
	key := c.keys[e.KeyID]
	if key == nil {
		return nil, errors.New("unknown test key")
	}
	return key.Open(ctx, s, e)
}
func TestSQLitePayloadKeyRotationRetainsOldImmutableCiphertext(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "rotation.db")
	old, newKey := newPayloadTestCodec(t), newPayloadTestCodec(t)
	c := &rotatingPayloadCodec{keys: map[string]*payloadTestCodec{"old": old, "new": newKey}, active: "old"}
	options := Options{PayloadCodec: c, PayloadNamespace: "deployment", RequirePayloadEncryption: true}
	db := openSQLite(t, path)
	s, e := openInstalledSQLFixture(ctx, db, SQLite, options)
	require.NoError(t, e)
	m := conformance.SampleMessage("old")
	m.Text = "old-key-content"
	_, e = s.Ingest(ctx, conformance.SampleVersion(), m)
	require.NoError(t, e)
	var oldID, oldCipher string
	require.NoError(t, db.QueryRow("SELECT id,data FROM channel_sensitive_payloads LIMIT 1").Scan(&oldID, &oldCipher))
	require.NoError(t, s.Close())
	c.active = "new"
	db = openSQLite(t, path)
	s, e = openInstalledSQLFixture(ctx, db, SQLite, options)
	require.NoError(t, e)
	defer func() { require.NoError(t, s.Close()) }()
	got, e := s.Ingest(ctx, conformance.SampleVersion(), m)
	require.NoError(t, e)
	require.True(t, got.Duplicate)
	require.Equal(t, m.Text, got.Message.Message.Text)
	other := conformance.SampleMessage("new")
	other.ChatID = "other-chat"
	_, e = s.Ingest(ctx, conformance.SampleVersion(), other)
	require.NoError(t, e)
	var untouched string
	require.NoError(t, db.QueryRow("SELECT data FROM channel_sensitive_payloads WHERE id=?", oldID).Scan(&untouched))
	require.Equal(t, oldCipher, untouched)
	require.Equal(t, int64(1), old.seals.Load())
	require.Equal(t, int64(1), newKey.seals.Load())
}

func TestSQLitePayloadPruneRetainsPendingControlProvenance(t *testing.T) {
	ctx := context.Background()
	clock := conformance.NewClock(time.Now().UTC())
	db := openSQLite(t, filepath.Join(t.TempDir(), "pending-control.db"))
	s, e := openInstalledSQLFixture(ctx, db, SQLite, Options{Clock: clock.Now, PayloadCodec: newPayloadTestCodec(t), PayloadNamespace: "deployment", RequirePayloadEncryption: true})
	require.NoError(t, e)
	defer func() { require.NoError(t, s.Close()) }()
	message := conformance.StopMessage("pending")
	first, e := s.Ingest(ctx, conformance.SampleVersion(), message)
	require.NoError(t, e)
	require.NotNil(t, first.Reply)
	clock.Advance(time.Hour)
	_, e = s.Prune(ctx, clock.Now())
	require.NoError(t, e)
	duplicate, e := s.Ingest(ctx, conformance.SampleVersion(), message)
	require.NoError(t, e, "pending control reply keeps the exact message binding provenance")
	require.True(t, duplicate.Duplicate)
	deliveries, err := s.ListDeliveries(ctx, core.DeliveryQuery{OpenOnly: true})
	require.NoError(t, err)
	require.Len(t, deliveries, 1)
	require.Equal(t, first.Reply.ID, deliveries[0].ID)
}

func TestSQLitePayloadExportReturnsLogicalContent(t *testing.T) {
	ctx := context.Background()
	s, e := openInstalledSQLFixture(ctx, openSQLite(t, filepath.Join(t.TempDir(), "export.db")), SQLite, Options{PayloadCodec: newPayloadTestCodec(t), PayloadNamespace: "deployment", RequirePayloadEncryption: true})
	require.NoError(t, e)
	defer func() { require.NoError(t, s.Close()) }()
	m := conformance.SampleMessage("export")
	_, e = s.Ingest(ctx, conformance.SampleVersion(), m)
	require.NoError(t, e)
	export, e := s.Export(ctx)
	require.NoError(t, e)
	require.Len(t, export.Messages, 1)
	require.Equal(t, m.Text, export.Messages[0].Message.Text)
}

func TestSQLitePayloadReadRejectsMissingStrongReference(t *testing.T) {
	ctx := context.Background()
	db := openSQLite(t, filepath.Join(t.TempDir(), "missing-link.db"))
	s, e := openInstalledSQLFixture(ctx, db, SQLite, Options{PayloadCodec: newPayloadTestCodec(t), PayloadNamespace: "deployment", RequirePayloadEncryption: true})
	require.NoError(t, e)
	defer func() { require.NoError(t, s.Close()) }()
	m := conformance.SampleMessage("link")
	r, e := s.Ingest(ctx, conformance.SampleVersion(), m)
	require.NoError(t, e)
	_, e = db.Exec("DELETE FROM channel_payload_refs WHERE record_kind='message' AND record_id=?", r.Message.ID)
	require.NoError(t, e)
	_, e = s.Ingest(ctx, conformance.SampleVersion(), m)
	require.Error(t, e, "authenticated ciphertext alone cannot substitute for a missing retention reference")
}
