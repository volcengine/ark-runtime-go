// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

type payloadTestCodec struct {
	mu                 sync.Mutex
	seals, opens       int
	sealHook, openHook func() error
	fail               bool
}

func (c *payloadTestCodec) Seal(_ context.Context, s codec.Scope, b []byte) (codec.Envelope, error) {
	c.mu.Lock()
	c.seals++
	hook, fail := c.sealHook, c.fail
	c.mu.Unlock()
	if fail {
		return codec.Envelope{}, errors.New("kms unavailable")
	}
	if hook != nil {
		if err := hook(); err != nil {
			return codec.Envelope{}, err
		}
	}
	block, _ := aes.NewCipher(bytes.Repeat([]byte{7}, 32))
	aead, _ := cipher.NewGCM(block)
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return codec.Envelope{}, err
	}
	aad, err := codec.CanonicalScope(s)
	if err != nil {
		return codec.Envelope{}, err
	}
	return codec.Envelope{Version: 1, KeyID: "test", Metadata: nonce, Ciphertext: aead.Seal(nil, nonce, b, aad)}, nil
}
func (c *payloadTestCodec) Open(_ context.Context, s codec.Scope, e codec.Envelope) ([]byte, error) {
	c.mu.Lock()
	c.opens++
	hook, fail := c.openHook, c.fail
	c.mu.Unlock()
	if fail {
		return nil, errors.New("kms unavailable")
	}
	if hook != nil {
		if err := hook(); err != nil {
			return nil, err
		}
	}
	block, _ := aes.NewCipher(bytes.Repeat([]byte{7}, 32))
	aead, _ := cipher.NewGCM(block)
	aad, err := codec.CanonicalScope(s)
	if err != nil {
		return nil, err
	}
	return aead.Open(nil, e.Metadata, e.Ciphertext, aad)
}
func (c *payloadTestCodec) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seals, c.opens
}

func TestEncryptedEmbeddedPayloadRoundTripAndLeaseDoesNotCallKMS(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "encrypted.db")
	c := &payloadTestCodec{}
	clock := conformance.NewClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	s, err := openInstalledFixture(path, Options{Clock: clock.Now, NoSync: true, PayloadCodec: c, PayloadNamespace: "test", RequirePayloadEncryption: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	c.openHook = func() error {
		if s.db.Stats().OpenTxN != 0 {
			return errors.New("open called inside read transaction")
		}
		return nil
	}
	m := conformance.SampleMessage("sensitive")
	m.Text = "unique-private-message"
	m.Attachments = []core.Attachment{{Kind: "file", Key: "unique-private-attachment"}}
	in, err := s.Ingest(ctx, conformance.SampleVersion(), m)
	require.NoError(t, err)
	require.Equal(t, core.ContentHash(m), in.Message.ContentHash)
	l, err := s.LeaseTurn(ctx, conformance.Claim("worker"))
	require.NoError(t, err)
	require.Equal(t, m.Text, l.Message.Message.Text)
	_, err = s.UpdateTurn(ctx, l.Turn.Lease, core.TurnUpdate{Phase: core.PhaseNone, Input: core.FrozenInput{ContextFrozen: true, Context: "unique-private-context"}})
	require.NoError(t, err)
	_, err = s.UpdateTurn(ctx, l.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "create"}, Input: core.FrozenInput{MountsFrozen: true, Mounts: []core.MountedFile{{FileID: "file", ActualPath: "file:///mnt/session/uploads/unique-private-path"}}}})
	require.NoError(t, err)
	gotTurn, err := s.GetTurn(ctx, l.Turn.ID)
	require.NoError(t, err)
	require.Equal(t, "file:///mnt/session/uploads/unique-private-path", gotTurn.Input.Mounts[0].ActualPath)
	snapshot := core.NewSessionSnapshot(l.Turn.ConversationID, 1, core.CreateSessionSpec{Binding: conformance.SampleBinding(), CreationKey: "create", LookupKey: "lookup", ChatType: "group", EnvironmentOverride: &core.EnvironmentOverride{Type: "sandbox", Env: map[string]string{"PRIVATE": "unique-private-env"}}})
	frozen, err := s.FreezeSession(ctx, l.Turn.Lease, snapshot)
	require.NoError(t, err)
	require.Equal(t, snapshot.RequestHash, frozen.RequestHash)
	gotSnapshot, err := s.GetSessionSnapshot(ctx, snapshot.ID)
	require.NoError(t, err)
	require.Equal(t, "unique-private-env", gotSnapshot.Spec.EnvironmentOverride.Env["PRIVATE"])
	beforeSeal, beforeOpen := c.counts()
	_, err = s.RenewTurn(ctx, l.Turn.Lease, time.Minute)
	require.NoError(t, err)
	afterSeal, afterOpen := c.counts()
	require.Equal(t, beforeSeal, afterSeal)
	require.Equal(t, beforeOpen, afterOpen)
	d := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: l.Turn.ID}, 0, "answer", l.Turn.BindingID, core.ReplyTarget{ChatID: "chat", Mode: "reply"}, core.TextBody("unique-private-answer"))
	require.NoError(t, s.SettleTurn(ctx, l.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{d}))
	delivery, err := s.LeaseDelivery(ctx, "sender", time.Minute)
	require.NoError(t, err)
	require.Equal(t, "unique-private-answer", delivery.Body.Text)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	for _, secret := range []string{m.Text, m.Attachments[0].Key, "unique-private-context", "unique-private-env", "unique-private-answer", "unique-private-path"} {
		require.False(t, bytes.Contains(raw, []byte(secret)), "plaintext sentinel leaked: %s", secret)
	}
	require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(bucketMessages).Get([]byte(in.Message.ID))
		require.True(t, json.Valid(data))
		require.NotContains(t, string(data), m.Text)
		require.NotNil(t, tx.Bucket(bucketPayloads))
		require.NotNil(t, tx.Bucket(bucketPayloadRefs))
		return nil
	}))
}

func TestEmbeddedPayloadPolicyMismatchDoesNotModifyPlainDatabase(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "plain.db")
	s, err := openInstalledFixture(path, Options{NoSync: true})
	require.NoError(t, err)
	m := conformance.SampleMessage("plain")
	_, err = s.Ingest(ctx, conformance.SampleVersion(), m)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	c := &payloadTestCodec{}
	_, err = open(path, Options{NoSync: true, PayloadCodec: c, PayloadNamespace: "test", RequirePayloadEncryption: true})
	require.ErrorIs(t, err, payload.ErrPolicyMismatch)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	seals, opens := c.counts()
	require.Zero(t, seals)
	require.Zero(t, opens)
	s, err = open(path, Options{NoSync: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	l, err := s.LeaseTurn(ctx, conformance.Claim("worker"))
	require.NoError(t, err)
	require.Equal(t, m.Text, l.Message.Message.Text)
}

func TestEncryptedEmbeddedConformance(t *testing.T) {
	conformance.RunStorePayload(t, func(t *testing.T, config conformance.StoreConfig) conformance.TestStore {
		s, err := openInstalledFixture(filepath.Join(t.TempDir(), "encrypted-conformance.db"), Options{Clock: config.Clock, MaxOpenTurns: config.MaxOpenTurns, MaxMessageBytes: config.MaxMessageBytes, QueueTTL: config.QueueTTL, NoSync: true, PayloadCodec: &payloadTestCodec{}, PayloadNamespace: "conformance", RequirePayloadEncryption: true})
		require.NoError(t, err)
		return s
	})
}

func TestPayloadCodecConfiguration(t *testing.T) {
	for _, options := range []Options{{RequirePayloadEncryption: true}, {PayloadCodec: &payloadTestCodec{}}} {
		s, err := openInstalledFixture(filepath.Join(t.TempDir(), "invalid.db"), options)
		if s != nil {
			require.NoError(t, s.Close())
		}
		require.ErrorIs(t, err, core.ErrInvalid)
	}
}

func TestPayloadPreparationDoesNotHoldWriteTransaction(t *testing.T) {
	ctx := context.Background()
	c := &payloadTestCodec{}
	entered, release := make(chan struct{}), make(chan struct{})
	var blocked atomic.Bool
	c.sealHook = func() error {
		if !blocked.Swap(true) {
			close(entered)
			<-release
		}
		return nil
	}
	s, err := openInstalledFixture(filepath.Join(t.TempDir(), "concurrent.db"), Options{NoSync: true, PayloadCodec: c, PayloadNamespace: "test", RequirePayloadEncryption: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	type result struct {
		r   core.IngestResult
		err error
	}
	first := make(chan result, 1)
	a := conformance.SampleMessage("a")
	a.ChatID = "a"
	go func() { r, err := s.Ingest(ctx, conformance.SampleVersion(), a); first <- result{r, err} }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first Seal did not start")
	}
	second := make(chan result, 1)
	b := conformance.SampleMessage("b")
	b.ChatID = "b"
	go func() { r, err := s.Ingest(ctx, conformance.SampleVersion(), b); second <- result{r, err} }()
	var rb result
	select {
	case rb = <-second:
		require.NoError(t, rb.err)
	case <-time.After(2 * time.Second):
		t.Fatal("another writer blocked behind Seal")
	}
	close(release)
	ra := <-first
	require.NoError(t, ra.err)
	require.Greater(t, ra.r.Message.Seq, rb.r.Message.Seq)
	require.Equal(t, payloadMessageID(a.BindingID, a.MessageID), ra.r.Message.ID)
	seals, _ := c.counts()
	require.Equal(t, 2, seals, "stable content IDs avoid extra Seal when transaction seq changes")
}

func TestEncryptedEmbeddedRemoteOperationConformance(t *testing.T) {
	factory := func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, err := openInstalledFixture(filepath.Join(t.TempDir(), "remote-encrypted.db"), Options{Clock: c.Clock, NoSync: true, PayloadCodec: &payloadTestCodec{}, PayloadNamespace: "remote-test", RequirePayloadEncryption: true})
		require.NoError(t, err)
		return s
	}
	conformance.RunRemoteOperationStore(t, factory)
	conformance.RunRemoteOperationFences(t, factory)
	conformance.RunRemoteCreationBinding(t, factory)
}

func TestPayloadFailureNeverFallsBackAndMetadataLeaseRemainsAvailable(t *testing.T) {
	ctx := context.Background()
	c := &payloadTestCodec{fail: true}
	s, err := openInstalledFixture(filepath.Join(t.TempDir(), "kms-failure.db"), Options{NoSync: true, PayloadCodec: c, PayloadNamespace: "test", RequirePayloadEncryption: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	_, err = s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("failed"))
	require.Error(t, err)
	require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
		require.Zero(t, tx.Bucket(bucketMessages).Stats().KeyN)
		require.Zero(t, tx.Bucket(bucketTurns).Stats().KeyN)
		require.Zero(t, tx.Bucket(bucketPayloads).Stats().KeyN)
		require.Zero(t, tx.Bucket(bucketPayloadRefs).Stats().KeyN)
		return nil
	}))
	c.mu.Lock()
	c.fail = false
	c.mu.Unlock()
	_, err = s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("ok"))
	require.NoError(t, err)
	l, err := s.LeaseTurn(ctx, conformance.Claim("worker"))
	require.NoError(t, err)
	c.mu.Lock()
	c.fail = true
	c.mu.Unlock()
	seals, opens := c.counts()
	_, err = s.RenewTurn(ctx, l.Turn.Lease, time.Minute)
	require.NoError(t, err)
	afterSeals, afterOpens := c.counts()
	require.Equal(t, seals, afterSeals)
	require.Equal(t, opens, afterOpens)
	_, err = s.Export(ctx)
	require.Error(t, err, "reader must fail closed when KMS cannot Open")
}

func TestPayloadPruneRetainsSessionSnapshotPayload(t *testing.T) {
	ctx := context.Background()
	clock := conformance.NewClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c := &payloadTestCodec{}
	s, err := openInstalledFixture(filepath.Join(t.TempDir(), "retained.db"), Options{Clock: clock.Now, NoSync: true, PayloadCodec: c, PayloadNamespace: "test", RequirePayloadEncryption: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	b := conformance.SampleBinding()
	b.Owner = "owner"
	b.Project = "project"
	b.ChannelID = "channel"
	b.BindingEpoch = 7
	_, err = s.Ingest(ctx, core.NewBindingVersion(b), conformance.SampleMessage("snapshot"))
	require.NoError(t, err)
	l, err := s.LeaseTurn(ctx, conformance.Claim("worker"))
	require.NoError(t, err)
	_, err = s.UpdateTurn(ctx, l.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "create"}})
	require.NoError(t, err)
	snapshot := core.NewSessionSnapshot(l.Turn.ConversationID, 1, core.CreateSessionSpec{Binding: b, CreationKey: "create", LookupKey: "lookup", ChatType: "group", EnvironmentOverride: &core.EnvironmentOverride{Type: "sandbox", Env: map[string]string{"PRIVATE": "retained-private-env"}}})
	_, err = s.FreezeSession(ctx, l.Turn.Lease, snapshot)
	require.NoError(t, err)
	_, err = s.BindSession(ctx, l.Turn.Lease, core.SessionBinding{State: core.SessionActive, Generation: 1, SessionID: "session", CreationKey: "create", LookupKey: "lookup", BindingVersion: l.Turn.BindingVersion, SnapshotID: snapshot.ID})
	require.NoError(t, err)
	require.NoError(t, s.SettleTurn(ctx, l.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, nil))
	clock.Advance(8 * 24 * time.Hour)
	pruned, err := s.Prune(ctx, clock.Now())
	require.NoError(t, err)
	require.Equal(t, 1, pruned.Turns)
	require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, 1, tx.Bucket(bucketPayloads).Stats().KeyN, "only the live snapshot payload remains")
		require.Equal(t, 1, tx.Bucket(bucketPayloadRefs).Stats().KeyN)
		return nil
	}))
	got, err := s.GetSessionSnapshot(ctx, snapshot.ID)
	require.NoError(t, err)
	require.Equal(t, "retained-private-env", got.Spec.EnvironmentOverride.Env["PRIVATE"])
	require.Equal(t, snapshot.RequestHash, got.RequestHash)
}

func TestPayloadPruneRetainsPendingControlProvenance(t *testing.T) {
	ctx := context.Background()
	clock := conformance.NewClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	s, err := openInstalledFixture(filepath.Join(t.TempDir(), "pending-control.db"), Options{Clock: clock.Now, NoSync: true, PayloadCodec: &payloadTestCodec{}, PayloadNamespace: "test", RequirePayloadEncryption: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	b := conformance.SampleBinding()
	b.Owner = "owner"
	b.Project = "project"
	v := core.NewBindingVersion(b)
	m := conformance.StopMessage("pending")
	first, err := s.Ingest(ctx, v, m)
	require.NoError(t, err)
	require.NotNil(t, first.Reply)
	clock.Advance(8 * 24 * time.Hour)
	b.AgentVersion = "2"
	next := core.NewBindingVersion(b)
	_, err = s.Ingest(ctx, next, conformance.SampleMessage("new"))
	require.NoError(t, err)
	pruned, err := s.Prune(ctx, clock.Now().Add(-time.Hour))
	require.NoError(t, err)
	require.Zero(t, pruned.Controls)
	versions, err := s.ListBindingVersions(ctx)
	require.NoError(t, err)
	found := false
	for _, row := range versions {
		if row.Version == v.Version {
			found = true
		}
	}
	require.True(t, found, "pending control's exact binding version was pruned")
	duplicate, err := s.Ingest(ctx, next, m)
	require.NoError(t, err)
	require.True(t, duplicate.Duplicate)
	require.Equal(t, v.Version, duplicate.Message.BindingVersion)
	require.Equal(t, m.Text, duplicate.Message.Message.Text)
}
