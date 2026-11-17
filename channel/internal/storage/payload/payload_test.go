// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package payload

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
)

type testCodec struct{ seals int }

func (c *testCodec) Seal(_ context.Context, _ codec.Scope, b []byte) (codec.Envelope, error) {
	c.seals++
	return codec.Envelope{Version: 1, KeyID: "test", Ciphertext: append([]byte(nil), b...)}, nil
}
func (c *testCodec) Open(_ context.Context, _ codec.Scope, e codec.Envelope) ([]byte, error) {
	return append([]byte(nil), e.Ciphertext...), nil
}

func TestPreparationKeepsSensitiveTextOutsideMetadata(t *testing.T) {
	c := &testCodec{}
	p := NewSession(c, "deployment")
	m := core.NewInboundMessage(core.Message{BindingID: "channel", MessageID: "provider", Text: "sensitive-source"}, core.Message{}.ReceivedAt)
	m.ID = "message"
	b, refs, err := p.Protect(m, core.Binding{Owner: "owner", Project: "project"})
	require.NoError(t, err)
	require.NotContains(t, string(b), "sensitive-source")
	require.Len(t, refs, 1)
	require.True(t, p.Pending())
	require.Equal(t, 0, c.seals)
	require.NoError(t, p.Prepare(context.Background()))
	require.Equal(t, 1, c.seals)
	b, refs, err = p.Protect(m, core.Binding{Owner: "owner", Project: "project"})
	require.NoError(t, err)
	require.Len(t, refs, 1)
	require.False(t, p.Pending())
	var metadata core.InboundMessage
	require.NoError(t, json.Unmarshal(b, &metadata))
	require.Empty(t, metadata.Message.Text)
	require.NoError(t, p.Hydrate(context.Background(), &metadata, func(context.Context, string) (Record, error) { return p.Records()[0], nil }))
	require.Equal(t, "sensitive-source", metadata.Message.Text)
	require.Equal(t, m.ContentHash, metadata.ContentHash)
}

func TestVerifyChecksReferenceScopeWithoutOpeningPlaintext(t *testing.T) {
	c := &testCodec{}
	s := NewSession(c, "deployment")
	v := core.NewInboundMessage(core.Message{BindingID: "binding", Text: "sensitive"}, core.Message{}.ReceivedAt)
	v.ID = "message"
	b, _, e := s.Protect(v, core.Binding{ID: "binding", Owner: "owner", Project: "project"})
	require.NoError(t, e)
	require.NoError(t, s.Prepare(context.Background()))
	require.NoError(t, json.Unmarshal(b, &v))
	require.NoError(t, s.Verify(context.Background(), &v, func(context.Context, string) (Record, error) { return s.Records()[0], nil }))
	bad := s.Records()[0]
	bad.Scope.Namespace = "another"
	require.Error(t, s.Verify(context.Background(), &v, func(context.Context, string) (Record, error) { return bad, nil }))
	delete(v.Payloads.Refs, "content")
	require.Error(t, s.Verify(context.Background(), &v, func(context.Context, string) (Record, error) { return s.Records()[0], nil }))
}

func TestProtectedMetadataRejectsMixedPlaintext(t *testing.T) {
	c := &testCodec{}
	s := NewSession(c, "deployment")
	m := core.NewInboundMessage(core.Message{BindingID: "binding", Text: "secret"}, core.Message{}.ReceivedAt)
	m.ID = "message"
	wire, _, e := s.Protect(m, core.Binding{ID: "binding"})
	require.NoError(t, e)
	require.NoError(t, s.Prepare(context.Background()))
	require.NoError(t, json.Unmarshal(wire, &m))
	m.Message.Text = "secret"
	require.Error(t, s.Verify(context.Background(), &m, func(context.Context, string) (Record, error) { return s.Records()[0], nil }), "matching ciphertext must not hide a plaintext metadata copy")
}

func protectTurn(t *testing.T, s *Session, turn core.Turn, records map[string]Record) core.Turn {
	t.Helper()
	_, _, err := s.Protect(turn, core.Binding{ID: "binding"})
	require.NoError(t, err)
	require.NoError(t, s.Prepare(context.Background()))
	wire, _, err := s.Protect(turn, core.Binding{ID: "binding"})
	require.NoError(t, err)
	for _, record := range s.Records() {
		require.NotContains(t, records, record.ID, "new generations must not reuse immutable payload records")
		records[record.ID] = record
	}
	var stored core.Turn
	require.NoError(t, json.Unmarshal(wire, &stored))
	return stored
}

func TestMountPayloadsKeepPathsOutsideMetadata(t *testing.T) {
	s := NewSession(&testCodec{}, "deployment")
	turn := core.Turn{ID: "turn", BindingID: "binding", Session: core.TurnSession{Generation: 1}, Input: core.FrozenInput{MountsFrozen: true, Mounts: []core.MountedFile{
		{FileID: "file", ActualPath: "file:///mnt/session/uploads/private-name.csv"},
		{FileID: "self-hosted-file", Skipped: true, DownloadURL: "https://storage.example/file?signature=sensitive"},
	}}}
	wire, _, err := s.Protect(turn, core.Binding{ID: "binding"})
	require.NoError(t, err)
	require.NotContains(t, string(wire), "private-name.csv")
	require.NotContains(t, string(wire), "signature=sensitive")
	records := map[string]Record{}
	stored := protectTurn(t, s, turn, records)
	require.Empty(t, stored.Input.Mounts)
	load := func(_ context.Context, id string) (Record, error) { return records[id], nil }
	require.NoError(t, s.Verify(context.Background(), &stored, load))
	redacted := stored
	redacted.Input.Mounts = turn.Input.Mounts
	require.ErrorIs(t, s.Verify(context.Background(), &redacted, load), ErrInvalid, "ciphertext refs cannot hide plaintext mount paths")
	require.NoError(t, s.Hydrate(context.Background(), &stored, load))
	require.Equal(t, turn.Input.Mounts, stored.Input.Mounts)
}

func TestTurnPayloadGenerationDoesNotRestoreOldContextOrPath(t *testing.T) {
	first := core.Turn{ID: "turn", BindingID: "binding", Phase: core.PhaseSend, Session: core.TurnSession{Generation: 1}, Input: core.FrozenInput{
		BlocksFrozen: true, Blocks: []core.InputBlock{{Type: "notice", FileID: "file"}}, ContextFrozen: true, Context: "old context",
		MountsFrozen: true, Mounts: []core.MountedFile{{FileID: "file", SessionID: "session-1", ActualPath: "old-path"}},
	}}
	records := map[string]Record{}
	s := NewSession(&testCodec{}, "deployment")
	stored := protectTurn(t, s, first, records)
	load := func(_ context.Context, id string) (Record, error) { return records[id], nil }
	require.NoError(t, s.Hydrate(context.Background(), &stored, load))
	next, err := stored.Apply(core.TurnUpdate{Rebind: true, Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 2, CreationKey: "new"}})
	require.NoError(t, err)
	next, err = next.Apply(core.TurnUpdate{Phase: core.PhaseCreate, Input: core.FrozenInput{ContextFrozen: true, Context: "new context", MountsFrozen: true, Mounts: []core.MountedFile{{FileID: "file", SessionID: "session-2", ActualPath: "new-path"}}}})
	require.NoError(t, err)
	secondSession := NewSession(&testCodec{}, "deployment")
	storedNext := protectTurn(t, secondSession, next, records)
	require.NoError(t, secondSession.Hydrate(context.Background(), &storedNext, load))
	require.Equal(t, "new context", storedNext.Input.Context)
	require.Equal(t, "new-path", storedNext.Input.Mounts[0].ActualPath)
	require.Equal(t, first.Input.Blocks, storedNext.Input.Blocks)
}

func TestTurnLegacyPayloadWithoutMountReference(t *testing.T) {
	s := NewSession(&testCodec{}, "deployment")
	state := cloneState(nil, core.Binding{ID: "binding"})
	_, err := s.reference(state, "turn", "turn", "context", "old context")
	require.NoError(t, err)
	require.NoError(t, s.Prepare(context.Background()))
	turn := core.Turn{ID: "turn", BindingID: "binding", Payloads: state, Input: core.FrozenInput{ContextFrozen: true, MountsFrozen: true}}
	require.NoError(t, s.Hydrate(context.Background(), &turn, func(_ context.Context, id string) (Record, error) { return s.records[id], nil }))
	require.Equal(t, "old context", turn.Input.Context)
	require.Empty(t, turn.Input.Mounts)
}

func TestEncryptedSnapshotRestoresFrozenRequest(t *testing.T) {
	s := NewSession(&testCodec{}, "deployment")
	spec := core.CreateSessionSpec{Binding: core.Binding{ID: "binding"}, CreationKey: "creation", LookupKey: "lookup", EnvironmentType: "cloud", EnvironmentOverride: &core.EnvironmentOverride{Type: "cloud", Env: map[string]string{"PRIVATE": "sensitive-value"}}}
	snapshot := core.NewSessionSnapshot("route", 1, spec)
	_, _, err := s.Protect(snapshot, spec.Binding)
	require.NoError(t, err)
	require.NoError(t, s.Prepare(context.Background()))
	wire, _, err := s.Protect(snapshot, spec.Binding)
	require.NoError(t, err)
	require.NotContains(t, string(wire), "sensitive-value")
	var restored core.SessionSnapshot
	require.NoError(t, json.Unmarshal(wire, &restored))
	require.NoError(t, s.Hydrate(context.Background(), &restored, func(_ context.Context, id string) (Record, error) { return s.records[id], nil }))
	require.True(t, restored.Valid())
	require.Equal(t, snapshot.RequestHash, restored.RequestHash)
	require.Equal(t, spec, restored.Spec)
}
