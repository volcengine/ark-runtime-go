// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package payload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestStorageCompatibleSnapshotKeepsFutureSpecOnlyInCiphertext(t *testing.T) {
	spec := core.CreateSessionSpec{Binding: core.Binding{ID: "binding"}, CreationKey: "creation", LookupKey: "lookup", EnvironmentType: "cloud"}
	snapshot := core.NewSessionSnapshot("route", 1, spec)
	rawSpec, err := json.Marshal(spec)
	require.NoError(t, err)
	rawSpec = append(rawSpec[:len(rawSpec)-1], []byte(`,"Future":{"secret":"future-private-spec"}}`)...)
	hash := sha256.Sum256(rawSpec)
	snapshot.RequestHash = hex.EncodeToString(hash[:])
	require.NoError(t, snapshot.RestoreStoredSpecJSON(rawSpec))
	raw, err := core.EncodeStored(snapshot)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	fields["spec"] = rawSpec
	raw, err = json.Marshal(fields)
	require.NoError(t, err)
	require.NoError(t, core.DecodeStored(raw, &snapshot))
	codec := &testCodec{}
	session := NewSession(codec, "deployment")
	metadata, refs, err := session.Protect(snapshot, spec.Binding)
	require.NoError(t, err)
	require.NotContains(t, string(metadata), "future-private-spec")
	require.Equal(t, 0, codec.seals)
	require.NoError(t, session.Prepare(context.Background()))
	var restored core.SessionSnapshot
	require.NoError(t, core.DecodeStored(metadata, &restored))
	require.NoError(t, session.Hydrate(context.Background(), &restored, func(_ context.Context, id string) (Record, error) { return session.records[id], nil }))
	require.True(t, restored.Valid())
	require.Equal(t, snapshot.RequestHash, restored.RequestHash)
	storedSpec, err := restored.StoredSpecJSON()
	require.NoError(t, err)
	require.Equal(t, string(rawSpec), string(storedSpec))
	nextMetadata, nextRefs, err := session.Protect(restored, spec.Binding)
	require.NoError(t, err)
	require.NotContains(t, string(nextMetadata), "future-private-spec")
	require.Equal(t, refs, nextRefs)
	require.False(t, session.Pending())
	require.Equal(t, 1, codec.seals)
}

func TestStorageCompatibleInputPreservesUnknownPayloadAndDigest(t *testing.T) {
	var turn core.Turn
	require.NoError(t, core.DecodeStored([]byte(`{"id":"turn","binding_id":"binding","session":{"generation":1},"input":{"blocks_frozen":true,"blocks":[{"type":"image","key":"a","file_id":"f","future_secret":"future-private-block"}],"context_frozen":true,"context":"private-context","mounts_frozen":true,"mounts":[{"file_id":"f","mount_path":"file","actual_path":"/private-file","future_secret":"future-private-mount"}]}}`), &turn))
	originalDigest := turn.Input.LogicalHash()
	codec := &testCodec{}
	session := NewSession(codec, "deployment")
	metadata, refs, err := session.Protect(turn, core.Binding{ID: "binding"})
	require.NoError(t, err)
	require.NotContains(t, string(metadata), "future-private")
	require.NotContains(t, string(metadata), "private-context")
	require.NoError(t, session.Prepare(context.Background()))
	var restored core.Turn
	require.NoError(t, core.DecodeStored(metadata, &restored))
	require.NoError(t, session.Hydrate(context.Background(), &restored, func(_ context.Context, id string) (Record, error) { return session.records[id], nil }))
	require.Equal(t, originalDigest, restored.Input.LogicalDigest)
	require.Equal(t, originalDigest, restored.Input.LogicalHash())
	blocks, err := core.EncodeStored(restored.Input.Blocks)
	require.NoError(t, err)
	require.Contains(t, string(blocks), "future-private-block")
	mounts, err := core.EncodeStored(restored.Input.Mounts)
	require.NoError(t, err)
	require.Contains(t, string(mounts), "future-private-mount")
	restored.Revision++
	nextMetadata, nextRefs, err := session.Protect(restored, core.Binding{ID: "binding"})
	require.NoError(t, err)
	require.NotContains(t, string(nextMetadata), "future-private")
	require.Equal(t, refs, nextRefs)
	require.False(t, session.Pending())
	require.Equal(t, 3, codec.seals)
}

func TestStorageCompatibleDeliveryKeepsUnionExtensionOnlyInCiphertext(t *testing.T) {
	var delivery core.Delivery
	require.NoError(t, core.DecodeStored([]byte(`{"id":"delivery","binding_id":"binding","body":{"type":"text","text":"private-answer","future":{"secret":"future-private-body"}}}`), &delivery))
	session := NewSession(&testCodec{}, "deployment")
	metadata, _, err := session.Protect(delivery, core.Binding{ID: "binding"})
	require.NoError(t, err)
	require.NotContains(t, string(metadata), "private-answer")
	require.NotContains(t, string(metadata), "future-private-body")
	require.NoError(t, session.Prepare(context.Background()))
	var restored core.Delivery
	require.NoError(t, core.DecodeStored(metadata, &restored))
	require.NoError(t, session.Hydrate(context.Background(), &restored, func(_ context.Context, id string) (Record, error) { return session.records[id], nil }))
	body, err := core.EncodeStored(restored.Body)
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"text","text":"private-answer","future":{"secret":"future-private-body"}}`, string(body))
}
