// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func storedObject(t *testing.T, data []byte) map[string]json.RawMessage {
	t.Helper()
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &fields))
	return fields
}

func TestStoredRecordPreservesUnknownAndOwnsKnownFields(t *testing.T) {
	raw := []byte(`{"id":"t","wait_reason":"busy","revision":2,"future":{"keep":true},"session":{"generation":4,"session_id":"old","future_session":"keep"},"outcome":{"status":"failed","future_outcome":1},"_payloads":{"owner":"o","project":"p","refs":{"old":"payload:old","keep":"payload:keep"},"future_payload":"keep"}}`)
	var turn Turn
	require.NoError(t, DecodeStored(raw, &turn))
	turn.WaitReason = ""
	turn.Session.SessionID = ""
	turn.Revision = 0
	turn.Outcome = nil
	delete(turn.Payloads.Refs, "old")
	encoded, err := EncodeStored(turn)
	require.NoError(t, err)
	fields := storedObject(t, encoded)
	require.JSONEq(t, `{"keep":true}`, string(fields["future"]))
	require.NotContains(t, fields, "wait_reason")
	require.NotContains(t, fields, "outcome")
	require.JSONEq(t, `0`, string(fields["revision"]))
	session := storedObject(t, fields["session"])
	require.NotContains(t, session, "session_id")
	require.JSONEq(t, `"keep"`, string(session["future_session"]))
	payloads := storedObject(t, fields["_payloads"])
	require.JSONEq(t, `"keep"`, string(payloads["future_payload"]))
	require.JSONEq(t, `{"keep":"payload:keep"}`, string(payloads["refs"]))
	require.NotContains(t, string(encoded), "StoredRecord")
	publicJSON, err := json.Marshal(turn)
	require.NoError(t, err)
	require.NotContains(t, string(publicJSON), "future")
}

func TestStoredRecordAnonymousEmbeddingAndStableArrays(t *testing.T) {
	type wrapper struct {
		Turn
		Index string `json:"index"`
	}
	var record wrapper
	require.NoError(t, DecodeStored([]byte(`{"id":"t","index":"old","future_root":1,"input":{"blocks":[{"type":"image","key":"a","file_id":"fa","future":"A"},{"type":"image","key":"b","file_id":"fb","future":"B"}]}}`), &record))
	record.Index = "new"
	record.Input.Blocks = []InputBlock{record.Input.Blocks[1], {Type: "image", Key: "c", FileID: "fc"}}
	data, err := EncodeStored(record)
	require.NoError(t, err)
	root := storedObject(t, data)
	require.JSONEq(t, `1`, string(root["future_root"]))
	require.JSONEq(t, `"new"`, string(root["index"]))
	input := storedObject(t, root["input"])
	var blocks []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(input["blocks"], &blocks))
	require.JSONEq(t, `"B"`, string(blocks[0]["future"]))
	require.NotContains(t, blocks[1], "future")
}

func TestStoredRecordCustomUnionAndOpaqueBody(t *testing.T) {
	for _, rawBody := range []string{`{"type":"text","text":"answer","future":{"x":1}}`, `{"type":"artifact","artifact":{"kind":"file","file_id":"f","future":2},"future_body":true}`, `{"type":"sticker","sticker":{"id":"future"}}`} {
		t.Run(rawBody, func(t *testing.T) {
			var delivery Delivery
			require.NoError(t, DecodeStored([]byte(`{"id":"d","body":`+rawBody+`,"attempt":1}`), &delivery))
			delivery.Attempt++
			encoded, err := EncodeStored(delivery)
			require.NoError(t, err)
			require.JSONEq(t, rawBody, string(storedObject(t, encoded)["body"]))
			if delivery.Body.Type == "sticker" {
				require.False(t, delivery.ValidNew(delivery.Source))
				var body DeliveryBody
				require.ErrorIs(t, json.Unmarshal([]byte(rawBody), &body), ErrUnknownBody)
			}
		})
	}
}

func TestStoredRecordKnownValuesDoNotGainState(t *testing.T) {
	original := Turn{ID: "t", Revision: 2}
	raw, err := json.Marshal(original)
	require.NoError(t, err)
	var got Turn
	require.NoError(t, DecodeStored(raw, &got))
	require.Equal(t, original, got)
	encoded, err := EncodeStored(got)
	require.NoError(t, err)
	require.Equal(t, raw, encoded)
	require.Equal(t, digestJSON(original), digestJSON(got))
}

func TestStoredSessionSnapshotPreservesFrozenSpecAndHash(t *testing.T) {
	spec := CreateSessionSpec{Binding: Binding{ID: "b"}, CreationKey: "create", LookupKey: "lookup", EnvironmentType: "cloud"}
	snapshot := NewSessionSnapshot("route", 1, spec)
	rawSpec, err := json.Marshal(spec)
	require.NoError(t, err)
	rawSpec = append(rawSpec[:len(rawSpec)-1], []byte(`,"FutureExecution":{"mode":"next"}}`)...)
	snapshot.RequestHash = digestBytes(rawSpec)
	raw, err := json.Marshal(snapshot)
	require.NoError(t, err)
	fields := storedObject(t, raw)
	fields["spec"] = rawSpec
	raw, err = json.Marshal(fields)
	require.NoError(t, err)
	var got SessionSnapshot
	require.NoError(t, DecodeStored(raw, &got))
	require.True(t, got.Valid(), "完整原文hash可以验真，但未知执行语义必须由任务kind隔离")
	require.Equal(t, snapshot.RequestHash, got.RequestHash)
	encoded, err := EncodeStored(got)
	require.NoError(t, err)
	require.Equal(t, string(rawSpec), string(storedObject(t, encoded)["spec"]))
	got.Spec.CreationKey = "tampered"
	require.False(t, got.Valid())

	var tampered SessionSnapshot
	require.NoError(t, DecodeStored([]byte(strings.Replace(string(raw), `"mode":"next"`, `"mode":"bad"`, 1)), &tampered))
	require.False(t, tampered.Valid())
}

func TestStoredTurnSnapshotValidatesOriginalBinding(t *testing.T) {
	binding, err := json.Marshal(Binding{ID: "b"})
	require.NoError(t, err)
	binding = append(binding[:len(binding)-1], []byte(`,"future_policy":{"x":1}}`)...)
	raw := []byte(`{"id":"turn-snapshot:` + digestBytes(binding) + `","schema_version":1,"binding":` + string(binding) + `}`)
	var got TurnSnapshot
	require.NoError(t, DecodeStored(raw, &got))
	require.True(t, got.Valid())
	got.Binding.ID = "other"
	require.False(t, got.Valid())
	require.True(t, NewTurnSnapshot(Binding{ID: "b"}, time.Time{}).Valid())
}

func TestStoredRecordContainersAndPayloadField(t *testing.T) {
	var turns map[string][]Turn
	require.NoError(t, DecodeStored([]byte(`{"page":[{"id":"t","future":1,"input":{"blocks":[{"type":"image","key":"a","file_id":"f","future_secret":"private"}]}}]}`), &turns))
	turns["page"][0].Revision = 4
	encoded, err := EncodeStored(turns)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"future":1`)
	turn := turns["page"][0]
	blocks, err := EncodeStored(turn.Input.Blocks)
	require.NoError(t, err)
	require.Contains(t, string(blocks), "future_secret")
	copyTurn := turn
	copyTurn.Input.Blocks = nil
	copyTurn.ForgetStoredFields("input.blocks")
	metadata, err := EncodeStored(copyTurn)
	require.NoError(t, err)
	require.NotContains(t, string(metadata), "private")
	original, err := EncodeStored(turn)
	require.NoError(t, err)
	require.Contains(t, string(original), "private", "清除原文必须copy-on-write")
}

func TestStoredDeliveryBodyStandaloneRetainsExtension(t *testing.T) {
	var body DeliveryBody
	require.NoError(t, DecodeStored([]byte(`{"type":"text","text":"old","future":"keep"}`), &body))
	body.Text = "new"
	encoded, err := EncodeStored(body)
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"text","text":"new","future":"keep"}`, string(encoded))
}

func TestRestoreStoredSpecJSONPreservesEncryptedOriginal(t *testing.T) {
	raw := json.RawMessage(`{"Binding":{"id":"b"},"CreationKey":"c","LookupKey":"l","Extras":{},"EnvironmentOverride":{"Type":"cloud","Env":{"SECRET":"private"}},"ChatType":"p2p","EnvironmentType":"cloud","Future":{"secret":"private"}}`)
	snapshot := NewSessionSnapshot("route", 1, CreateSessionSpec{Binding: Binding{ID: "b"}, CreationKey: "c", LookupKey: "l"})
	snapshot.RequestHash = digestBytes(raw)
	before := snapshot.RequestHash
	require.NoError(t, snapshot.RestoreStoredSpecJSON(raw))
	require.True(t, snapshot.Valid())
	require.Equal(t, before, snapshot.RequestHash)
	preserved, err := snapshot.StoredSpecJSON()
	require.NoError(t, err)
	require.Equal(t, string(raw), string(preserved))
	snapshot.RequestHash = strings.Repeat("0", 64)
	require.ErrorIs(t, snapshot.RestoreStoredSpecJSON(raw), ErrInvalid)
}

func TestStoredFrozenInputLogicalHashIncludesOriginalBlocks(t *testing.T) {
	raw := []byte(`{"blocks_frozen":true,"blocks":[{"type":"image","key":"a","file_id":"f","future_secret":"private"}],"context_frozen":true,"context":"hello"}`)
	var input FrozenInput
	require.NoError(t, DecodeStored(raw, &input))
	require.Equal(t, digestBytes(raw), input.LogicalHash())
	var hydrated FrozenInput
	hydrated.BlocksFrozen, hydrated.ContextFrozen, hydrated.Context = true, true, "hello"
	require.NoError(t, DecodeStored([]byte(`[{"type":"image","key":"a","file_id":"f","future_secret":"private"}]`), &hydrated.Blocks))
	require.Equal(t, digestBytes(raw), hydrated.LogicalHash(), "独立解密数组必须保留完整输入hash")
	hydrated.Blocks[0].FileID = "tampered"
	require.NotEqual(t, digestBytes(raw), hydrated.LogicalHash())
	known := FrozenInput{BlocksFrozen: true, Blocks: []InputBlock{{Type: "image", FileID: "f"}}, ContextFrozen: true, Context: "hello"}
	require.Equal(t, digestJSON(known), known.LogicalHash(), "现有已知对象算法不变")
}

func TestStoredRemoteOperationPreservesUnknownFields(t *testing.T) {
	var operation RemoteOperation
	require.NoError(t, DecodeStored([]byte(`{"future":{"keep":true}}`), &operation))
	data, err := EncodeStored(&operation)
	require.NoError(t, err)
	require.JSONEq(t, `{"keep":true}`, string(storedObject(t, data)["future"]))
}

func TestStoredBindingVersionValidatesOriginalContent(t *testing.T) {
	content, err := json.Marshal(Binding{ID: "b"})
	require.NoError(t, err)
	content = append(content[:len(content)-1], []byte(`,"future":{"policy":"keep"}}`)...)
	var version BindingVersion
	require.NoError(t, DecodeStored([]byte(`{"binding_id":"b","version":"bv1:`+digestBytes(content)+`","content":`+string(content)+`}`), &version))
	require.True(t, version.Valid())
	version.Content.ID = "tampered"
	require.False(t, version.Valid())
}

func TestStoredAttachmentRetainsPayloadExtension(t *testing.T) {
	var attachments []Attachment
	require.NoError(t, DecodeStored([]byte(`[{"kind":"image","key":"a","future_secret":"keep"}]`), &attachments))
	encoded, err := EncodeStored(attachments)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"future_secret":"keep"`)
}

func TestStoredRecordStableMountIdentityIncludesMessage(t *testing.T) {
	var turn Turn
	require.NoError(t, DecodeStored([]byte(`{"id":"t","input":{"mounts":[{"message_id":"a","key":"same","file_id":"f","future":"A"},{"message_id":"b","key":"same","file_id":"f","future":"B"}]}}`), &turn))
	turn.Input.Mounts = []MountedFile{turn.Input.Mounts[1], turn.Input.Mounts[0]}
	turn.Input.Mounts[0].ActualPath = "updated"
	encoded, err := EncodeStored(turn)
	require.NoError(t, err)
	input := storedObject(t, storedObject(t, encoded)["input"])
	var mounts []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(input["mounts"], &mounts))
	require.JSONEq(t, `"B"`, string(mounts[0]["future"]))
	require.JSONEq(t, `"A"`, string(mounts[1]["future"]))
}
