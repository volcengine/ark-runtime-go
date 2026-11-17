// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func seedLegacyMountSnapshot(t *testing.T, store core.Store) {
	t.Helper()
	ctx := context.Background()
	message := runtimeMessage()
	message.ProviderEventID, message.MessageID = "seed-event", "seed-message"
	in, err := store.Ingest(ctx, core.NewBindingVersion(runtimeBinding()), message)
	require.NoError(t, err)
	lease, err := store.LeaseTurn(ctx, conformance.Claim("setup"))
	require.NoError(t, err)
	_, err = store.UpdateTurn(ctx, lease.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "seed-key"}})
	require.NoError(t, err)
	snapshot, err := store.FreezeSession(ctx, lease.Turn.Lease, core.NewSessionSnapshot(lease.Turn.ConversationID, 1, core.CreateSessionSpec{Binding: runtimeBinding(), CreationKey: "seed-key", LookupKey: "seed-lookup", ChatType: "p2p"}))
	require.NoError(t, err)
	_, err = store.BindSession(ctx, lease.Turn.Lease, core.SessionBinding{State: core.SessionActive, Generation: 1, SessionID: "sesn-legacy", CreationKey: "seed-key", LookupKey: "seed-lookup", BindingVersion: in.Turn.BindingVersion, SnapshotID: snapshot.ID})
	require.NoError(t, err)
	require.NoError(t, store.SettleTurn(ctx, lease.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, nil))
}

func TestRuntimeLegacyMountEnvironmentProof(t *testing.T) {
	for _, frozenType := range []string{"self_hosted", "", "cloud"} {
		t.Run(frozenType, func(t *testing.T) {
			store, err := newEmbeddedStore(t)
			require.NoError(t, err)
			seedLegacyMountSnapshot(t, store)
			f := &conformance.MAServer{EnvironmentType: "cloud"}
			var mutableReads atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.Contains(r.URL.Path, "/environments/") {
					mutableReads.Add(1)
				}
				if r.Method == http.MethodGet && r.URL.Path == "/api/v3/sessions/sesn-legacy" {
					response := map[string]any{"id": "sesn-legacy", "type": "session", "created_at": "2026-10-13T00:00:00Z", "updated_at": "2026-10-13T00:00:00Z", "status": "idle", "agent": map[string]any{"id": "agent", "version": 1}, "environment_id": "env"}
					if frozenType != "" {
						response["environment"] = map[string]any{"id": "env", "config": map[string]any{"type": frozenType}}
					}
					_ = json.NewEncoder(w).Encode(response)
					return
				}
				f.ServeHTTP(w, r)
			}))
			t.Cleanup(server.Close)
			client, err := newTestMA(server.URL, "fixture-key", ma.ClientOptions{FilePollInterval: time.Millisecond})
			require.NoError(t, err)
			p := &attachmentProvider{files: map[string]string{"pdf": "%PDF"}}
			runtime, err := core.NewRuntime(store, []core.Connection{{Binding: mountBinding(), MAClient: client, Provider: p}}, testRuntimeOptions())
			require.NoError(t, err)
			message := mountMessage()
			message.Attachments = []core.Attachment{{Kind: "file", Key: "pdf", FileName: "report.pdf"}}
			mustEnqueue(t, runtime, "binding", message)
			runUntilCleanup(t, runtime)
			f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
			state := waitLatest(t, store, core.TurnDone, core.DeliveryDone, 6*time.Second)
			require.Len(t, state.turn.Input.Mounts, 1)
			require.Equal(t, frozenType != "cloud", state.turn.Input.Mounts[0].Skipped)
			require.Zero(t, mutableReads.Load(), "a mutable Environment cannot prove the legacy Session snapshot type")
			if frozenType == "" {
				require.Contains(t, strings.Join(lastContentTexts(f), ""), "缺少创建时冻结的环境类型")
				require.Contains(t, state.export.Deliveries[0].Body.Text, "缺少创建时冻结的环境类型", "the user must receive a durable degradation diagnostic")
			}
			f.Lock()
			defer f.Unlock()
			require.Zero(t, f.CreateCount)
			if frozenType != "cloud" {
				require.Empty(t, f.SessionResources)
				require.Equal(t, "document", f.LastContent[1].(map[string]any)["type"])
			}
		})
	}
}

func TestRuntimePDFPresentationFollowsNewGenerationEnvironment(t *testing.T) {
	for _, initial := range []string{"cloud", "self_hosted"} {
		t.Run(initial, func(t *testing.T) {
			next := "cloud"
			if initial == "cloud" {
				next = "self_hosted"
			}
			f := &conformance.MAServer{EnvironmentType: initial, SendCodes: []int{http.StatusNotFound}, SendErrorCodes: []string{"InvalidSession"}}
			p := &attachmentProvider{files: map[string]string{"pdf": "%PDF"}}
			h := mountHarness(t, f, p)
			var creates atomic.Uint32
			h.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost && r.URL.Path == "/api/v3/sessions" {
					f.Lock()
					f.CreateSessionID = fmt.Sprintf("sesn-%d", creates.Add(1))
					f.Unlock()
				}
				f.ServeHTTP(w, r)
				if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/events") {
					f.Lock()
					f.EnvironmentType = next
					f.Unlock()
				}
			})
			message := mountMessage()
			message.Attachments = []core.Attachment{{Kind: "file", Key: "pdf", FileName: "report.pdf"}}
			h.enqueue(message)
			h.run()
			f.AppendAfterInputCount(t, 2, conformance.AnswerEvents("ok")...)
			state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
			require.Equal(t, uint64(2), state.turn.Session.Generation)
			require.Len(t, state.turn.Input.Mounts, 1)
			require.Equal(t, "file-1", state.turn.Input.Mounts[0].FileID)
			require.Equal(t, "document", state.turn.Input.Blocks[0].Type, "the upload fact stays independent of the generation presentation")
			require.Equal(t, next == "self_hosted", state.turn.Input.Mounts[0].Skipped)
			f.Lock()
			defer f.Unlock()
			require.Len(t, f.Uploads, 1)
			require.Equal(t, "user_data", f.Uploads[0].Purpose, "a reusable PDF must remain eligible for the document fallback")
			expectedType := "text"
			if next == "self_hosted" {
				expectedType = "document"
			}
			require.Equal(t, expectedType, f.LastContent[1].(map[string]any)["type"])
		})
	}
}
