// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
	sqlstore "github.com/volcengine/ark-runtime-go/channel/store/sql"
)

type promptRecoveryAuthorizer struct {
	*userAuthFixture
	reads atomic.Int32
}

func (a *promptRecoveryAuthorizer) AuthPrompt(_ context.Context, ref core.AuthFlowRef) (core.AuthFlow, error) {
	a.reads.Add(1)
	return core.AuthFlow{ID: ref.FlowID, URL: "https://accounts.feishu.cn/device", UserCode: "ORIGINAL-CODE", ExpiresAt: time.Now().Add(time.Minute)}, nil
}

func TestRuntimeUserRecoversOriginalPromptBeforePolling(t *testing.T) {
	for _, kind := range []string{"sql", "embedded"} {
		t.Run(kind, func(t *testing.T) {
			s := userRuntimeStore(t, kind)
			auth := s.(core.UserAuthStore)
			ctx := context.Background()
			b := userBinding()
			p := userPrincipalFixture(b)
			a := &promptRecoveryAuthorizer{userAuthFixture: &userAuthFixture{store: auth}}
			remote := &conformance.MAServer{EnvironmentType: "cloud"}
			options := testRuntimeOptions()
			options.MessageDeadline = time.Minute
			r, err := core.NewRuntime(s, []core.Connection{{Binding: b, Provider: &runtimeProvider{}, MAClient: newMAForHarness(t, remote), UserAuthorizer: a}}, options)
			require.NoError(t, err)
			result := mustEnqueue(t, r, b.ID, runtimeMessage())
			held, err := auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "crashed-start", TTL: time.Minute})
			require.NoError(t, err)
			_, err = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthPending, FlowID: "flow", ExpiresAt: time.Now().Add(time.Minute), Secret: &core.UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte("encrypted original prompt")}, Release: true})
			require.NoError(t, err)
			runCtx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- r.Run(runCtx) }()
			defer func() { cancel(); require.NoError(t, <-done) }()
			require.Eventually(t, func() bool { turn, e := s.GetTurn(ctx, result.Turn.ID); return e == nil && turn.AuthWait != nil }, 2*time.Second, 10*time.Millisecond)
			require.EqualValues(t, 1, a.reads.Load())
			a.mu.Lock()
			require.Zero(t, a.starts)
			require.Zero(t, a.polls)
			a.mu.Unlock()
			deliveries, err := s.ListDeliveries(ctx, core.DeliveryQuery{})
			require.NoError(t, err)
			require.Len(t, deliveries, 1)
			body, err := json.Marshal(deliveries[0])
			require.NoError(t, err)
			require.Contains(t, string(body), "ORIGINAL-CODE")
			require.Contains(t, string(body), "accounts.feishu.cn/device")
			remote.Lock()
			require.Zero(t, remote.CreateCount)
			require.Zero(t, remote.SendCount)
			remote.Unlock()
		})
	}
}

func promptStore(t *testing.T, kind string, c *conformance.UserAuthTestCodec) core.Store {
	path := filepath.Join(t.TempDir(), "prompt.db")
	if kind == "embedded" {
		s, e := openEmbeddedFixture(path, embedded.Options{NoSync: true, PayloadCodec: c, PayloadNamespace: "test", RequirePayloadEncryption: true})
		require.NoError(t, e)
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		return s
	}
	db, e := sql.Open("sqlite", "file:"+path+"?_txlock=immediate&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)")
	require.NoError(t, e)
	db.SetMaxOpenConns(1)
	options := sqlstore.Options{PayloadCodec: c, PayloadNamespace: "test", RequirePayloadEncryption: true}
	require.NoError(t, sqlstore.Install(context.Background(), db, sqlstore.SQLite, options))
	s, e := openSQLStore(context.Background(), db, sqlstore.SQLite, options)
	require.NoError(t, e)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}
func TestUserAuthFirstFlowAndPromptCommitAtomically(t *testing.T) {
	for _, kind := range []string{"sql", "embedded"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			crypt := &conformance.UserAuthTestCodec{}
			s := promptStore(t, kind, crypt)
			auth := s.(core.UserAuthStore)
			b := userBinding()
			p := userPrincipalFixture(b)
			result, err := s.Ingest(ctx, core.NewBindingVersion(b), runtimeMessage())
			require.NoError(t, err)
			claim := conformance.Claim("worker")
			claim.UserAuth = true
			leased, err := s.LeaseTurn(ctx, claim)
			require.NoError(t, err)
			require.NotNil(t, leased)
			r, err := auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "worker", TTL: time.Minute})
			require.NoError(t, err)
			deadline := time.Now().UTC().Add(time.Minute)
			r, err = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthVerifying, ExpiresAt: deadline})
			require.NoError(t, err)
			prompt := core.NewDelivery(core.DeliverySource{Kind: core.SourceProgress, ID: result.Turn.ID}, 1, "user-auth-prompt", b.ID, core.ReplyTargetFor(runtimeMessage(), b), core.TextBody("secret-url-and-code"))
			wait := core.UserAuthWait{RegisterFlow: true, Principal: p, Lease: r.Lease, Revision: r.Revision, FlowID: "flow", DeadlineAt: deadline, Until: time.Now().Add(time.Second), Prompt: &prompt}
			crypt.Fail.Store(true)
			require.Error(t, auth.WaitForUserAuth(ctx, leased.Turn.Lease, wait))
			crypt.Fail.Store(false)
			gate, err := auth.GetUserAuth(ctx, p)
			require.NoError(t, err)
			require.Equal(t, r.Revision, gate.Revision)
			require.Empty(t, gate.FlowID)
			turn, err := s.GetTurn(ctx, result.Turn.ID)
			require.NoError(t, err)
			require.Nil(t, turn.AuthWait)
			ds, err := s.ListDeliveries(ctx, core.DeliveryQuery{})
			require.NoError(t, err)
			require.Empty(t, ds)
			require.NoError(t, auth.WaitForUserAuth(ctx, leased.Turn.Lease, wait))
			gate, err = auth.GetUserAuth(ctx, p)
			require.NoError(t, err)
			require.Equal(t, "flow", gate.FlowID)
			require.Equal(t, core.UserAuthPending, gate.State)
			require.Empty(t, gate.Lease.Owner)
			turn, err = s.GetTurn(ctx, result.Turn.ID)
			require.NoError(t, err)
			require.NotNil(t, turn.AuthWait)
			require.False(t, turn.AuthWait.RegisterFlow)
			ds, err = s.ListDeliveries(ctx, core.DeliveryQuery{})
			require.NoError(t, err)
			require.Len(t, ds, 1)
		})
	}
}

type failPromptOnceAuthorizer struct {
	*promptRecoveryAuthorizer
	store  core.UserAuthStore
	crypt  *conformance.UserAuthTestCodec
	starts atomic.Int32
}

func (a *failPromptOnceAuthorizer) Start(ctx context.Context, p core.UserPrincipal, request core.AuthRequest) (core.AuthFlow, error) {
	a.starts.Add(1)
	flow := core.AuthFlow{ID: "flow", URL: "https://accounts.feishu.cn/device", UserCode: "ORIGINAL-CODE", ExpiresAt: time.Now().Add(time.Minute)}
	_, err := a.store.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: request.Lease, Revision: request.Revision, State: core.UserAuthPending, FlowID: flow.ID, ExpiresAt: flow.ExpiresAt, Secret: &core.UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte("encrypted-device-prompt")}})
	if err == nil {
		a.crypt.Fail.Store(true)
	}
	return flow, err
}

type observeWaitStore struct {
	core.Store
	core.UserAuthStore
	failed atomic.Bool
}

func (s *observeWaitStore) WaitForUserAuth(ctx context.Context, l core.Lease, w core.UserAuthWait) error {
	err := s.UserAuthStore.WaitForUserAuth(ctx, l, w)
	if err != nil {
		s.failed.Store(true)
	}
	return err
}

func TestRuntimeUserPromptSealFailureReopensOriginalFlowAndInput(t *testing.T) {
	for _, kind := range []string{"sql", "embedded"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			crypt := &conformance.UserAuthTestCodec{}
			path := filepath.Join(t.TempDir(), "runtime-prompt.db")
			open := func(install bool) core.Store {
				if kind == "embedded" {
					opts := embedded.Options{NoSync: true, PayloadCodec: crypt, PayloadNamespace: "test", RequirePayloadEncryption: true}
					if install {
						require.NoError(t, embedded.Install(path, opts))
					}
					s, e := openEmbeddedStore(path, opts)
					require.NoError(t, e)
					return s
				}
				db, e := sql.Open("sqlite", "file:"+path+"?_txlock=immediate&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)")
				require.NoError(t, e)
				db.SetMaxOpenConns(1)
				opts := sqlstore.Options{PayloadCodec: crypt, PayloadNamespace: "test", RequirePayloadEncryption: true}
				if install {
					require.NoError(t, sqlstore.Install(ctx, db, sqlstore.SQLite, opts))
				}
				s, e := openSQLStore(ctx, db, sqlstore.SQLite, opts)
				require.NoError(t, e)
				return s
			}
			store := open(true)
			wrapped := &observeWaitStore{Store: store, UserAuthStore: store.(core.UserAuthStore)}
			b := userBinding()
			remote := &conformance.MAServer{EnvironmentType: "cloud"}
			client := &userVaultMA{MAClient: newMAForHarness(t, remote)}
			a := &failPromptOnceAuthorizer{promptRecoveryAuthorizer: &promptRecoveryAuthorizer{userAuthFixture: &userAuthFixture{lookupReauth: true, store: wrapped}}, store: wrapped, crypt: crypt}
			opts := restartRuntimeOptions()
			opts.MessageDeadline = time.Minute
			connection := core.Connection{Binding: b, Provider: &runtimeProvider{}, MAClient: client, UserAuthorizer: a}
			r, e := core.NewRuntime(wrapped, []core.Connection{connection}, opts)
			require.NoError(t, e)
			input := mustEnqueue(t, r, b.ID, runtimeMessage())
			runCtx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- r.Run(runCtx) }()
			require.Eventually(t, wrapped.failed.Load, time.Second, 10*time.Millisecond)
			select {
			case runErr := <-done:
				require.ErrorContains(t, runErr, "injected seal failure")
			case <-time.After(time.Second):
				cancel()
				t.Fatal("store failure did not stop runtime")
			}
			cancel()
			crypt.Fail.Store(false)
			gate, e := wrapped.GetUserAuth(ctx, userPrincipalFixture(b))
			require.NoError(t, e)
			require.Equal(t, "flow", gate.FlowID)
			turn, e := store.GetTurn(ctx, input.Turn.ID)
			require.NoError(t, e)
			require.Nil(t, turn.AuthWait)
			require.NoError(t, store.Close())
			store = open(false)
			defer func() { require.NoError(t, store.Close()) }()
			a.store = store.(core.UserAuthStore)
			a.userAuthFixture.store = a.store
			// clean restart 保留原 fence，等原 lease 到期后继续，不伪造远端 fence。
			second, e := core.NewRuntime(store, []core.Connection{connection}, opts)
			require.NoError(t, e)
			runCtx, cancel = context.WithCancel(ctx)
			done = make(chan error, 1)
			go func() { done <- second.Run(runCtx) }()
			defer func() { cancel(); require.NoError(t, <-done) }()
			require.Eventually(t, func() bool { turn, e := store.GetTurn(ctx, input.Turn.ID); return e == nil && turn.AuthWait != nil }, 15*time.Second, 10*time.Millisecond)
			require.EqualValues(t, 1, a.starts.Load())
			require.EqualValues(t, 1, a.reads.Load())
			deliveries, e := store.ListDeliveries(ctx, core.DeliveryQuery{})
			require.NoError(t, e)
			require.Len(t, deliveries, 1)
			raw, e := json.Marshal(deliveries[0])
			require.NoError(t, e)
			require.Contains(t, string(raw), "ORIGINAL-CODE")
			a.mu.Lock()
			a.ready = true
			a.mu.Unlock()
			remote.AppendAfterInput(t, conformance.AnswerEvents("authorized")...)
			require.Eventually(t, func() bool {
				turn, e := store.GetTurn(ctx, input.Turn.ID)
				return e == nil && turn.Status == core.TurnDone
			}, 15*time.Second, 10*time.Millisecond)
			turn, e = store.GetTurn(ctx, input.Turn.ID)
			require.NoError(t, e)
			require.Equal(t, input.Turn.InputEventID, turn.InputEventID)
			remote.Lock()
			require.Equal(t, 1, remote.CreateCount)
			require.Equal(t, 1, remote.SendCount)
			remote.Unlock()
		})
	}
}
