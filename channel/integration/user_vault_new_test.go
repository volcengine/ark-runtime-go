// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	memory "github.com/volcengine/ark-runtime-go/channel/internal/testutil/memorystore"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

type replacementVaultMA struct {
	core.MAClient
	mu          sync.Mutex
	oldID       string
	vault       core.VaultRef
	creates     int
	displayName string
}

func (m *replacementVaultMA) GetVault(_ context.Context, id string) (core.VaultRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if id == m.oldID {
		return core.VaultRef{}, &ma.APIError{Status: 404, Code: "InvalidVault.NotFound"}
	}
	if id == m.vault.ID && id != "" {
		return m.vault, nil
	}
	return core.VaultRef{}, core.ErrNotFound
}
func (m *replacementVaultMA) FindVaultByName(_ context.Context, name string) (core.VaultRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if name == m.vault.Name {
		return m.vault, nil
	}
	return core.VaultRef{}, nil
}
func (m *replacementVaultMA) CreateVault(_ context.Context, spec core.BotVaultCreate) (core.VaultRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.creates++
	m.displayName = spec.DisplayName
	m.vault = core.VaultRef{ID: "replacement-vault", Name: spec.Name, Principal: spec.Metadata["ark_channel_principal"]}
	return m.vault, nil
}
func (m *replacementVaultMA) GetEnvironment(ctx context.Context, id string) (core.EnvironmentConfig, error) {
	return m.MAClient.(core.EnvironmentReader).GetEnvironment(ctx, id)
}

type replacementAuthorizer struct {
	*userAuthFixture
	oldID string
}

func (a *replacementAuthorizer) Start(ctx context.Context, p core.UserPrincipal, req core.AuthRequest) (core.AuthFlow, error) {
	if req.VaultID == a.oldID {
		return core.AuthFlow{}, &ma.APIError{Status: 404, Code: "InvalidVault.NotFound"}
	}
	if req.ExistingCredentialID != "" {
		return core.AuthFlow{}, core.ErrConflict
	}
	return a.userAuthFixture.Start(ctx, p, req)
}

func TestNewReauthorizesDeletedUserVaultAfterRestart(t *testing.T) {
	for _, kind := range []string{"sql", "embedded"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			s := userRuntimeStore(t, kind)
			auth := s.(core.UserAuthStore)
			b := userBinding()
			principal := userPrincipalFixture(b)
			old := b
			old.VaultIDs = []string{"deleted-vault"}
			snapshot := seedUserSession(t, s, old, &core.UserCredentialSpec{Principal: principal, VaultID: "deleted-vault", CredentialID: "deleted-credential", GrantVersion: "old", FlowID: "old-flow"})
			held, err := auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: principal, VaultID: "deleted-vault", Owner: "seed-auth", TTL: time.Minute})
			require.NoError(t, err)
			_, err = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: principal, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthReady, Grant: core.UserGrant{Status: core.AuthReady, VaultID: "deleted-vault", CredentialID: "deleted-credential", GrantVersion: "old", FlowID: "old-flow", GrantedScopes: []string{"read"}}})
			require.NoError(t, err)
			f := &conformance.MAServer{EnvironmentType: "cloud", CreateSessionID: "sesn-replacement"}
			client := &replacementVaultMA{MAClient: newMAForHarness(t, f), oldID: "deleted-vault"}
			a := &replacementAuthorizer{userAuthFixture: &userAuthFixture{flow: "replacement-flow", version: "replacement-grant"}, oldID: "deleted-vault"}
			provider := &runtimeProvider{}
			connection := core.Connection{Binding: b, MAClient: client, Provider: provider, UserAuthorizer: a}
			options := restartRuntimeOptions()
			options.MessageDeadline = time.Minute
			first, err := core.NewRuntime(s, []core.Connection{connection}, options)
			require.NoError(t, err)
			msg := runtimeMessage()
			msg.Text = "/new"
			result := mustEnqueue(t, first, b.ID, msg)
			t.Cleanup(logUserAuthOnFailure(t, func() core.Store { return s }, result.Turn.ID, principal))
			runCtx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- first.Run(runCtx) }()
			defer func() {
				cancel()
				if done != nil {
					require.NoError(t, <-done)
				}
			}()
			require.Eventually(t, func() bool { turn, e := s.GetTurn(ctx, result.Turn.ID); return e == nil && turn.AuthWait != nil }, 15*time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool {
				ds, e := s.ListDeliveries(ctx, core.DeliveryQuery{})
				for _, d := range ds {
					if d.Status == core.DeliveryDone && d.Source.Kind == core.SourceProgress {
						return e == nil
					}
				}
				return false
			}, 15*time.Second, 10*time.Millisecond)
			saved, err := auth.GetUserAuth(ctx, principal)
			require.NoError(t, err)
			require.Equal(t, "replacement-vault", saved.VaultID)
			client.mu.Lock()
			require.Equal(t, "Channel User · "+principal.OpenID, client.displayName)
			client.mu.Unlock()
			require.Empty(t, saved.Grant.CredentialID)
			a.mu.Lock()
			require.Equal(t, "replacement-vault", a.request.VaultID)
			require.Empty(t, a.request.ExistingCredentialID)
			require.Equal(t, 1, a.starts)
			a.mu.Unlock()
			f.Lock()
			require.Zero(t, f.CreateCount)
			require.Zero(t, f.SendCount)
			f.Unlock()
			cancel()
			require.NoError(t, <-done)
			done = nil
			a.mu.Lock()
			a.ready = true
			a.mu.Unlock()
			restarted, err := core.NewRuntime(s, []core.Connection{connection}, options)
			require.NoError(t, err)
			runUntilCleanup(t, restarted)
			require.Eventually(t, func() bool {
				turn, e := s.GetTurn(ctx, result.Turn.ID)
				return e == nil && turn.Status == core.TurnDone
			}, 15*time.Second, 10*time.Millisecond)
			previous, err := s.GetSessionSnapshot(ctx, snapshot.ID)
			require.NoError(t, err)
			require.Equal(t, "deleted-vault", previous.Spec.UserCredential.VaultID)
			conversation, err := s.GetConversation(ctx, result.Turn.ConversationID)
			require.NoError(t, err)
			require.Equal(t, "sesn-replacement", conversation.Session.SessionID)
			current, err := s.GetSessionSnapshot(ctx, conversation.Session.SnapshotID)
			require.NoError(t, err)
			require.Equal(t, "replacement-vault", current.Spec.UserCredential.VaultID)
			require.NotContains(t, current.Spec.Binding.VaultIDs, "deleted-vault")
			client.mu.Lock()
			require.Equal(t, 1, client.creates)
			require.Equal(t, principal.Key(), client.vault.Principal)
			client.mu.Unlock()
			a.mu.Lock()
			require.Equal(t, 1, a.starts)
			a.mu.Unlock()
			f.Lock()
			require.Equal(t, 1, f.CreateCount)
			require.Zero(t, f.SendCount)
			f.Unlock()
		})
	}
}

func TestUserVaultReplacementRequiresOriginalLeaseAndReference(t *testing.T) {
	for _, kind := range []string{"sql", "embedded"} {
		t.Run(kind, func(t *testing.T) {
			s := userRuntimeStore(t, kind)
			auth := s.(core.UserAuthStore)
			ctx := context.Background()
			p := userPrincipalFixture(userBinding())
			held, err := auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "old-vault", Owner: "initial", TTL: time.Minute})
			require.NoError(t, err)
			_, err = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthReady, Grant: core.UserGrant{Status: core.AuthReady, VaultID: "old-vault", CredentialID: "old-credential", GrantVersion: "old-grant", FlowID: "old-flow"}})
			require.NoError(t, err)
			held, err = auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "old-vault", Owner: "replace", TTL: time.Minute})
			require.NoError(t, err)
			update := core.UserAuthUpdate{Principal: p, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthPending, ReplaceVault: &core.UserVaultReplacement{PreviousVaultID: "old-vault", VaultID: "new-vault"}}
			for _, name := range []string{"lease", "revision", "previous", "empty", "same", "ready", "grant"} {
				t.Run(name, func(t *testing.T) {
					u := update
					replacement := *update.ReplaceVault
					u.ReplaceVault = &replacement
					switch name {
					case "lease":
						u.Lease.Owner = "foreign"
					case "revision":
						u.Revision--
					case "previous":
						u.ReplaceVault.PreviousVaultID = "foreign"
					case "empty":
						u.ReplaceVault.VaultID = ""
					case "same":
						u.ReplaceVault.VaultID = "old-vault"
					case "ready":
						u.State = core.UserAuthReady
					case "grant":
						u.Grant = core.UserGrant{Status: core.AuthReady, VaultID: "new-vault"}
					}
					_, err := auth.UpdateUserAuth(ctx, u)
					require.Error(t, err)
					current, e := auth.GetUserAuth(ctx, p)
					require.NoError(t, e)
					require.Equal(t, held.Revision, current.Revision)
					require.Equal(t, "old-vault", current.VaultID)
				})
			}
			for _, name := range []string{"uncertain", "refresh", "secret"} {
				record := held
				switch name {
				case "uncertain":
					record.State = core.UserAuthUncertain
				case "refresh":
					record.Operation = core.UserAuthRefresh
				case "secret":
					record.State = core.UserAuthVerifying
					record.Secret = &core.UserAuthSecret{Version: 1, KeyID: "test", Ciphertext: []byte("unknown")}
				}
				_, err := record.Apply(update, time.Now())
				require.ErrorIs(t, err, core.ErrUserAuthGate)
			}
			next, err := auth.UpdateUserAuth(ctx, update)
			require.NoError(t, err)
			require.Equal(t, "new-vault", next.VaultID)
			require.Empty(t, next.CredentialID)
			require.Empty(t, next.FlowID)
			require.Empty(t, next.Grant)
			require.Nil(t, next.Secret)
			require.Equal(t, core.UserAuthPending, next.State)
			require.Greater(t, next.Attempt, held.Attempt)
			update.Revision = next.Revision
			_, err = auth.UpdateUserAuth(ctx, update)
			require.Error(t, err, "replaying an old Vault replacement must fail")
		})
	}
}

func seedReplacementAuth(t *testing.T, s core.UserAuthStore, b core.Binding, ttl time.Duration) core.UserPrincipal {
	t.Helper()
	p := userPrincipalFixture(b)
	ctx := context.Background()
	held, err := s.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "deleted-vault", Owner: "seed-auth", TTL: ttl})
	require.NoError(t, err)
	_, err = s.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthReady, Grant: core.UserGrant{Status: core.AuthReady, VaultID: "deleted-vault", CredentialID: "deleted-credential", GrantVersion: "old", FlowID: "old-flow", GrantedScopes: []string{"read"}}})
	require.NoError(t, err)
	return p
}

func TestNewDoesNotDiscardUnconfirmedUserAuthorization(t *testing.T) {
	ctx := context.Background()
	clock := conformance.NewClock(time.Now().UTC())
	s, err := memory.New(memory.Options{Clock: clock.Now, PayloadCodec: &conformance.UserAuthTestCodec{}, PayloadNamespace: "test", RequirePayloadEncryption: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	b := userBinding()
	p := seedReplacementAuth(t, s, b, time.Second)
	held, err := s.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "deleted-vault", Owner: "lost-authorizer", TTL: time.Second})
	require.NoError(t, err)
	secret := &core.UserAuthSecret{Version: 1, KeyID: "test", Ciphertext: []byte("opaque-unknown-intent")}
	_, err = s.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthVerifying, FlowID: "", Secret: secret, ExpiresAt: clock.Now().Add(time.Minute)})
	require.NoError(t, err)
	clock.Advance(3 * time.Second)
	f := &conformance.MAServer{EnvironmentType: "cloud"}
	client := &replacementVaultMA{MAClient: newMAForHarness(t, f), oldID: "deleted-vault"}
	a := &replacementAuthorizer{userAuthFixture: &userAuthFixture{status: core.AuthUnknown}, oldID: "deleted-vault"}
	options := testRuntimeOptions()
	options.Clock = clock.Now
	options.MessageDeadline = time.Minute
	r, err := core.NewRuntime(s, []core.Connection{{Binding: b, MAClient: client, Provider: &runtimeProvider{}, UserAuthorizer: a}}, options)
	require.NoError(t, err)
	msg := runtimeMessage()
	msg.Text = "/new"
	result := mustEnqueue(t, r, b.ID, msg)
	runUntilCleanup(t, r)
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(ctx, result.Turn.ID)
		return e == nil && turn.Status == core.TurnFailed
	}, time.Second, 10*time.Millisecond)
	record, err := s.GetUserAuth(ctx, p)
	require.NoError(t, err)
	require.Equal(t, core.UserAuthVerifying, record.State)
	require.Equal(t, "deleted-vault", record.VaultID)
	require.Equal(t, secret, record.Secret)
	client.mu.Lock()
	require.Zero(t, client.creates)
	client.mu.Unlock()
	a.mu.Lock()
	require.Zero(t, a.starts)
	a.mu.Unlock()
}

type blockedReplacementVaultMA struct {
	*replacementVaultMA
	started     chan struct{}
	postRelease chan struct{}
}

func (m *blockedReplacementVaultMA) CreateVault(ctx context.Context, spec core.BotVaultCreate) (core.VaultRef, error) {
	ref, err := m.replacementVaultMA.CreateVault(ctx, spec)
	m.started <- struct{}{}
	<-m.postRelease
	return ref, err
}

func TestNewVaultCreationIsUniqueAfterSameRuntimeLeaseTakeover(t *testing.T) {
	ctx := context.Background()
	clock := conformance.NewClock(time.Now().UTC())
	s, err := memory.New(memory.Options{Clock: clock.Now, PayloadCodec: &conformance.UserAuthTestCodec{}, PayloadNamespace: "test", RequirePayloadEncryption: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	b := userBinding()
	seedReplacementAuth(t, s, b, time.Second)
	f := &conformance.MAServer{EnvironmentType: "cloud"}
	base := &replacementVaultMA{MAClient: newMAForHarness(t, f), oldID: "deleted-vault"}
	client := &blockedReplacementVaultMA{replacementVaultMA: base, started: make(chan struct{}, 2), postRelease: make(chan struct{})}
	var posts sync.Once
	unblockPosts := func() { posts.Do(func() { close(client.postRelease) }) }
	defer unblockPosts()
	a := &replacementAuthorizer{userAuthFixture: &userAuthFixture{}, oldID: "deleted-vault"}
	options := deadlineRuntimeOptions()
	options.Clock = clock.Now
	options.MessageDeadline = 10 * time.Minute
	r, err := core.NewRuntime(s, []core.Connection{{Binding: b, MAClient: client, Provider: &runtimeProvider{}, UserAuthorizer: a}}, options)
	require.NoError(t, err)
	msg := runtimeMessage()
	msg.Text = "/new"
	result := mustEnqueue(t, r, b.ID, msg)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	defer func() { unblockPosts(); cancel(); require.NoError(t, <-done) }()
	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("first create did not start")
	}
	clock.Advance(3 * time.Second)
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(ctx, result.Turn.ID)
		return e == nil && turn.WaitReason == "user_vault_unavailable"
	}, time.Second, 10*time.Millisecond)
	base.mu.Lock()
	count := base.creates
	base.mu.Unlock()
	require.Equal(t, 1, count, "stale and replacement workers must not both POST")
	unblockPosts()
	// 新 owner 允许等待原创建租约过期后查账；推进虚拟时钟，不放宽真实测试等待。
	clock.Advance(2 * time.Minute)
	require.Eventually(t, func() bool { turn, e := s.GetTurn(ctx, result.Turn.ID); return e == nil && turn.AuthWait != nil }, 2*time.Second, 10*time.Millisecond)
	base.mu.Lock()
	require.Equal(t, 1, base.creates, "lease takeover must not duplicate Vault POST")
	base.mu.Unlock()
}

func TestNewKeepsTemporaryVaultFailuresRetryable(t *testing.T) {
	for _, failure := range []error{&ma.APIError{Status: 403, Code: "Forbidden"}, &ma.APIError{Status: 429, Code: "Throttled"}, &ma.APIError{Status: 500, Code: "InvalidVault.NotFound"}, errors.New("transport unavailable")} {
		t.Run(failure.Error(), func(t *testing.T) {
			s := newUserStore(t)
			b := userBinding()
			p := seedReplacementAuth(t, s, b, time.Minute)
			f := &conformance.MAServer{EnvironmentType: "cloud"}
			base := &replacementVaultMA{MAClient: newMAForHarness(t, f), oldID: "deleted-vault"}
			client := &temporaryVaultMA{replacementVaultMA: base, failure: failure}
			a := &replacementAuthorizer{userAuthFixture: &userAuthFixture{}, oldID: "deleted-vault"}
			options := testRuntimeOptions()
			options.MessageDeadline = time.Minute
			r, err := core.NewRuntime(s, []core.Connection{{Binding: b, MAClient: client, Provider: &runtimeProvider{}, UserAuthorizer: a}}, options)
			require.NoError(t, err)
			msg := runtimeMessage()
			msg.Text = "/new"
			result := mustEnqueue(t, r, b.ID, msg)
			runUntilCleanup(t, r)
			require.Eventually(t, func() bool {
				turn, e := s.GetTurn(context.Background(), result.Turn.ID)
				return e == nil && turn.WaitReason == "user_vault_unavailable"
			}, time.Second, 10*time.Millisecond)
			record, err := s.GetUserAuth(context.Background(), p)
			require.NoError(t, err)
			require.Equal(t, core.UserAuthReady, record.State)
			require.Equal(t, "deleted-vault", record.VaultID)
			base.mu.Lock()
			require.Zero(t, base.creates)
			base.mu.Unlock()
			a.mu.Lock()
			require.Zero(t, a.starts)
			a.mu.Unlock()
		})
	}
}

type temporaryVaultMA struct {
	*replacementVaultMA
	failure error
}

func (m *temporaryVaultMA) GetVault(context.Context, string) (core.VaultRef, error) {
	return core.VaultRef{}, m.failure
}

func TestNewWaitsForUnknownExecutionBeforeCreatingReplacementVault(t *testing.T) {
	ctx := context.Background()
	s := newUserStore(t)
	b := userBinding()
	p := seedReplacementAuth(t, s, b, time.Minute)
	old := b
	old.VaultIDs = []string{"deleted-vault"}
	snapshot := seedUserSession(t, s, old, &core.UserCredentialSpec{Principal: p, VaultID: "deleted-vault", CredentialID: "deleted-credential", GrantVersion: "old", FlowID: "old-flow"})
	msg := runtimeMessage()
	msg.MessageID = "unknown-write"
	msg.ProviderEventID = msg.MessageID
	_, err := s.Ingest(ctx, core.NewBindingVersion(b), msg)
	require.NoError(t, err)
	claim := conformance.Claim("lost-response")
	claim.UserAuth = true
	leased, err := s.LeaseTurn(ctx, claim)
	require.NoError(t, err)
	require.NotNil(t, leased)
	_, err = s.UpdateTurn(ctx, leased.Turn.Lease, core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{SnapshotID: snapshot.ID, Generation: 1, SessionID: "sesn-demo", CreationKey: "seed-create"}, Submit: core.Submission{WriteAttempts: 1, SubmittedAt: time.Now()}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteInput, UserAuthKey: p.Key(), GrantVersion: "old"}})
	require.NoError(t, err)
	require.NoError(t, s.SettleTurn(ctx, leased.Turn.Lease, core.TurnOutcome{Status: core.TurnFailed, ErrorKind: "send_unconfirmed"}, nil))
	f := &conformance.MAServer{EnvironmentType: "cloud"}
	client := &replacementVaultMA{MAClient: newMAForHarness(t, f), oldID: "deleted-vault"}
	a := &replacementAuthorizer{userAuthFixture: &userAuthFixture{}, oldID: "deleted-vault"}
	options := testRuntimeOptions()
	options.MessageDeadline = time.Minute
	r, err := core.NewRuntime(s, []core.Connection{{Binding: b, MAClient: client, Provider: &runtimeProvider{}, UserAuthorizer: a}}, options)
	require.NoError(t, err)
	msg = runtimeMessage()
	msg.Text = "/new"
	result := mustEnqueue(t, r, b.ID, msg)
	runUntilCleanup(t, r)
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(ctx, result.Turn.ID)
		return e == nil && turn.WaitReason == "user_auth_busy"
	}, time.Second, 10*time.Millisecond)
	record, err := s.GetUserAuth(ctx, p)
	require.NoError(t, err)
	require.Equal(t, core.UserAuthDraining, record.State)
	require.Equal(t, "deleted-vault", record.VaultID)
	ops, err := s.ListRemoteOperations(ctx, core.RemoteOperationQuery{OpenOnly: true})
	require.NoError(t, err)
	require.Len(t, ops, 1)
	client.mu.Lock()
	require.Zero(t, client.creates)
	client.mu.Unlock()
	a.mu.Lock()
	require.Zero(t, a.starts)
	a.mu.Unlock()
}
