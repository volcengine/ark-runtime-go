// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	memory "github.com/volcengine/ark-runtime-go/channel/internal/testutil/memorystore"
)

// identityMaintenanceStore 保留真实持久语义，只阻止执行 Worker 消费续期 fixture。
type identityMaintenanceStore struct {
	core.Store
	listErr     error
	snapshotErr error
}

func (s identityMaintenanceStore) LeaseTurn(context.Context, core.ClaimOptions) (*core.TurnLease, error) {
	return nil, nil
}
func (s identityMaintenanceStore) ListTurns(ctx context.Context, q core.TurnQuery) ([]core.Turn, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.Store.ListTurns(ctx, q)
}
func (s identityMaintenanceStore) GetSessionSnapshot(ctx context.Context, id string) (*core.SessionSnapshot, error) {
	if s.snapshotErr != nil {
		return nil, s.snapshotErr
	}
	return s.Store.GetSessionSnapshot(ctx, id)
}

type maintenanceProvider struct {
	runtimeProvider
	calls   atomic.Int32
	expires func() time.Time
	fail    bool
	failure error
	block   bool
	entered chan struct{}
}

func (p *maintenanceProvider) BotToken(ctx context.Context) (string, time.Time, error) {
	n := p.calls.Add(1)
	if p.entered != nil {
		select {
		case p.entered <- struct{}{}:
		default:
		}
	}
	if p.block {
		<-ctx.Done()
		return "", time.Time{}, ctx.Err()
	}
	if p.failure != nil {
		return "", time.Time{}, p.failure
	}
	if p.fail {
		return "", time.Time{}, errors.New("application token unavailable")
	}
	expires := time.Now().Add(2 * time.Hour)
	if p.expires != nil {
		expires = p.expires()
	}
	return fmt.Sprintf("token-%d", n), expires, nil
}

type maintenanceResolver struct {
	core.ConnectionResolver
	mu          sync.Mutex
	connections map[uint64]core.Connection
	err         error
	resolves    atomic.Int32
	releases    atomic.Int32
}

func (r *maintenanceResolver) ResolveForTurn(_ context.Context, _ core.Turn, snapshot *core.SessionSnapshot) (core.Connection, error) {
	r.resolves.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	if snapshot == nil {
		return core.Connection{}, errors.New("missing frozen identity")
	}
	c := r.connections[snapshot.Spec.Binding.BindingEpoch]
	c.Release = func() { r.releases.Add(1) }
	return c, r.err
}

func maintenanceBinding(epoch uint64) core.Binding {
	b := identityBinding()
	b.Owner = "account"
	b.Project = "project"
	b.ChannelID = "channel"
	b.BindingEpoch = epoch
	b.BotCredential.VaultID = fmt.Sprintf("vault-%d", epoch)
	b.VaultIDs = []string{b.BotCredential.VaultID}
	return b
}
func seedIdentityMaintenance(t *testing.T, store core.Store, b core.Binding, index int, ready ...bool) core.Turn {
	t.Helper()
	ctx := context.Background()
	message := runtimeMessage()
	message.ChatID = fmt.Sprintf("chat-%d", index)
	message.MessageID = fmt.Sprintf("message-%d", index)
	message.ProviderEventID = message.MessageID
	message.TenantKey = "tenant"
	message.BindingID = b.ID
	_, err := store.Ingest(ctx, core.NewBindingVersion(b), message)
	require.NoError(t, err)
	leased, err := store.LeaseTurn(ctx, conformance.Claim("freeze"))
	require.NoError(t, err)
	require.NotNil(t, leased)
	turn, err := store.UpdateTurn(ctx, leased.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: fmt.Sprintf("creation-%d", index)}})
	if err != nil {
		current, readErr := store.GetTurn(ctx, leased.Turn.ID)
		t.Logf("seed %d id=%s lease=%+v current=%+v read_error=%v", index, leased.Turn.ID, leased.Turn.Lease, current, readErr)
	}
	require.NoError(t, err)
	snapshot := core.NewSessionSnapshot(turn.ConversationID, 1, core.CreateSessionSpec{Binding: b, CreationKey: fmt.Sprintf("creation-%d", index), LookupKey: "lookup", ChatType: "p2p"})
	_, err = store.FreezeSession(ctx, turn.Lease, snapshot)
	require.NoError(t, err)
	if len(ready) > 0 && ready[0] {
		require.NoError(t, store.ReleaseTurn(ctx, turn.Lease))
	} else {
		require.NoError(t, store.WaitTurn(ctx, turn.Lease, core.TurnWait{Reason: "long_task", Until: time.Now().Add(time.Hour)}))
	}
	saved, err := store.GetTurn(ctx, turn.ID)
	require.NoError(t, err)
	return *saved
}
func newIdentityMaintenanceStore(t *testing.T) core.Store {
	t.Helper()
	s, err := memory.New(memory.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}
func startIdentityMaintenance(t *testing.T, s core.Store, resolver core.ConnectionResolver, options core.RuntimeOptions) (*core.Runtime, context.CancelFunc, <-chan error) {
	t.Helper()
	options.Resolver = resolver
	runtime, err := core.NewRuntime(s, nil, options)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { defer close(done); done <- runtime.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err, ok := <-done:
			if ok {
				require.NoError(t, err)
			}
		case <-time.After(5 * time.Second):
			t.Error("identity runtime did not join after cancellation")
		}
	})
	return runtime, cancel, done
}

// delayedIdentityScanStore 模拟合法的启动扫描延迟，延迟不消耗后续单主体续期的调用预算。
type delayedIdentityScanStore struct {
	core.Store
	delay time.Duration
}

func (s delayedIdentityScanStore) ListTurns(ctx context.Context, q core.TurnQuery) ([]core.Turn, error) {
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
	}
	return s.Store.ListTurns(ctx, q)
}

type maintenanceRefreshObserver struct{ completed atomic.Int32 }

func (o *maintenanceRefreshObserver) Observe(_ context.Context, event core.Observation) {
	if event.Name == core.EventAuth && event.Status == "refreshed" {
		o.completed.Add(1)
	}
}

func TestDynamicIdentityMaintenanceRestoresFrozenEpochsAfterRestart(t *testing.T) {
	for _, tc := range []struct {
		name      string
		scanDelay time.Duration
	}{{"immediate_scan", 0}, {"delayed_scan", 1250 * time.Millisecond}} {
		t.Run(tc.name, func(t *testing.T) {
			s := newIdentityMaintenanceStore(t)
			old, newer := maintenanceBinding(1), maintenanceBinding(2)
			seedIdentityMaintenance(t, s, old, 1)
			seedIdentityMaintenance(t, s, old, 2)
			seedIdentityMaintenance(t, s, newer, 3)
			oldProvider, newProvider := &maintenanceProvider{}, &maintenanceProvider{}
			oldVault, newVault := &vaultMA{}, &vaultMA{}
			resolver := &maintenanceResolver{connections: map[uint64]core.Connection{
				1: {Binding: old, Provider: oldProvider, MAClient: oldVault}, 2: {Binding: newer, Provider: newProvider, MAClient: newVault},
			}}
			refreshed := &maintenanceRefreshObserver{}
			options := testRuntimeOptions()
			// 本用例验证恢复/去重，不测试1s SLA；给race+coverage下的真实Store读取保留明确预算。
			options.DeliveryTimeout = time.Second // 单主体维护预算为5s。
			options.Observer = refreshed
			waitBudget := 2 * options.DeliveryTimeout * 5 // 外层同时覆盖启动扫描、主体维护和完成通知。
			for i := 0; i < 2; i++ {
				started := time.Now()
				wrapped := identityMaintenanceStore{Store: delayedIdentityScanStore{Store: s, delay: tc.scanDelay}}
				_, cancel, done := startIdentityMaintenance(t, wrapped, resolver, options)
				want := int32(i + 1)
				deadline := time.NewTimer(waitBudget)
				defer deadline.Stop()
				poll := time.NewTicker(10 * time.Millisecond)
				defer poll.Stop()
			awaitRefresh:
				for {
					oldVault.vmu.Lock()
					oldWrites := len(oldVault.writes)
					oldVault.vmu.Unlock()
					newVault.vmu.Lock()
					newWrites := len(newVault.writes)
					newVault.vmu.Unlock()
					if refreshed.completed.Load() == 2*want && oldWrites == int(want) && newWrites == int(want) && resolver.resolves.Load() == 3*want && resolver.releases.Load() == 3*want {
						break awaitRefresh
					}
					select {
					case err := <-done:
						t.Fatalf("runtime ended before completed renewal: %v (deadline=%t, canceled=%t)", err, errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled))
					case <-deadline.C:
						t.Fatalf("renewal incomplete after %s: refreshed=%d vault writes=(%d,%d) token calls=(%d,%d) resolved=%d released=%d", waitBudget, refreshed.completed.Load(), oldWrites, newWrites, oldProvider.calls.Load(), newProvider.calls.Load(), resolver.resolves.Load(), resolver.releases.Load())
					case <-poll.C:
					}
				}
				deadline.Stop()
				poll.Stop()
				cancel()
				require.NoError(t, <-done)
				require.Equal(t, want, oldProvider.calls.Load(), "duplicate frozen Turns share exactly one refresh per runtime")
				require.Equal(t, want, newProvider.calls.Load(), "each epoch refreshes exactly once per runtime")
				t.Logf("restart=%d completed two frozen epochs and three source references in %s", i, time.Since(started))
			}
			require.Equal(t, int32(4), refreshed.completed.Load())
			require.Equal(t, int32(6), resolver.resolves.Load())
			require.Equal(t, resolver.resolves.Load(), resolver.releases.Load())
			require.Equal(t, int32(2), oldProvider.calls.Load())
			require.Equal(t, int32(2), newProvider.calls.Load())
			oldVault.vmu.Lock()
			defer oldVault.vmu.Unlock()
			newVault.vmu.Lock()
			defer newVault.vmu.Unlock()
			require.Len(t, oldVault.writes, 2, "duplicate Turns share one credential refresh per process")
			require.Len(t, newVault.writes, 2)
			require.Contains(t, oldVault.writes[0], "vault-1/")
			require.Contains(t, newVault.writes[0], "vault-2/")
		})
	}
}

func TestDynamicIdentityMaintenanceStoreFailureTerminatesRuntime(t *testing.T) {
	for _, kind := range []string{"list", "snapshot", "resolver"} {
		t.Run(kind, func(t *testing.T) {
			s := newIdentityMaintenanceStore(t)
			b := maintenanceBinding(1)
			seedIdentityMaintenance(t, s, b, 1)
			sentinel := errors.New("persistent store unavailable")
			wrapped := identityMaintenanceStore{Store: s}
			resolver := &maintenanceResolver{connections: map[uint64]core.Connection{1: {Binding: b, Provider: &maintenanceProvider{}, MAClient: &vaultMA{}}}}
			switch kind {
			case "list":
				wrapped.listErr = sentinel
			case "snapshot":
				wrapped.snapshotErr = sentinel
			case "resolver":
				resolver.err = core.NewStoreFailure(sentinel)
			}
			_, cancel, done := startIdentityMaintenance(t, wrapped, resolver, testRuntimeOptions())
			defer cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, sentinel)
				var failure core.StoreFailure
				require.ErrorAs(t, err, &failure)
			case <-time.After(time.Second):
				t.Fatal("identity Store failure must be returned by Runtime.Run")
			}
			require.Equal(t, resolver.resolves.Load(), resolver.releases.Load())
		})
	}
}

func TestDynamicIdentityWarmRejectsUnsupportedUser(t *testing.T) {
	s := newIdentityMaintenanceStore(t)
	opts := testRuntimeOptions()
	opts.Resolver = &maintenanceResolver{}
	runtime, err := core.NewRuntime(s, nil, opts)
	require.NoError(t, err)
	b := maintenanceBinding(1)
	b.Identity = core.IdentityPolicy{P2P: core.IdentityUser}
	require.ErrorIs(t, runtime.WarmBotIdentity(context.Background(), core.Connection{Binding: b}, "p2p"), core.ErrIdentityUnsupported)
}

func TestDynamicIdentityMaintenanceRenewsBeforeExpiry(t *testing.T) {
	s := newIdentityMaintenanceStore(t)
	b := maintenanceBinding(1)
	seedIdentityMaintenance(t, s, b, 1)
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	clock := func() time.Time { return time.Unix(0, now.Load()) }
	provider := &maintenanceProvider{expires: func() time.Time { return clock().Add(2 * time.Hour) }}
	vault := &vaultMA{}
	resolver := &maintenanceResolver{connections: map[uint64]core.Connection{1: {Binding: b, Provider: provider, MAClient: vault}}}
	options := testRuntimeOptions()
	options.Clock = clock
	options.IdentityRenewalInterval = 5 * time.Millisecond
	_, cancel, done := startIdentityMaintenance(t, identityMaintenanceStore{Store: s}, resolver, options)
	defer func() { cancel(); require.NoError(t, <-done) }()
	require.Eventually(t, func() bool { return provider.calls.Load() == 1 }, time.Second, 5*time.Millisecond)
	now.Add(int64(109 * time.Minute))
	require.Eventually(t, func() bool { return resolver.resolves.Load() > 3 }, time.Second, 5*time.Millisecond)
	require.Equal(t, int32(1), provider.calls.Load(), "unexpired credential is reused across checks")
	now.Add(int64(2 * time.Minute))
	require.Eventually(t, func() bool { return provider.calls.Load() == 2 }, time.Second, 5*time.Millisecond, "refresh starts before expiration margin")
}

func TestDynamicIdentityMaintenanceIsolatesFailureAndReleasesCancellation(t *testing.T) {
	for _, kind := range []string{"token", "vault", "resolver", "blocked"} {
		t.Run(kind, func(t *testing.T) {
			s := newIdentityMaintenanceStore(t)
			old, newer := maintenanceBinding(1), maintenanceBinding(2)
			seedIdentityMaintenance(t, s, old, 1)
			seedIdentityMaintenance(t, s, newer, 2)
			bad, good := &maintenanceProvider{}, &maintenanceProvider{}
			badVault := &maintenanceVault{fail: kind == "vault"}
			goodVault := &vaultMA{}
			if kind == "token" {
				bad.fail = true
			}
			if kind == "blocked" {
				bad.block = true
				bad.entered = make(chan struct{}, 1)
			}
			resolver := &selectiveMaintenanceResolver{maintenanceResolver: maintenanceResolver{connections: map[uint64]core.Connection{
				1: {Binding: old, Provider: bad, MAClient: badVault}, 2: {Binding: newer, Provider: good, MAClient: goodVault},
			}}, failFirst: kind == "resolver"}
			options := testRuntimeOptions()
			options.IdentityRenewalConcurrency = 2
			_, cancel, done := startIdentityMaintenance(t, identityMaintenanceStore{Store: s}, resolver, options)
			require.Eventually(t, func() bool { return good.calls.Load() == 1 }, time.Second, 5*time.Millisecond, "one subject must not stop another")
			if kind == "blocked" {
				select {
				case <-bad.entered:
				case <-time.After(time.Second):
					t.Fatal("blocked identity was not called")
				}
			}
			select {
			case err := <-done:
				t.Fatalf("single principal failure stopped runtime: %v", err)
			default:
			}
			cancel()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("cancel must join all identity work")
			}
			require.Equal(t, resolver.resolves.Load(), resolver.releases.Load(), "all acquired connections are released, including failed resolution and cancellation")
			goodVault.vmu.Lock()
			defer goodVault.vmu.Unlock()
			require.Len(t, goodVault.writes, 1)
		})
	}
}

type maintenanceVault struct {
	core.MAClient
	fail    bool
	failure error
}

func (v *maintenanceVault) UpsertEnvCredential(context.Context, core.BotCredentialSpec, string) (string, error) {
	if v.failure != nil {
		return "", v.failure
	}
	if v.fail {
		return "", errors.New("vault unavailable")
	}
	return "credential", nil
}

type selectiveMaintenanceResolver struct {
	maintenanceResolver
	failFirst bool
}

func (r *selectiveMaintenanceResolver) ResolveForTurn(ctx context.Context, turn core.Turn, snapshot *core.SessionSnapshot) (core.Connection, error) {
	c, err := r.maintenanceResolver.ResolveForTurn(ctx, turn, snapshot)
	if snapshot.Spec.Binding.BindingEpoch == 1 && r.failFirst {
		return c, errors.New("one application unavailable")
	}
	return c, err
}

func TestStaticIdentityMaintenanceSupportsP2POnlyBot(t *testing.T) {
	s := newIdentityMaintenanceStore(t)
	b := maintenanceBinding(1)
	b.Identity = core.IdentityPolicy{P2P: core.IdentityBot}
	provider := &maintenanceProvider{}
	vault := &vaultMA{}
	runtime, err := core.NewRuntime(s, []core.Connection{{Binding: b, Provider: provider, MAClient: vault}}, testRuntimeOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	defer func() { cancel(); require.NoError(t, <-done) }()
	require.Eventually(t, func() bool { return provider.calls.Load() == 1 }, time.Second, 5*time.Millisecond)
}

func TestIdentityMaintenanceOptionsHaveFiniteBounds(t *testing.T) {
	defaults, err := (core.RuntimeOptions{}).Normalize()
	require.NoError(t, err)
	require.Equal(t, time.Minute, defaults.IdentityRenewalInterval)
	require.Equal(t, 4, defaults.IdentityRenewalConcurrency)
	for _, options := range []core.RuntimeOptions{
		{IdentityRenewalInterval: -time.Second}, {IdentityRenewalInterval: 11 * time.Minute},
		{IdentityRenewalConcurrency: -1}, {IdentityRenewalConcurrency: 33},
	} {
		_, err := options.Normalize()
		require.ErrorIs(t, err, core.ErrRuntimeConfig)
	}
}

func TestDynamicIdentityMaintenanceHasBoundedConcurrency(t *testing.T) {
	s := newIdentityMaintenanceStore(t)
	resolver := &maintenanceResolver{connections: map[uint64]core.Connection{}}
	entered := make(chan struct{}, 16)
	for i := 1; i <= 9; i++ {
		b := maintenanceBinding(uint64(i))
		seedIdentityMaintenance(t, s, b, i)
		resolver.connections[uint64(i)] = core.Connection{Binding: b, Provider: &maintenanceProvider{block: true, entered: entered}, MAClient: &vaultMA{}}
	}
	options := testRuntimeOptions()
	options.IdentityRenewalConcurrency = 2
	_, cancel, done := startIdentityMaintenance(t, identityMaintenanceStore{Store: s}, resolver, options)
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("identity workers did not start")
		}
	}
	require.Equal(t, int32(2), resolver.resolves.Load(), "blocked identity work must hold finite worker slots")
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("blocked workers leaked on cancellation")
	}
	require.Equal(t, resolver.resolves.Load(), resolver.releases.Load())
}

func TestDynamicIdentityMaintenanceKeepsFrozenVaultInsteadOfCurrentDefaults(t *testing.T) {
	s := newIdentityMaintenanceStore(t)
	frozen := maintenanceBinding(1)
	seedIdentityMaintenance(t, s, frozen, 1)
	current := frozen
	current.BotCredential.VaultID = "new-default-vault"
	current.VaultIDs = []string{"new-default-vault"}
	provider := &maintenanceProvider{}
	vault := &vaultMA{}
	resolver := &maintenanceResolver{connections: map[uint64]core.Connection{1: {Binding: current, Provider: provider, MAClient: vault}}}
	_, cancel, done := startIdentityMaintenance(t, identityMaintenanceStore{Store: s}, resolver, testRuntimeOptions())
	require.Eventually(t, func() bool { vault.vmu.Lock(); defer vault.vmu.Unlock(); return len(vault.writes) == 1 }, time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	vault.vmu.Lock()
	defer vault.vmu.Unlock()
	require.Contains(t, vault.writes[0], "vault-1/")
	require.NotContains(t, vault.writes[0], "new-default-vault")
}

func TestDynamicIdentityMaintenancePreservesBoundaryStoreFailures(t *testing.T) {
	for _, kind := range []string{"token", "vault"} {
		t.Run(kind, func(t *testing.T) {
			s := newIdentityMaintenanceStore(t)
			b := maintenanceBinding(1)
			seedIdentityMaintenance(t, s, b, 1)
			cause := errors.New("identity backing store failed")
			failure := core.NewStoreFailure(cause)
			provider := &maintenanceProvider{}
			vault := &maintenanceVault{}
			if kind == "token" {
				provider.failure = failure
			} else {
				vault.failure = failure
			}
			resolver := &maintenanceResolver{connections: map[uint64]core.Connection{1: {Binding: b, Provider: provider, MAClient: vault}}}
			_, cancel, done := startIdentityMaintenance(t, identityMaintenanceStore{Store: s}, resolver, testRuntimeOptions())
			defer cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, cause)
			case <-time.After(time.Second):
				t.Fatal("identity boundary StoreFailure was swallowed")
			}
			require.Equal(t, resolver.resolves.Load(), resolver.releases.Load())
		})
	}
}

func TestStaticIdentityMaintenancePropagatesStoreFailure(t *testing.T) {
	s := newIdentityMaintenanceStore(t)
	b := maintenanceBinding(1)
	cause := errors.New("static identity storage failed")
	runtime, err := core.NewRuntime(s, []core.Connection{{Binding: b, Provider: &maintenanceProvider{failure: core.NewStoreFailure(cause)}, MAClient: &vaultMA{}}}, testRuntimeOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, cause)
	case <-time.After(time.Second):
		t.Fatal("static identity StoreFailure was swallowed")
	}
}

// workerIdentityStore 让后台扫描看不到 fixture，确保错误由真实 Turn worker 返回。
type workerIdentityStore struct{ core.Store }

func (workerIdentityStore) ListTurns(context.Context, core.TurnQuery) ([]core.Turn, error) {
	return nil, nil
}
func (v *maintenanceVault) GetEnvironment(ctx context.Context, id string) (core.EnvironmentConfig, error) {
	return v.MAClient.(core.EnvironmentReader).GetEnvironment(ctx, id)
}
func TestDynamicIdentityTurnWorkerPreservesStoreFailure(t *testing.T) {
	for _, stage := range []string{"first_create", "frozen_resume"} {
		t.Run(stage, func(t *testing.T) {
			s := newIdentityMaintenanceStore(t)
			b := maintenanceBinding(1)
			if stage == "frozen_resume" {
				seedIdentityMaintenance(t, s, b, 1, true)
			} else {
				_, err := s.Ingest(context.Background(), core.NewBindingVersion(b), runtimeMessage())
				require.NoError(t, err)
			}
			cause := errors.New("worker credential storage failed")
			vault := &maintenanceVault{MAClient: newMAForHarness(t, &conformance.MAServer{EnvironmentType: "cloud"}), failure: core.NewStoreFailure(cause)}
			provider := &maintenanceProvider{}
			resolver := fixtureResolver{connections: map[string]core.Connection{b.ID: {Binding: b, MAClient: vault, Provider: provider}}}
			_, cancel, done := startIdentityMaintenance(t, workerIdentityStore{Store: s}, resolver, testRuntimeOptions())
			defer cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, cause)
			case <-time.After(time.Second):
				t.Fatal("Turn worker swallowed identity StoreFailure")
			}
		})
	}
}

func TestDynamicIdentityMaintenanceDeduplicatesFailingPrincipal(t *testing.T) {
	s := newIdentityMaintenanceStore(t)
	b := maintenanceBinding(1)
	for i := 1; i <= 3; i++ {
		seedIdentityMaintenance(t, s, b, i)
	}
	provider := &maintenanceProvider{fail: true}
	resolver := &maintenanceResolver{connections: map[uint64]core.Connection{1: {Binding: b, Provider: provider, MAClient: &vaultMA{}}}}
	_, cancel, done := startIdentityMaintenance(t, identityMaintenanceStore{Store: s}, resolver, testRuntimeOptions())
	require.Eventually(t, func() bool { return resolver.releases.Load() == 3 }, time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	require.Equal(t, int32(1), provider.calls.Load(), "a failed principal must consume only one remote attempt per renewal interval")
}
