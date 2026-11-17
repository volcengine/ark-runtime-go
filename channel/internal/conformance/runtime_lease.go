// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

type leaseRuntimeProvider struct {
	count     atomic.Int32
	delivered chan struct{}
}

func (p *leaseRuntimeProvider) Start(ctx context.Context, _ func(context.Context, core.Message) error) error {
	<-ctx.Done()
	return ctx.Err()
}
func (p *leaseRuntimeProvider) Deliver(_ context.Context, _ core.Delivery) (string, error) {
	p.count.Add(1)
	select {
	case p.delivered <- struct{}{}:
	default:
	}
	return "remote", nil
}

// runtimeTakeover 验证真实 Store 的旧 fence 不能继续创建，且共享 Runtime 仍处理其它 Channel。
func runtimeTakeover(t *testing.T, open opener) {
	s, clock := open(t, StoreConfig{})
	f := &MAServer{}
	server := httptest.NewServer(f)
	defer server.Close()
	client, err := ma.NewClient(server.URL, "test", ma.ClientOptions{})
	require.NoError(t, err)
	p := &leaseRuntimeProvider{delivered: make(chan struct{}, 2)}
	binding := SampleBinding()
	binding.AgentVersion = "1"
	binding.EnvironmentID = "env"
	other := binding
	other.ID = "other"
	other.AppID = "other-app"
	connections := []core.Connection{{Binding: binding, MAClient: client, Provider: p}, {Binding: other, MAClient: client, Provider: p}}
	// 失租由逻辑时钟驱动；真实调用预算不承担亚秒级存储性能断言。
	opts := core.RuntimeOptions{Workers: 1, PollInterval: 5 * time.Millisecond, TurnTimeout: 5 * time.Second, LeaseTTL: 30 * time.Second, DeliveryTimeout: time.Second, MessageDeadline: 5 * time.Minute, Clock: clock.Now}
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	stale := append([]core.Connection(nil), connections...)
	stale[0].PrepareSession = func(_ context.Context, b core.Binding) (core.Binding, error) {
		close(entered)
		<-release
		close(returned)
		return b, nil
	}
	first, err := core.NewRuntime(s, stale, opts)
	require.NoError(t, err)
	message := SampleMessage("takeover")
	message.Text = "/new"
	accepted, err := first.Enqueue(context.Background(), binding.ID, message)
	require.NoError(t, err)
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.Run(firstCtx) }()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		cancelFirst()
		err := <-firstDone
		require.NoError(t, err, "underlying runtime error: %v", errors.Unwrap(errors.Unwrap(err)))
	})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first runtime never reached preparation")
	}
	clock.Advance(opts.LeaseTTL + time.Second)
	second, err := core.NewRuntime(s, connections, opts)
	require.NoError(t, err)
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.Run(secondCtx) }()
	t.Cleanup(func() {
		cancelSecond()
		err := <-secondDone
		require.NoError(t, err, "underlying runtime error: %v", errors.Unwrap(errors.Unwrap(err)))
	})
	select {
	case <-p.delivered:
	case <-time.After(5 * time.Second):
		t.Fatal("takeover runtime did not deliver the new session")
	}
	turn, err := s.GetTurn(context.Background(), accepted.Turn.ID)
	require.NoError(t, err)
	require.Equal(t, core.TurnDone, turn.Status)
	require.Equal(t, int32(1), p.count.Load())
	releaseOnce.Do(func() { close(release) })
	<-returned
	message = SampleMessage("other")
	message.BindingID = other.ID
	message.Text = "/new"
	_, err = first.Enqueue(context.Background(), other.ID, message)
	require.NoError(t, err)
	select {
	case <-p.delivered:
	case <-time.After(5 * time.Second):
		t.Fatal("shared runtime did not deliver for the other channel")
	}
	require.Equal(t, int32(2), p.count.Load())
	f.Lock()
	defer f.Unlock()
	require.Equal(t, 2, f.CreateCount, "stale preparation must not produce a third POST")
}
