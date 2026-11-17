// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package channel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/volcengine/ark-runtime-go/channel/internal/handles"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// Connection combines a non-secret Binding, MA Adapter, and channel handle.
type Connection struct {
	Binding  Binding
	MA       *AgentClient
	Provider *ProviderHandle
}

// Runtime runs the durable queue and Outbox without owning the Store lifecycle.
type Runtime struct{ value *core.Runtime }

// ErrRuntimeConfig reports invalid dependencies or runtime options.
var ErrRuntimeConfig = core.ErrRuntimeConfig

// NewRuntime validates and assembles static connections.
func NewRuntime(store *Store, connections []Connection, options Options) (*Runtime, error) {
	if store == nil {
		return nil, ErrRuntimeConfig
	}
	cs := make([]core.Connection, 0, len(connections))
	for _, c := range connections {
		var b core.Binding
		if err := project(c.Binding, &b); err != nil {
			return nil, err
		}
		key, err := handles.BindingKey(c.Binding)
		if err != nil {
			return nil, ErrRuntimeConfig
		}
		if native, bound, valid := handles.BoundBinding(c.Provider, key, store, c.MA); bound {
			if !valid {
				return nil, ErrRuntimeConfig
			}
			b = native
		}
		if b.StreamFinalAnswer {
			if _, ok := handles.ProviderValue(c.Provider).(core.TextStreamProvider); !ok {
				return nil, fmt.Errorf("%w: provider does not support final answer streaming", ErrRuntimeConfig)
			}
		}
		cs = append(cs, core.Connection{Binding: b, MAClient: handles.AgentValue(c.MA), Provider: handles.ProviderValue(c.Provider), UserAuthorizer: handles.Authorizer(c.Provider)})
	}
	// Options has the same domain fields while excluding the internal resolver.
	var opts core.RuntimeOptions
	opts.Workers = options.Workers
	opts.PollInterval = options.PollInterval
	opts.TurnTimeout = options.TurnTimeout
	opts.LeaseTTL = options.LeaseTTL
	opts.DeliveryTimeout = options.DeliveryTimeout
	opts.DeliveryMaxAttempts = options.DeliveryMaxAttempts
	opts.IdleTimeout = options.IdleTimeout
	opts.MessageDeadline = options.MessageDeadline
	opts.UnknownWriteGrace = options.UnknownWriteGrace
	opts.Clock = options.Clock
	opts.Observer = options.Observer
	opts.Retention = options.Retention
	opts.PruneInterval = options.PruneInterval
	opts.ProgressInterval = options.ProgressInterval
	opts.UserAuthPollInterval = options.UserAuthPollInterval
	opts.Attachments = options.Attachments
	opts.Context = options.Context
	opts.Artifacts = options.Artifacts
	opts.Hooks = options.Hooks
	opts.HookTimeout = options.HookTimeout
	opts.HookConcurrency = options.HookConcurrency
	opts.IdentityRenewalInterval = options.IdentityRenewalInterval
	opts.IdentityRenewalConcurrency = options.IdentityRenewalConcurrency
	opts.CreateConcurrency = options.CreateConcurrency
	opts.SubmitConcurrency = options.SubmitConcurrency
	opts.ObserveConcurrency = options.ObserveConcurrency
	opts.AccountLimit = options.AccountLimit
	opts.CreationLimit = options.CreationLimit
	r, err := core.NewRuntime(handles.StoreValue(store), cs, opts)
	if err != nil {
		return nil, err
	}
	return &Runtime{value: r}, nil
}

// Run processes messages until cancellation or component failure and waits
// for components to exit after cancellation.
func (r *Runtime) Run(ctx context.Context) error {
	if r == nil || r.value == nil || ctx == nil {
		return ErrRuntimeConfig
	}
	return r.value.Run(ctx)
}
func project(from, to any) error {
	raw, err := json.Marshal(from)
	if err != nil {
		return errors.New("invalid channel value")
	}
	if json.Unmarshal(raw, to) != nil {
		return errors.New("invalid channel value")
	}
	return nil
}
