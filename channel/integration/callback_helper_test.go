// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type runtimeCallbackProvider struct {
	runtimeProvider
	store    core.Store
	messages []core.Message
	acks     chan runtimeCallbackACK
}

type runtimeCallbackACK struct {
	err     error
	turnIDs []string
}

// Start 通过 Runtime 的真实入站回调投递消息，并在回调返回（即持久化完成）后检查 Store。
func (p *runtimeCallbackProvider) Start(ctx context.Context, accept func(context.Context, core.Message) error) error {
	for _, message := range p.messages {
		err := accept(ctx, message)
		ack := runtimeCallbackACK{err: err}
		turns, listErr := p.store.ListTurns(ctx, core.TurnQuery{})
		if listErr != nil && err == nil {
			ack.err = listErr
		}
		for _, turn := range turns {
			ack.turnIDs = append(ack.turnIDs, turn.ID)
		}
		p.acks <- ack
	}
	<-ctx.Done()
	return nil
}
