// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInterruptProofRequiresMatchingInputControlAndTerminal(t *testing.T) {
	j := displayJob()
	input := Event{ID: j.InputEventID, Type: "user.message"}
	interrupt := Event{ID: interruptEventID(j), Type: "user.interrupt"}
	idle := Event{ID: "idle", Type: "session.status_idle", StopReason: "end_turn"}
	childIdle := idle
	childIdle.SessionThreadID = "sthr-child"
	wrongInterrupt := interrupt
	wrongInterrupt.ID = "unrelated-stop"
	childInterrupt := interrupt
	childInterrupt.SessionThreadID = "sthr-child"
	for _, tc := range []struct {
		name   string
		events []Event
		proved bool
	}{
		{"stop echo then idle", []Event{input, interrupt, idle}, true},
		{"natural terminal before stop echo", []Event{input, idle, interrupt}, true},
		{"old idle only", []Event{idle, input, interrupt}, false},
		{"http echo absent from history", []Event{input, idle}, false},
		{"input absent", []Event{interrupt, idle}, false},
		{"interrupt before input", []Event{interrupt, input, idle}, false},
		{"wrong interrupt", []Event{input, wrongInterrupt, idle}, false},
		{"child interrupt", []Event{input, childInterrupt, idle}, false},
		{"child terminal", []Event{input, interrupt, childIdle}, false},
		{"requires action", []Event{input, interrupt, {ID: "pause", Type: "session.status_idle", StopReason: "requires_action"}}, false},
		{"running after terminal", []Event{input, interrupt, idle, {ID: "running", Type: "session.status_running"}}, false},
		{"next input", []Event{input, interrupt, idle, {ID: "next", Type: "user.message"}}, false},
		{"conflicting input", []Event{{ID: input.ID, Type: "user.interrupt"}, interrupt, idle}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proof := interruptProof(tc.events, j)
			if !tc.proved {
				require.Nil(t, proof)
				return
			}
			require.NotNil(t, proof)
			require.Equal(t, RemoteProofKind("interrupted"), proof.Kind)
			require.Equal(t, interrupt.ID, proof.Reference)
		})
	}
}

func TestCleanupProofMustMatchFrozenInterrupt(t *testing.T) {
	op := RemoteOperation{TurnID: "turn-one", Generation: 2, Kind: RemoteInput, CleanupStarted: true, Counted: true}
	j := &job{Turn: Turn{ID: op.TurnID, Session: TurnSession{Generation: op.Generation}}}
	_, err := op.Resolve(RemoteProof{Kind: RemoteProofKind("interrupted"), Reference: "unrelated"}, time.Now())
	require.Error(t, err)
	resolved, err := op.Resolve(RemoteProof{Kind: RemoteProofKind("interrupted"), Reference: interruptEventID(j)}, time.Now())
	require.NoError(t, err)
	require.True(t, resolved.Resolved)
	require.False(t, resolved.Counted)
	op.CleanupStarted = false
	_, err = op.Apply(RemoteUpdate{CleanupStarted: true, Proof: &RemoteProof{Kind: RemoteInterrupted, Reference: interruptEventID(j)}}, time.Now())
	require.ErrorIs(t, err, ErrRemoteBarrier, "new interrupt intent cannot be settled in the same update")
}
