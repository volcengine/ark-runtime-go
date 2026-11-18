// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type streamBoundaryStore struct {
	Store
	state      TextStreamState
	result     DeliveryResult
	deliveries []Delivery
}

func (s *streamBoundaryStore) UpdateTextStream(_ context.Context, _ Lease, next TextStreamState) error {
	if err := s.state.ValidateUpdate(next); err != nil {
		return err
	}
	s.state = next
	return nil
}
func (s *streamBoundaryStore) CompleteDelivery(_ context.Context, _ Lease, result DeliveryResult) error {
	s.result = result
	return nil
}
func (s *streamBoundaryStore) SettleTurn(_ context.Context, _ Lease, _ TurnOutcome, deliveries []Delivery) error {
	s.deliveries = deliveries
	return nil
}

type streamUnavailableResolver struct{ ConnectionResolver }

func (streamUnavailableResolver) ResolveForDelivery(context.Context, Delivery) (Connection, error) {
	return Connection{}, errors.New("temporary resolver failure")
}

func TestTextStreamResolverUsesFailureBudgetAfterPrintingWaits(t *testing.T) {
	now := time.Now()
	state := TextStreamState{Handle: "card", MessageID: "original", FirstSendAt: now, Position: 5, PendingPosition: 5, LastContentAt: now, Sequence: 7}
	s := &streamBoundaryStore{state: state}
	r := &Runtime{store: s, options: RuntimeOptions{Clock: time.Now, DeliveryMaxAttempts: 2, PollInterval: time.Millisecond, Resolver: streamUnavailableResolver{}}}
	d := NewDelivery(DeliverySource{Kind: SourceTurn, ID: "turn"}, 0, "answer", "binding", ReplyTarget{ChatID: "chat"}, StreamTextBody("final"))
	d.FirstAttemptAt, d.Attempt, d.TextStream = now, 10, &state
	require.NoError(t, r.processDelivery(t.Context(), d))
	require.Equal(t, DeliveryPending, s.result.Status, "printing waits must not exhaust resolver failure budget")
	require.EqualValues(t, 1, s.state.Failures)
	require.Equal(t, "original", s.state.MessageID)
	require.EqualValues(t, 7, s.state.Sequence)
	d.TextStream = &s.state
	require.NoError(t, r.processDelivery(t.Context(), d))
	require.Equal(t, DeliveryFailed, s.result.Status, "real resolver failures must still exhaust the budget")
	require.EqualValues(t, 2, s.state.Failures)
}

func TestTextStreamEmptyAnswerKeepsNoticesOrdinary(t *testing.T) {
	for _, notice := range []string{"renewed", "mount", "artifact"} {
		t.Run(notice, func(t *testing.T) {
			j := displayJob()
			j.binding.StreamFinalAnswer = true
			outcome := judgeTurn([]Event{{ID: "input", Type: "user.message"}, displayMessage("draft", "我查一下"), {ID: "tool", Type: "agent.tool_use"}, {ID: "result", Type: "agent.tool_result", ToolUseID: "tool"}, {ID: "end", Type: "session.status_idle", StopReason: "end_turn"}}, j)
			require.Equal(t, "done", outcome.Status)
			require.Empty(t, outcome.Text)
			var artifacts []Artifact
			switch notice {
			case "renewed":
				j.Session.Renewed = true
			case "mount":
				j.Input.Mounts = []MountedFile{{FileID: "file", Skipped: true, SkipReason: "environment_unknown"}}
			case "artifact":
				j.binding.DeliverArtifacts = true
				artifacts = []Artifact{{FileID: "one", Kind: "document"}, {FileID: "two", Kind: "document"}}
			}
			s := &streamBoundaryStore{}
			r := &Runtime{store: s, options: RuntimeOptions{Clock: time.Now, Artifacts: ArtifactLimits{MaxCount: 1}}}
			require.NoError(t, r.settleAnswer(j, outcome.Text, artifacts))
			require.NotEmpty(t, s.deliveries)
			require.Equal(t, BodyText, s.deliveries[0].Body.Type)
			require.NotEmpty(t, s.deliveries[0].Body.Text)
			require.NotContains(t, s.deliveries[0].Body.Text, "我查一下")
		})
	}
}
