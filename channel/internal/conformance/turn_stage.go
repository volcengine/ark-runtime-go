// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func turnStageClaim(t *testing.T, open opener) {
	for _, phase := range []core.TurnPhase{core.PhaseNone, core.PhaseCreate, core.PhaseSend, core.PhaseObserve} {
		name := string(phase)
		if phase == core.PhaseNone {
			name = "unstarted"
		}
		t.Run(name, func(t *testing.T) {
			s, _ := open(t, StoreConfig{})
			created := ingest(t, s, SampleMessage("stage"))
			original := lease(t, s, "setup")
			if phase != core.PhaseNone {
				_, err := s.UpdateTurn(t.Context(), original.Turn.Lease, core.TurnUpdate{Phase: phase, Session: core.TurnSession{Generation: 1, CreationKey: "key", SessionID: "sesn-stage"}})
				require.NoError(t, err)
			}
			require.NoError(t, s.ReleaseTurn(t.Context(), original.Turn.Lease))
			for _, stage := range []core.TurnStage{core.StageCreate, core.StageSubmit, core.StageObserve, core.StageAny} {
				claim := Claim("stage-" + string(stage))
				claim.Stage = stage
				got, err := s.LeaseTurn(t.Context(), claim)
				require.NoError(t, err)
				expected := stage == core.StageAny || stage == core.StageSubmit && (phase == core.PhaseNone || phase == core.PhaseSend) || stage == core.StageCreate && (phase == core.PhaseNone || phase == core.PhaseCreate) || stage == core.StageObserve && phase == core.PhaseObserve
				if !expected {
					require.Nil(t, got)
					continue
				}
				require.NotNil(t, got)
				require.Equal(t, created.Turn.ID, got.Turn.ID)
				if phase == core.PhaseNone && stage == core.StageCreate {
					other := Claim("competing-submit")
					other.Stage = core.StageSubmit
					concurrent, err := s.LeaseTurn(t.Context(), other)
					require.NoError(t, err)
					require.Nil(t, concurrent, "两个池不能同时领取同一准备阶段")
				}
				require.Greater(t, got.Turn.Lease.Fence, original.Turn.Lease.Fence)
				require.ErrorIs(t, s.ReleaseTurn(t.Context(), original.Turn.Lease), core.ErrLeaseLost)
				require.NoError(t, s.ReleaseTurn(t.Context(), got.Turn.Lease))
			}
			invalid := Claim("invalid")
			invalid.Stage = "future"
			_, err := s.LeaseTurn(t.Context(), invalid)
			require.ErrorIs(t, err, core.ErrInvalid)
		})
	}
}

func turnStageExpiredTail(t *testing.T, open opener) {
	s, clock := open(t, StoreConfig{QueueTTL: time.Minute})
	ingest(t, s, P2PMessage("head"))
	head := lease(t, s, "head")
	_, err := s.UpdateTurn(t.Context(), head.Turn.Lease, core.TurnUpdate{Phase: core.PhaseObserve, Session: core.TurnSession{Generation: 1, SessionID: "sesn-head"}})
	require.NoError(t, err)
	tail := ingest(t, s, P2PMessage("tail"))
	clock.Advance(time.Minute)
	observe := Claim("observe")
	observe.Stage = core.StageObserve
	got, err := s.LeaseTurn(t.Context(), observe)
	require.NoError(t, err)
	if got != nil {
		require.NotEqual(t, tail.Turn.ID, got.Turn.ID)
		require.NoError(t, s.ReleaseTurn(t.Context(), got.Turn.Lease))
	}
	for _, stage := range []core.TurnStage{core.StageCreate, core.StageSubmit} {
		claim := Claim(string(stage))
		claim.Stage = stage
		got, err := s.LeaseTurn(t.Context(), claim)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, tail.Turn.ID, got.Turn.ID)
		require.NoError(t, s.ReleaseTurn(t.Context(), got.Turn.Lease))
	}
}
