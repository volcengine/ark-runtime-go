// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type recordedOwner struct{ role, owner string }
type ownerRecordingStore struct {
	core.Store
	owners chan recordedOwner
}

func (s *ownerRecordingStore) record(role, owner string) {
	select {
	case s.owners <- recordedOwner{role, owner}:
	default:
	}
}
func (s *ownerRecordingStore) LeaseTurn(ctx context.Context, c core.ClaimOptions) (*core.TurnLease, error) {
	s.record("turn", c.Owner)
	return s.Store.LeaseTurn(ctx, c)
}
func (s *ownerRecordingStore) LeaseDelivery(ctx context.Context, owner string, ttl time.Duration) (*core.Delivery, error) {
	s.record("delivery", owner)
	return s.Store.LeaseDelivery(ctx, owner, ttl)
}
func (s *ownerRecordingStore) LeaseRemoteOperation(ctx context.Context, c core.RemoteClaim) (*core.RemoteOperation, error) {
	s.record("remote", c.Owner)
	return s.Store.LeaseRemoteOperation(ctx, c)
}

func TestRuntimeOwnersUseDistinctInstanceIdentity(t *testing.T) {
	backing, err := newEmbeddedStore(t)
	require.NoError(t, err)
	var instanceIDs []string
	for i := 0; i < 2; i++ {
		store := &ownerRecordingStore{Store: backing, owners: make(chan recordedOwner, 16)}
		options := testRuntimeOptions()
		options.Workers = 1
		runtime := mustRuntime(t, store, runtimeBinding(), newMAForHarness(t, &conformance.MAServer{}), &runtimeProvider{}, options)
		runUntilCleanup(t, runtime)
		observed := map[string]string{}
		deadline := time.After(time.Second)
		for len(observed) < 3 {
			select {
			case item := <-store.owners:
				if previous := observed[item.role]; previous != "" {
					require.Equal(t, previous, item.owner)
				}
				observed[item.role] = item.owner
			case <-deadline:
				t.Fatal("all worker roles must expose their lease owner")
			}
		}
		turnID := strings.TrimSuffix(strings.TrimPrefix(observed["turn"], "turn-worker-"), "-0")
		deliveryID := strings.TrimPrefix(observed["delivery"], "delivery-worker-")
		remoteID := strings.TrimPrefix(observed["remote"], "remote-")
		require.Regexp(t, `^[0-9a-f]{32}$`, turnID, "owner identity must not depend on process virtual addresses")
		require.Equal(t, turnID, deliveryID)
		require.Equal(t, turnID, remoteID)
		instanceIDs = append(instanceIDs, turnID)
	}
	require.NotEqual(t, instanceIDs[0], instanceIDs[1], "two runtimes sharing a store need distinct instance identities")
}
