// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package environments

import (
	"context"
	"net/http"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/environment"
	selfhosted "github.com/volcengine/ark-runtime-go/arkruntime/selfhosted"
)

const (
	heartbeatDefault = 30 * time.Second
	heartbeatFloor   = time.Second
)

type heartbeatStopCause string

const (
	heartbeatStopCauseLeaseLost        heartbeatStopCause = "lease_lost"
	heartbeatStopCauseLeaseNotExtended heartbeatStopCause = "lease_not_extended"
	heartbeatStopCauseStopRequested    heartbeatStopCause = "stop_requested"
	heartbeatStopCauseHeartbeatLost    heartbeatStopCause = "heartbeat_lost"
	heartbeatStopCausePermanentFailure heartbeatStopCause = "heartbeat_permanent_failure"
)

func (w *EnvironmentWorker) heartbeatLoop(ctx context.Context, work claimedWork, api selfhosted.API, cancel context.CancelFunc, markStopped func(heartbeatStopCause)) {
	interval := clampHeartbeatInterval(heartbeatDefault/2, heartbeatDefault)
	ttl := heartbeatDefault
	logger := w.logger().With("component", "environment-worker", "work_id", work.ID, "session_id", work.SessionID)
	last := work.LatestHeartbeatAt
	if last == "" {
		last = selfhosted.ExpectedLastHeartbeatNoHeartbeat
	}
	lastSuccess := time.Now()
	beat := func() bool {
		beatCtx, beatCancel := context.WithTimeout(ctx, interval)
		defer beatCancel()
		resp, err := api.HeartbeatWork(beatCtx, selfhosted.HeartbeatWorkRequest{
			EnvironmentID:         work.EnvironmentID,
			WorkID:                work.ID,
			ExpectedLastHeartbeat: environment.NewOptString(last),
			DesiredTTLSeconds:     environment.NewOptInt64(int64(ttl / time.Second)),
		})
		if err != nil {
			if selfhosted.IsStatus(err, http.StatusPreconditionFailed) {
				logger.Warn("heartbeat lease lost", "err", err)
				if markStopped != nil {
					markStopped(heartbeatStopCauseLeaseLost)
				}
				cancel()
				return false
			}
			if ctx.Err() != nil {
				return false
			}
			if isFatal4xx(err) {
				logger.Error("heartbeat permanent failure", "err", err)
				if markStopped != nil {
					markStopped(heartbeatStopCausePermanentFailure)
				}
				cancel()
				return false
			}
			if stale := time.Since(lastSuccess); stale > ttl {
				logger.Error("heartbeat staleness exceeded", "since_last_success", stale, "ttl", ttl, "err", err)
				if markStopped != nil {
					markStopped(heartbeatStopCauseHeartbeatLost)
				}
				cancel()
				return false
			}
			logger.Warn("heartbeat failed", "since_last_success", time.Since(lastSuccess), "ttl", ttl, "err", err)
			return true
		}
		if resp == nil {
			logger.Warn("heartbeat empty response")
			return true
		}
		lastSuccess = time.Now()
		if resp.LastHeartbeat != "" {
			last = resp.LastHeartbeat
		}
		if resp.TTLSeconds > 0 {
			ttl = time.Duration(resp.TTLSeconds) * time.Second
			interval = clampHeartbeatInterval(ttl/2, heartbeatDefault)
		}
		state := resp.State
		if state == selfhosted.WorkStateStopping || state == selfhosted.WorkStateStopped {
			logger.Info("heartbeat stop requested", "state", state)
			if markStopped != nil {
				markStopped(heartbeatStopCauseStopRequested)
			}
			cancel()
			return false
		}
		if !resp.LeaseExtended {
			logger.Warn("heartbeat lease not extended", "state", state)
			if markStopped != nil {
				markStopped(heartbeatStopCauseLeaseNotExtended)
			}
			cancel()
			return false
		}
		return true
	}
	if !beat() {
		return
	}
	for {
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if !beat() {
			return
		}
	}
}

func clampHeartbeatInterval(interval, maxInterval time.Duration) time.Duration {
	if interval < heartbeatFloor {
		return heartbeatFloor
	}
	if maxInterval > 0 && interval > maxInterval {
		return maxInterval
	}
	return interval
}
