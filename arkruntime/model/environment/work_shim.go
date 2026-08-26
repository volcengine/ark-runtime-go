// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package environment

import (
	"bytes"

	"github.com/volcengine/ark-runtime-go/arkruntime/model"
)

const (
	// DefaultWorkerClientType is kept for source compatibility.
	//
	// Deprecated: the MA work API no longer accepts worker client headers.
	DefaultWorkerClientType = "ark-self-hosted-worker-go"
	// DefaultWorkerClientVersion is kept for source compatibility.
	//
	// Deprecated: the MA work API no longer accepts worker client headers.
	DefaultWorkerClientVersion = "0.1.0"
)

// PollWorkRequest is the ergonomic SDK request for polling one work item.
type PollWorkRequest struct {
	EnvironmentID string `json:"environment_id"`
	WorkerID      string `json:"worker_id,omitempty"`
	// Deprecated: MA poll returns at most one work item.
	MaxItems           int `json:"max_items,omitempty"`
	BlockMS            int `json:"block_ms,omitempty"`
	ReclaimOlderThanMS int `json:"reclaim_older_than_ms,omitempty"`
	// Deprecated: MA work API no longer accepts worker client headers.
	WorkerClientType string `json:"-"`
	// Deprecated: MA work API no longer accepts worker client headers.
	WorkerClientVersion string `json:"-"`
}

// AckWorkRequest is the ergonomic SDK request for acknowledging one work item.
type AckWorkRequest struct {
	EnvironmentID string    `json:"environment_id"`
	WorkID        string    `json:"work_id"`
	WorkerID      OptString `json:"worker_id,omitempty"`
}

// HeartbeatWorkRequest is the ergonomic SDK request for refreshing one work lease.
type HeartbeatWorkRequest struct {
	EnvironmentID         string    `json:"environment_id"`
	WorkID                string    `json:"work_id"`
	ExpectedLastHeartbeat OptString `json:"expected_last_heartbeat,omitempty"`
	DesiredTTLSeconds     OptInt64  `json:"desired_ttl_seconds,omitempty"`
}

// StopWorkRequest is the ergonomic SDK request for stopping one work item.
type StopWorkRequest struct {
	EnvironmentID string  `json:"environment_id"`
	WorkID        string  `json:"work_id"`
	Force         OptBool `json:"force,omitempty"`
}

// SessionIDValue returns the session id carried by the work item.
func (w WorkItem) SessionIDValue() string {
	if w.Data.ID != "" && (w.Data.Type == "" || w.Data.Type == "session") {
		return w.Data.ID
	}
	return ""
}

// LeaseTTLSeconds returns the lease TTL carried by legacy MA work payloads.
//
// Deprecated: MA work items no longer carry TTL fields. Use HeartbeatWorkResponse.TTLSeconds.
func (w WorkItem) LeaseTTLSeconds() int {
	return 0
}

// LatestHeartbeatValue returns the heartbeat CAS value from MA work payloads.
func (w WorkItem) LatestHeartbeatValue() string {
	if latestHeartbeatAt, ok := w.LatestHeartbeatAt.Get(); ok && latestHeartbeatAt != "" {
		return latestHeartbeatAt
	}
	return ""
}

// WorkItemResponse wraps WorkItem so it satisfies model.Response.
type WorkItemResponse struct {
	WorkItem
	model.HttpHeader
}

// UnmarshalJSON accepts MA's empty-poll response (`200 {}`) while preserving
// generated validation for real work items.
func (r *WorkItemResponse) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if bytes.Equal(trimmed, []byte("{}")) || bytes.Equal(trimmed, []byte("null")) {
		r.WorkItem = WorkItem{}
		return nil
	}
	var item WorkItem
	if err := item.UnmarshalJSON(data); err != nil {
		return err
	}
	r.WorkItem = item
	return nil
}

// HeartbeatWorkResponseWrapper wraps HeartbeatWorkResponse.
type HeartbeatWorkResponseWrapper struct {
	HeartbeatWorkResponse
	model.HttpHeader
}
