// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package channel

import (
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// IdentityPolicy selects execution identity for direct and group conversations.
type IdentityPolicy = core.IdentityPolicy

// BotCredentialSpec selects the Vault credential and allowed hosts for bot tokens.
type BotCredentialSpec = core.BotCredentialSpec

// Binding connects a channel to an Agent without secrets or internal routing data.
type Binding struct {
	ID                 string            `json:"id"`
	AppID              string            `json:"app_id"`
	AgentID            string            `json:"agent_id"`
	AgentVersion       string            `json:"agent_version"`
	EnvironmentID      string            `json:"environment_id,omitempty"`
	VaultIDs           []string          `json:"vault_ids,omitempty"`
	MemoryIDs          []string          `json:"memory_ids,omitempty"`
	GroupTrigger       string            `json:"group_trigger,omitempty"`
	GroupReplyMode     string            `json:"group_reply_mode,omitempty"`
	MaxTurnDuration    time.Duration     `json:"max_turn_duration,omitempty"`
	ShowProgress       bool              `json:"show_progress,omitempty"`
	ProgressMode       string            `json:"progress_mode,omitempty"`
	ProgressProtocol   string            `json:"progress_protocol,omitempty"`
	StreamProgress     bool              `json:"stream_progress,omitempty"`
	StreamFinalAnswer  bool              `json:"stream_final_answer,omitempty"`
	Approvals          bool              `json:"approvals,omitempty"`
	RecentContext      bool              `json:"recent_context,omitempty"`
	HookFailOpen       bool              `json:"hook_fail_open,omitempty"`
	Identity           IdentityPolicy    `json:"identity,omitempty"`
	BotCredential      BotCredentialSpec `json:"bot_credential,omitempty"`
	DeliverArtifacts   bool              `json:"deliver_artifacts,omitempty"`
	OutputArtifactsAll bool              `json:"output_artifacts_all,omitempty"`
	FileMounts         bool              `json:"file_mounts,omitempty"`
	UserAuthScopes     []string          `json:"user_auth_scopes,omitempty"`
}
