// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package channel

import (
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// AttachmentLimits bounds attachment inputs for one message.
type AttachmentLimits = core.AttachmentLimits

// ContextLimits bounds recent conversation context.
type ContextLimits = core.ContextLimits

// ArtifactLimits bounds artifact delivery.
type ArtifactLimits = core.ArtifactLimits

// Options configures runtime budgets, Hooks, and Observer.
type Options struct {
	// IdentityRenewalInterval controls frozen bot identity checks; it defaults to one minute.
	IdentityRenewalInterval time.Duration
	// IdentityRenewalConcurrency bounds background identity preparation; default 4, maximum 32.
	IdentityRenewalConcurrency int
	// CreateConcurrency, SubmitConcurrency, and ObserveConcurrency bound the three process-local pools.
	// Any nonzero value enables separate pools and cannot be combined with legacy
	// Workers, AccountLimit, or CreationLimit settings.
	CreateConcurrency  int
	SubmitConcurrency  int
	ObserveConcurrency int
	// AccountLimit applies only to the legacy shared pool; MA owns quotas in separate-pool mode.
	AccountLimit int
	// CreationLimit bounds unknown creations and defaults to AccountLimit.
	CreationLimit       int
	Workers             int
	PollInterval        time.Duration
	TurnTimeout         time.Duration
	LeaseTTL            time.Duration
	DeliveryTimeout     time.Duration
	DeliveryMaxAttempts uint64
	IdleTimeout         time.Duration
	// MessageDeadline is the legacy alias for the no-progress window; use IdleTimeout.
	MessageDeadline   time.Duration
	UnknownWriteGrace time.Duration
	Clock             func() time.Time
	// Observer receives structured observations; nil discards them.
	Observer Observer
	// Retention bounds settled-record lifetime. Cleanup also removes deduplication
	// indexes, so it must exceed the channel redelivery window. Default: 7 days;
	// minimum: 24 hours.
	Retention time.Duration
	// PruneInterval controls background cleanup and defaults to one hour.
	PruneInterval time.Duration
	// ProgressInterval sets the minimum interval between progress updates for a turn;
	// it defaults to two seconds to respect per-message rate limits.
	ProgressInterval time.Duration
	// UserAuthPollInterval sets the initial authorization recheck interval; subsequent
	// checks use jittered exponential backoff capped at 15 seconds.
	// It defaults to two seconds and cannot exceed 15 seconds.
	UserAuthPollInterval time.Duration
	// Attachments bounds attachment count and sizes per message.
	Attachments AttachmentLimits
	// Context bounds recent history injected into the first turn of a new Session.
	Context ContextLimits
	// Artifacts bounds artifact count and sizes per turn.
	Artifacts ArtifactLimits
	// Hooks provides controlled extensions; HookTimeout defaults to three seconds
	// and HookConcurrency defaults to 16. Hooks must honor their context: timeout
	// rejects late results but cannot interrupt a callback that does not return.
	Hooks           Hooks
	HookTimeout     time.Duration
	HookConcurrency int
}
