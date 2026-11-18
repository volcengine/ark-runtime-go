// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package channel

import core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"

// Hooks provides business extensions with deadlines and concurrency limits.
// Hooks must honor their context; late results are rejected, but a callback that
// never returns cannot be forcibly interrupted by Runtime.
type Hooks = core.Hooks

// HookMessage is a message view without credentials or attachment contents.
type HookMessage = core.HookMessage

// TriggerDecision determines whether to invoke the Agent.
type TriggerDecision = core.TriggerDecision

// SessionExtras appends permitted resources and tags to a new Session.
type SessionExtras = core.SessionExtras

// InputEnrichment adds input context that is explicitly marked as non-instructional.
type InputEnrichment = core.InputEnrichment

// DeliveryReport is a redacted delivery result.
type DeliveryReport = core.DeliveryReport

// DeliveryStatus identifies a delivery outcome.
type DeliveryStatus = core.DeliveryStatus
