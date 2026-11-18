// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package channel

import core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"

// Observer receives redacted observations and must return promptly and be concurrency-safe.
type Observer = core.Observer

// Observation excludes message contents and secrets.
// Arbitrary IDs must not be used as metric labels.
type Observation = core.Observation

// RouteKey is an irreversible conversation-routing identifier.
type RouteKey = core.RouteKey
