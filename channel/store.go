// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package channel

import "github.com/volcengine/ark-runtime-go/channel/internal/handles"

// Store is a built-in persistence handle constructed by embedded or sql;
// the caller is responsible for Close.
type Store = handles.Store

// AgentClient is an MA Adapter handle constructed by package ma.
type AgentClient = handles.Agent

// ProviderHandle identifies a built-in channel or a channel wrapped by NewProvider.
type ProviderHandle = handles.Provider
