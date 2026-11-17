// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestMetadataUpgradeKeepsPreviouslyAcceptedMessageDeduplication(t *testing.T) {
	h := newHarnessWith(t, &conformance.MAServer{}, &runtimeProvider{}, runtimeBinding(), testRuntimeOptions())
	m := runtimeMessage()
	first := h.enqueue(m)
	m.Metadata = &core.MessageMetadata{MessageType: "text", RootMessageID: "om-root"}
	duplicate := h.enqueue(m)
	require.True(t, duplicate.Duplicate)
	require.Equal(t, first.Message.ContentHash, duplicate.Message.ContentHash)
	require.Equal(t, first.Turn.ID, duplicate.Turn.ID)
	m.Text = "different body"
	_, err := h.runtime.Enqueue(t.Context(), "binding", m)
	require.ErrorIs(t, err, core.ErrConflict)
}
