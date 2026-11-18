// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunnerFinalStreamDoesNotRequireCOT(t *testing.T) {
	raw := strings.Replace(validJSON(), `"response_mode":"non_stream"`, `"response_mode":"final_stream"`, 1)
	cfg, err := Load(writeConfig(t, raw))
	require.NoError(t, err)
	require.Equal(t, "final_stream", cfg.Connections[0].ResponseMode)
	require.Empty(t, cfg.Connections[0].ProgressProtocol)
}
