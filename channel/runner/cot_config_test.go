// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunnerNativeCOTConfig(t *testing.T) {
	raw := strings.Replace(validJSON(), `"response_mode":"non_stream"`, `"response_mode":"process_stream","progress_protocol":"native_cot"`, 1)
	config, err := Load(writeConfig(t, raw))
	require.NoError(t, err)
	require.Equal(t, "native_cot", config.Connections[0].ProgressProtocol)
	require.Equal(t, "process_stream", config.Connections[0].ResponseMode)
	for _, invalid := range []string{
		strings.Replace(raw, "native_cot", "unknown", 1),
		strings.Replace(raw, "native_cot", "", 1),
	} {
		_, err := Load(writeConfig(t, invalid))
		require.Error(t, err)
	}
}
