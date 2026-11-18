// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func TestClientConformance(t *testing.T) {
	conformance.RunMAClient(t, func(t *testing.T, baseURL string) core.MAClient {
		client, err := ma.NewClient(baseURL, "fixture-key", ma.ClientOptions{RequestTimeout: 5 * time.Second, StreamTimeout: time.Second})
		require.NoError(t, err)
		return client
	})
}
