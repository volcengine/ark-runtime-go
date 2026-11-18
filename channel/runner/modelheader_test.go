// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/secret"
)

func TestLoadModelHeaderRefs(t *testing.T) {
	data := strings.Replace(validJSON(), `"api_key_ref":"env:MA_KEY"`, `"api_key_ref":"env:MA_KEY","model_header_refs":{"Authorization":"env:MODEL_AUTH"}`, 1)
	cfg, err := Load(writeConfig(t, data))
	require.NoError(t, err)
	require.Equal(t, map[string]string{"Authorization": "env:MODEL_AUTH"}, cfg.Connections[0].MA.ModelHeaderRefs)
	for _, refs := range []string{`{"Authorization":"plaintext-secret"}`, `{"Host":"env:MODEL_AUTH"}`, `{"Authorization":"env:A","authorization":"env:B"}`, `{"X-User-Model-Header-X-Api-Key":"env:A"}`} {
		_, err := Load(writeConfig(t, strings.Replace(data, `{"Authorization":"env:MODEL_AUTH"}`, refs, 1)))
		require.Error(t, err)
		require.NotContains(t, err.Error(), "plaintext-secret")
	}
}

func TestRunnerModelHeadersResolveFilePerSubmission(t *testing.T) {
	path := filepath.Join(t.TempDir(), "model-key")
	require.NoError(t, os.WriteFile(path, []byte("Bearer first"), 0600))
	received := make(chan http.Header, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r.Header.Clone()
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer server.Close()
	cfg := MAConfig{BaseURL: server.URL, ModelHeaderRefs: map[string]string{"Authorization": "file:" + path}}
	client, err := newRunnerMA(cfg, "platform", secret.EnvFileResolver{})
	require.NoError(t, err)
	for _, value := range []string{"Bearer first", "Bearer second"} {
		require.NoError(t, os.WriteFile(path, []byte(value), 0600))
		_, err = client.SendInterrupt(context.Background(), "s1", "e1")
		require.NoError(t, err)
		h := <-received
		require.Equal(t, value, h.Get("X-User-Model-Header-Authorization"))
		require.Equal(t, "Bearer platform", h.Get("Authorization"))
	}
	require.NoError(t, os.Remove(path))
	_, err = client.SendInterrupt(t.Context(), "s1", "e2")
	require.Error(t, err)
	require.NotContains(t, err.Error(), path)
	var unsent interface{ RequestNotSent() bool }
	require.ErrorAs(t, err, &unsent)
	require.Empty(t, received)
}
