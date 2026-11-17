// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/file"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/session"
)

func TestMAServerFixturesSatisfyPublicSDKResources(t *testing.T) {
	server := httptest.NewServer(&MAServer{})
	defer server.Close()
	sdk := arkruntime.NewClientWithApiKey("fixture", arkruntime.WithBaseUrl(server.URL+"/api/v3"))
	_, err := sdk.GetEnvironment(context.Background(), "env")
	require.NoError(t, err)
	uploaded, err := sdk.UploadFileStream(context.Background(), &file.FileCreateRequest{Purpose: file.PurposeUserData}, io.NopCloser(strings.NewReader("payload")), arkruntime.FileUploadOptions{})
	require.NoError(t, err)
	_, err = sdk.RetrieveFile(context.Background(), uploaded.ID)
	require.NoError(t, err)
	_, err = sdk.CreateSessionResource(context.Background(), "sesn-demo", &session.CreateSessionResourceRequest{Type: session.CreateSessionResourceRequestTypeFile, FileID: uploaded.ID}, arkruntime.WithExtraBody(map[string]interface{}{"mount_path": "fixture.txt"}))
	require.NoError(t, err)
	_, err = sdk.ListSessionResources(context.Background(), "sesn-demo")
	require.NoError(t, err)
}
