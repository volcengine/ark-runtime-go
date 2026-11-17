// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma_test

import (
	"context"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func TestUploadFilenameKeepsByteLimitAndUTF8Boundaries(t *testing.T) {
	for _, tc := range []struct{ name, filename, want string }{
		{"chinese", strings.Repeat("中", 70), strings.Repeat("中", 66)},
		{"emoji boundary", strings.Repeat("a", 199) + "😀.txt", strings.Repeat("a", 199)},
		{"exact emoji limit", strings.Repeat("😀", 50), strings.Repeat("😀", 50)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reader, err := r.MultipartReader()
				require.NoError(t, err)
				for {
					part, err := reader.NextPart()
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					if part.FormName() == "file" {
						_, params, err := mime.ParseMediaType(part.Header.Get("Content-Disposition"))
						require.NoError(t, err, "multipart filename must not contain escaped partial UTF8 bytes")
						require.True(t, utf8.ValidString(params["filename"]))
						require.LessOrEqual(t, len(params["filename"]), 200)
						require.Equal(t, tc.want, params["filename"])
					}
					_, _ = io.Copy(io.Discard, part)
					_ = part.Close()
				}
				_, _ = io.WriteString(w, `{"id":"file-1","status":"active","object":"file","purpose":"user_data","filename":"fixture","created_at":0,"expire_at":-1}`)
			}))
			defer server.Close()
			client, err := ma.NewClient(server.URL, "key", ma.ClientOptions{})
			require.NoError(t, err)
			_, err = client.UploadFile(context.Background(), core.FileUpload{FileName: tc.filename, Reader: strings.NewReader("payload")})
			require.NoError(t, err)
		})
	}
}
