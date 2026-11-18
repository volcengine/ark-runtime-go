// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func durationTestBox(kind string, payload []byte) []byte {
	box := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(box, uint32(len(box)))
	copy(box[4:], kind)
	copy(box[8:], payload)
	return box
}

func durationTestVideo(scale, ticks uint32) []byte {
	header := make([]byte, 100)
	binary.BigEndian.PutUint32(header[12:], scale)
	binary.BigEndian.PutUint32(header[16:], ticks)
	data := durationTestBox("ftyp", []byte("mp42\x00\x00\x00\x00mp42"))
	data = append(data, durationTestBox("mdat", []byte("original-video-bytes"))...)
	return append(data, durationTestBox("moov", durationTestBox("mvhd", header))...)
}

func TestVideoUploadsIncludeDurationAndPreserveBytes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rich  bool
		scale uint32
		want  string
	}{
		{"rich duration", true, 48000, "10250"},
		{"standalone duration", false, 48000, "10250"},
		{"rich unknown still uploads", true, 0, ""},
		{"standalone unknown still uploads", false, 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := durationTestVideo(tc.scale, 492000)
			uploads, sends := 0, 0
			p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal"):
					tokenResponse(w)
				case r.URL.Path == "/open-apis/im/v1/files":
					uploads++
					require.NoError(t, r.ParseMultipartForm(1<<20))
					require.Equal(t, "mp4", r.FormValue("file_type"))
					require.Equal(t, tc.want, r.FormValue("duration"))
					file, _, err := r.FormFile("file")
					require.NoError(t, err)
					got, err := io.ReadAll(file)
					require.NoError(t, err)
					require.NoError(t, file.Close())
					require.Equal(t, data, got, "探测后上传必须仍为完整原始视频")
					_, _ = io.WriteString(w, `{"code":0,"data":{"file_key":"file_video"}}`)
				case strings.HasSuffix(r.URL.Path, "/reply"):
					sends++
					_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om_video"}}`)
				default:
					t.Errorf("unexpected %s", r.URL.Path)
				}
			})
			a := core.Artifact{Kind: "video", FileID: "file-source", FileName: "clip.mp4"}
			content := core.FileContent{Body: io.NopCloser(bytes.NewReader(data)), FileName: "clip.mp4", Size: int64(len(data))}
			if tc.rich {
				part, err := p.UploadInlineMedia(t.Context(), a, content)
				require.NoError(t, err)
				require.Equal(t, "file_video", part.ResourceKey)
				require.Zero(t, sends)
			} else {
				d := core.Delivery{UUID: "video-duration", ReplyTarget: core.ReplyTarget{Mode: "reply", MessageID: "om-source"}, Body: core.ArtifactBody(a)}
				_, err := p.DeliverArtifact(t.Context(), d, content)
				require.NoError(t, err)
				require.Equal(t, 1, sends)
			}
			require.Equal(t, 1, uploads)
		})
	}
}

func TestVideoDurationProbeReplaysOversizedUnknownStream(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789"), (30<<20)/10+100)
	content := core.FileContent{Body: io.NopCloser(bytes.NewReader(data))}
	reader, duration, cleanup, err := prepareVideoReader(t.Context(), content)
	require.NoError(t, err)
	defer cleanup()
	require.Zero(t, duration)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Len(t, got, len(data))
	require.Equal(t, sha256.Sum256(data), sha256.Sum256(got))
}
