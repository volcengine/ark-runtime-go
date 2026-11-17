// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func fileLinksHarness(t *testing.T, environment string, mounts bool, fail bool, options core.RuntimeOptions) (*rtHarness, *atomic.Int32) {
	t.Helper()
	f := &conformance.MAServer{EnvironmentType: environment}
	p := &attachmentProvider{
		files: map[string]string{"image": "png", "video": "mp4", "pdf": "%PDF", "json": `{"hello":"world"}`},
		mimes: map[string]string{"image": "image/png", "video": "video/mp4", "pdf": "application/pdf", "json": "application/json"},
	}
	binding := runtimeBinding()
	binding.FileMounts = mounts
	h := newHarnessWith(t, f, p, binding, options)
	client, err := newTestMA(h.server.URL, "fixture-key", ma.ClientOptions{RequestTimeout: time.Second, StreamTimeout: time.Second, FilePollInterval: time.Millisecond})
	require.NoError(t, err)
	h.runtime, err = core.NewRuntime(h.store, []core.Connection{{Binding: binding, MAClient: client, Provider: p}}, options)
	require.NoError(t, err)
	calls := &atomic.Int32{}
	h.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v3/files/") && r.Header.Get("X-Ark-PreSignedURL-ExpiresAfter") != "" {
			calls.Add(1)
			require.Equal(t, "Bearer fixture-key", r.Header.Get("Authorization"))
			if fail {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			fileID := strings.TrimPrefix(r.URL.Path, "/api/v3/files/")
			_ = json.NewEncoder(w).Encode(map[string]any{"id": fileID, "status": "active", "download_url": "https://storage.example/" + fileID + "?signature=fixture"})
			return
		}
		f.ServeHTTP(w, r)
	})
	return h, calls
}

func TestRuntimeFileLinksReuseOnRetryAndRenewOnNewGeneration(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusNotFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			h, calls := fileLinksHarness(t, "self_hosted", false, false, testRuntimeOptions())
			h.fixture.SendCodes = []int{status}
			if status == http.StatusNotFound {
				h.fixture.SendErrorCodes = []string{"InvalidSession"}
			}
			message := runtimeMessage()
			message.Attachments = []core.Attachment{{Kind: "file", Key: "json", FileName: "sessions.json"}}
			h.enqueue(message)
			h.run()
			h.fixture.AppendAfterInputCount(t, 2, conformance.AnswerEvents("processed")...)
			state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
			if status == http.StatusNotFound {
				require.EqualValues(t, 2, calls.Load(), "a replacement Session gets freshly signed input")
				require.EqualValues(t, 2, state.turn.Session.Generation)
			} else {
				require.EqualValues(t, 1, calls.Load(), "an explicitly rejected send reuses the frozen link")
			}
			h.fixture.Lock()
			defer h.fixture.Unlock()
			require.Len(t, h.fixture.Uploads, 1)
		})
	}
}

type fileLinkFreezeStopStore struct {
	core.Store
	stop context.CancelFunc
	once atomic.Bool
}

func (s *fileLinkFreezeStopStore) UpdateTurn(ctx context.Context, lease core.Lease, update core.TurnUpdate) (core.Turn, error) {
	turn, err := s.Store.UpdateTurn(ctx, lease, update)
	if err == nil && update.Input.MountsFrozen && s.once.CompareAndSwap(false, true) {
		s.stop()
		return turn, context.Canceled
	}
	return turn, err
}

func TestRuntimeFileLinksSurviveRestartBeforeSubmit(t *testing.T) {
	h, calls := fileLinksHarness(t, "self_hosted", false, false, testRuntimeOptions())
	client, err := newTestMA(h.server.URL, "fixture-key", ma.ClientOptions{FilePollInterval: time.Millisecond})
	require.NoError(t, err)
	message := runtimeMessage()
	message.Attachments = []core.Attachment{{Kind: "file", Key: "json", FileName: "sessions.json"}}
	provider := &attachmentProvider{files: map[string]string{"json": `{"value":1}`}, mimes: map[string]string{"json": "application/json"}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	fault := &fileLinkFreezeStopStore{Store: h.store, stop: cancel}
	options := testRuntimeOptions()
	options.MessageDeadline = 5 * time.Second
	first, err := core.NewRuntime(fault, []core.Connection{{Binding: runtimeBinding(), MAClient: client, Provider: provider}}, options)
	require.NoError(t, err)
	ingested := mustEnqueue(t, first, "binding", message)
	require.NoError(t, first.Run(ctx))
	before, err := h.store.GetTurn(context.Background(), ingested.Turn.ID)
	require.NoError(t, err)
	require.True(t, fault.once.Load(), "the first process must stop after freezing file access")
	require.True(t, before.Input.MountsFrozen)
	require.Len(t, before.Input.Mounts, 1)
	require.Equal(t, "https://storage.example/file-1?signature=fixture", before.Input.Mounts[0].DownloadURL)
	second, err := core.NewRuntime(h.store, []core.Connection{{Binding: runtimeBinding(), MAClient: client, Provider: &runtimeProvider{}}}, options)
	require.NoError(t, err)
	runUntilCleanup(t, second)
	h.fixture.AppendAfterInput(t, conformance.AnswerEvents("recovered")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.EqualValues(t, 1, calls.Load())
	h.fixture.Lock()
	defer h.fixture.Unlock()
	require.Len(t, h.fixture.Uploads, 1)
}

func TestFileDownloadHintPreservesSignedURL(t *testing.T) {
	link := "https://storage.example/object?signature=a%2Fb&expires=86400"
	input := core.UserInput{
		Blocks: []core.InputBlock{{Type: "notice", FileID: "file-1", Title: "sessions.json"}},
		Mounts: []core.MountedFile{{FileID: "file-1", Skipped: true, DownloadURL: link}},
	}
	hint := input.AttachmentText(input.Blocks[0])
	require.Contains(t, hint, link, "download instructions must preserve the copyable signed URL")
	require.Equal(t, hint, input.ExpectedText())
}

func TestRuntimeCloudNoticeBecomesDownloadOnSelfHostedGeneration(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		name := "ordinary file"
		if mixed {
			name = "mixed attachments"
		}
		t.Run(name, func(t *testing.T) {
			h, calls := fileLinksHarness(t, "cloud", false, false, testRuntimeOptions())
			h.fixture.SendCodes = []int{http.StatusNotFound}
			h.fixture.SendErrorCodes = []string{"InvalidSession"}
			delegate := h.server.Config.Handler
			var sends atomic.Int32
			h.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				delegate.ServeHTTP(w, r)
				if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/events") && sends.Add(1) == 1 {
					h.fixture.Lock()
					h.fixture.EnvironmentType = "self_hosted"
					h.fixture.Unlock()
				}
			})
			message := runtimeMessage()
			message.Attachments = []core.Attachment{{Kind: "file", Key: "json", FileName: "sessions.json"}}
			if mixed {
				message.Attachments = append(message.Attachments, core.Attachment{Kind: "image", Key: "image", FileName: "photo.png"})
			}
			h.enqueue(message)
			h.run()
			h.fixture.AppendAfterInputCount(t, 2, conformance.AnswerEvents("recovered")...)
			state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
			require.EqualValues(t, 2, state.turn.Session.Generation)
			require.Len(t, state.turn.Input.Mounts, len(message.Attachments))
			require.EqualValues(t, len(message.Attachments), calls.Load())
			require.Contains(t, strings.Join(lastContentTexts(h.fixture), ""), "https://storage.example/file-1?signature=fixture")
			h.fixture.Lock()
			defer h.fixture.Unlock()
			require.Len(t, h.fixture.Uploads, len(message.Attachments), "a generation change must reuse frozen uploads")
		})
	}
}

func TestRuntimeAttachmentEnvironmentMatrix(t *testing.T) {
	for _, environment := range []string{"cloud", "self_hosted"} {
		for _, mounts := range []bool{false, true} {
			name := environment + "/mounts-off"
			if mounts {
				name = environment + "/mounts-on"
			}
			t.Run(name, func(t *testing.T) {
				h, calls := fileLinksHarness(t, environment, mounts, false, testRuntimeOptions())
				message := runtimeMessage()
				message.Attachments = []core.Attachment{
					{Kind: "image", Key: "image", FileName: "photo.png"},
					{Kind: "file", Key: "video", FileName: "clip.mp4"},
					{Kind: "file", Key: "pdf", FileName: "report.pdf"},
					{Kind: "file", Key: "json", FileName: "sessions.json"},
				}
				h.enqueue(message)
				h.run()
				h.fixture.AppendAfterInput(t, conformance.AnswerEvents("processed")...)
				state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
				texts := strings.Join(lastContentTexts(h.fixture), "")
				h.fixture.Lock()
				defer h.fixture.Unlock()
				var kinds []string
				for _, block := range h.fixture.LastContent {
					item := block.(map[string]any)
					if item["type"] != "text" {
						kinds = append(kinds, item["type"].(string))
					}
				}
				if environment == "self_hosted" {
					require.Len(t, h.fixture.Uploads, 4, "self-hosted ordinary files must upload even when platform mounts are disabled")
					require.Equal(t, "agent", h.fixture.Uploads[3].Purpose)
					require.Equal(t, []string{"image", "video", "document"}, kinds)
					require.Empty(t, h.fixture.SessionResources)
					require.EqualValues(t, 4, calls.Load())
					for _, id := range []string{"file-1", "file-2", "file-3", "file-4"} {
						require.Contains(t, texts, "https://storage.example/"+id+"?signature=fixture")
					}
					require.Contains(t, texts, "curl")
					require.NotContains(t, texts, "fixture-key")
					require.NotContains(t, texts, "/mnt/session/uploads/")
					require.Len(t, state.turn.Input.Mounts, 4)
				} else if mounts {
					require.Len(t, h.fixture.Uploads, 4)
					require.Len(t, h.fixture.SessionResources["sesn-demo"], 4)
					require.Equal(t, []string{"image", "video"}, kinds)
					require.Contains(t, texts, "/mnt/session/uploads/")
					require.Zero(t, calls.Load())
				} else {
					require.Len(t, h.fixture.Uploads, 4, "upload facts remain reusable if a later generation changes environment")
					require.Empty(t, h.fixture.SessionResources)
					require.Equal(t, []string{"image", "video", "document"}, kinds)
					require.Contains(t, texts, "暂不支持直接读取该格式")
					require.NotContains(t, texts, "https://storage.example/")
					require.Zero(t, calls.Load())
				}
			})
		}
	}
}

func TestRuntimeFileLinkFailureDoesNotSubmitPartialInput(t *testing.T) {
	h, calls := fileLinksHarness(t, "self_hosted", false, true, deadlineRuntimeOptions())
	message := runtimeMessage()
	message.Attachments = []core.Attachment{{Kind: "file", Key: "json", FileName: "sessions.json"}}
	h.enqueue(message)
	h.run()
	state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 6*time.Second)
	require.NotNil(t, state.turn.Outcome)
	require.Positive(t, calls.Load())
	h.fixture.Lock()
	defer h.fixture.Unlock()
	require.Zero(t, h.fixture.SendCount)
	require.Len(t, h.fixture.Uploads, 1, "retry URL resolution without uploading the same attachment again")
}
