// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// outputResolver 模拟托管侧渲染接口：只签名本会话产出路径。
type outputResolver struct {
	mu    sync.Mutex
	calls []string
	fail  bool
}

func (r *outputResolver) ResolveOutputURL(_ context.Context, sessionID, path string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, sessionID+"|"+path)
	if r.fail {
		return "", errors.New("render unavailable")
	}
	return "https://tos.example/" + strings.TrimPrefix(path, "/mnt/session/") + "?sig=fixture", nil
}

// outputMA 在公开 MA 客户端之上提供预签名地址下载与 Files 下载。
type outputMA struct {
	core.MAClient
	objects map[string]string
	large   map[string]bool
}

func (m outputMA) DownloadURL(_ context.Context, url string, _ int64) (core.FileContent, error) {
	if m.large[url] {
		return core.FileContent{}, fmt.Errorf("fixture: %w", core.ErrFileTooLarge)
	}
	data, ok := m.objects[url]
	if !ok {
		return core.FileContent{}, errors.New("missing object")
	}
	return core.FileContent{Body: io.NopCloser(strings.NewReader(data)), Size: int64(len(data))}, nil
}

func (m outputMA) DownloadFile(_ context.Context, id string, _ int64) (core.FileContent, error) {
	data, ok := m.objects[id]
	if !ok {
		return core.FileContent{}, errors.New("missing")
	}
	return core.FileContent{Body: io.NopCloser(strings.NewReader(data)), FileName: id + ".bin", Size: int64(len(data))}, nil
}

type linkProvider struct {
	artifactProvider
	links []string
}

func (p *linkProvider) DeliverArtifactLink(_ context.Context, delivery core.Delivery, name, url string) (string, error) {
	p.amu.Lock()
	defer p.amu.Unlock()
	p.order = append(p.order, "link")
	p.links = append(p.links, name+"|"+url)
	return "om-link-" + delivery.ID, nil
}

func outputHarness(t *testing.T, client core.MAClient, p core.Provider, resolver core.OutputFileResolver, all bool) *rtHarness {
	t.Helper()
	binding := runtimeBinding()
	binding.MaxTurnDuration = 0
	binding.DeliverArtifacts = true
	binding.OutputArtifactsAll = all
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: binding, MAClient: client, Provider: p, Outputs: resolver}}, testRuntimeOptions())
	require.NoError(t, err)
	return &rtHarness{t: t, store: store, fixture: &conformance.MAServer{}, runtime: runtime}
}

func waitDeliveries(t *testing.T, store core.Exporter, want int) []core.Delivery {
	t.Helper()
	var deliveries []core.Delivery
	require.Eventually(t, func() bool {
		export, err := store.Export(context.Background())
		if err != nil || len(export.Deliveries) != want {
			return false
		}
		for _, delivery := range export.Deliveries {
			if !delivery.Status.Terminal() {
				return false
			}
		}
		deliveries = export.Deliveries
		return true
	}, 15*time.Second, 10*time.Millisecond)
	return deliveries
}

func TestRuntimeDeliversSessionOutputFiles(t *testing.T) {
	f := &conformance.MAServer{}
	p := &linkProvider{}
	resolver := &outputResolver{}
	client := outputMA{MAClient: newMAForHarness(t, f), objects: map[string]string{
		"https://tos.example/outputs/report.md?sig=fixture": "# report",
		"https://tos.example/outputs/chart.png?sig=fixture": "PNG",
		"file-doc": "PDF",
	}, large: map[string]bool{"https://tos.example/outputs/big.mp4?sig=fixture": true}}
	h := outputHarness(t, client, p, resolver, true)
	h.fixture = f
	h.run()
	h.enqueue(runtimeMessage())
	events := []map[string]any{
		{"id": "w1", "type": "agent.tool_use", "name": "write", "input": map[string]any{"file_path": "/mnt/session/outputs/report.md", "content": "# report"}},
		{"id": "r1", "type": "agent.tool_result", "tool_use_id": "w1", "content": []any{map[string]any{"type": "text", "text": "ok"}}},
		toolResultWithFiles("t2", map[string]any{"type": "document", "title": "doc.pdf", "source": map[string]any{"type": "file", "file_id": "file-doc"}}),
	}
	events = append(events, conformance.AnswerEvents("已生成：/mnt/session/outputs/chart.png 和 `/workspace/outputs/big.mp4`")...)
	f.AppendAfterInput(t, events...)

	deliveries := waitDeliveries(t, h.store, 5)
	for _, delivery := range deliveries {
		require.Equal(t, core.DeliveryDone, delivery.Status, "delivery %s (%s) failed: %s", delivery.ID, delivery.Body.Type, delivery.ErrorKind)
	}
	p.amu.Lock()
	defer p.amu.Unlock()
	require.Equal(t, []string{"text", "artifact", "link", "artifact", "artifact"}, p.order, "answer-referenced files first (link for the oversize one), then file refs, then unreferenced writes")
	require.Equal(t, []string{"image:chart.png:PNG", "document:file-doc.bin:PDF", "document:report.md:# report"}, p.artifacts, "output files keep their basename and inferred kind")
	require.Equal(t, []string{"big.mp4|https://tos.example/outputs/big.mp4?sig=fixture"}, p.links)
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	require.Equal(t, []string{"sesn-demo|/mnt/session/outputs/chart.png", "sesn-demo|/mnt/session/outputs/big.mp4", "sesn-demo|/mnt/session/outputs/report.md"}, resolver.calls, "every output path is signed for the turn's own session")
	export, err := h.store.Export(context.Background())
	require.NoError(t, err)
	for _, delivery := range export.Deliveries {
		require.NotContains(t, delivery.Body.Artifact.Path, "sig=", "presigned urls are never persisted")
		require.NotContains(t, delivery.Body.Artifact.FileName, "sig=")
	}
}

func TestRuntimeExplicitModeSkipsUnreferencedWrites(t *testing.T) {
	f := &conformance.MAServer{}
	p := &linkProvider{}
	resolver := &outputResolver{}
	client := outputMA{MAClient: newMAForHarness(t, f), objects: map[string]string{
		"https://tos.example/outputs/chart.png?sig=fixture": "PNG",
		"file-doc": "PDF",
	}, large: map[string]bool{"https://tos.example/outputs/big.mp4?sig=fixture": true}}
	h := outputHarness(t, client, p, resolver, false)
	h.fixture = f
	h.run()
	h.enqueue(runtimeMessage())
	events := []map[string]any{
		// report.md 被 write 写过，但最终答案没有提到它：explicit 模式不投递。
		{"id": "w1", "type": "agent.tool_use", "name": "write", "input": map[string]any{"file_path": "/mnt/session/outputs/report.md", "content": "# report"}},
		{"id": "r1", "type": "agent.tool_result", "tool_use_id": "w1", "content": []any{map[string]any{"type": "text", "text": "ok"}}},
		toolResultWithFiles("t2", map[string]any{"type": "document", "title": "doc.pdf", "source": map[string]any{"type": "file", "file_id": "file-doc"}}),
	}
	events = append(events, conformance.AnswerEvents("已生成：/mnt/session/outputs/chart.png 和 `/workspace/outputs/big.mp4`")...)
	f.AppendAfterInput(t, events...)

	waitDeliveries(t, h.store, 4)
	p.amu.Lock()
	defer p.amu.Unlock()
	require.Equal(t, []string{"text", "artifact", "link", "artifact"}, p.order, "Files references still go out; unreferenced write does not")
	require.Equal(t, []string{"image:chart.png:PNG", "document:file-doc.bin:PDF"}, p.artifacts)
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	require.Equal(t, []string{"sesn-demo|/mnt/session/outputs/chart.png", "sesn-demo|/mnt/session/outputs/big.mp4"}, resolver.calls, "the unreferenced write is never signed either")
}

func TestRuntimeIgnoresOutputPathsWithoutResolver(t *testing.T) {
	f := &conformance.MAServer{}
	p := &artifactProvider{}
	client := downloadMA{MAClient: newMAForHarness(t, f), files: map[string]string{}}
	h := artifactHarness(t, client, p)
	h.fixture = f
	h.run()
	h.enqueue(runtimeMessage())
	f.AppendAfterInput(t, conformance.AnswerEvents("文件在 /mnt/session/outputs/report.md")...)
	deliveries := waitDeliveries(t, h.store, 1)
	require.Equal(t, core.BodyText, deliveries[0].Body.Type)
}

func TestRuntimeOutputFileWithoutLinkSupportFailsAfterAnswer(t *testing.T) {
	f := &conformance.MAServer{}
	p := &artifactProvider{}
	client := outputMA{MAClient: newMAForHarness(t, f), large: map[string]bool{"https://tos.example/outputs/big.zip?sig=fixture": true}}
	h := outputHarness(t, client, p, &outputResolver{}, true)
	h.fixture = f
	h.run()
	h.enqueue(runtimeMessage())
	f.AppendAfterInput(t, conformance.AnswerEvents("见 /mnt/session/outputs/big.zip")...)
	deliveries := waitDeliveries(t, h.store, 2)
	require.Equal(t, core.DeliveryDone, deliveries[0].Status)
	require.Equal(t, core.DeliveryFailed, deliveries[1].Status)
	require.Equal(t, "artifact_too_large", deliveries[1].ErrorKind)
}
