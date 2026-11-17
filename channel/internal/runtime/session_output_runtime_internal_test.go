// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

type sessionOutputFixture struct {
	MAClient
	files     []SessionOutputFile
	session   string
	calls     int
	lateFiles []SessionOutputFile
}

func TestSessionFileDeliveryUsesSeparateStoredProtocol(t *testing.T) {
	r := &Runtime{options: RuntimeOptions{Artifacts: ArtifactLimits{MaxCount: 5}}}
	j := &job{Turn: Turn{ID: "turn", BindingID: "binding", Session: TurnSession{SessionID: "sesn-demo"}}}
	artifacts := []Artifact{{Kind: "document", FileID: "file-one", Path: "/mnt/session/outputs/a.pdf", SessionID: "sesn-demo"}}
	deliveries, _ := r.artifactDeliveries(j, artifacts)
	require.Len(t, deliveries, 1)
	require.EqualValues(t, "session_file", deliveries[0].Body.Type, "旧 worker 不能忽略 SessionID 并用 marker 凭据下载")
	raw, err := json.Marshal(deliveries[0].Body)
	require.NoError(t, err)
	var restored DeliveryBody
	require.NoError(t, json.Unmarshal(raw, &restored))
	require.Equal(t, artifacts[0], restored.Artifact)
	require.True(t, SupportsDeliveryBody(restored))
	require.True(t, deliveries[0].ValidNew(deliveries[0].Source))
}

func TestCollectSessionArtifactsDoesNotTreatDecimalsAsFiles(t *testing.T) {
	f := &sessionOutputFixture{}
	r := &Runtime{}
	j := &job{Turn: Turn{Session: TurnSession{SessionID: "sesn-demo"}}, binding: Binding{DeliverArtifacts: true}, connection: Connection{MAClient: f}}
	_, err := r.collectSessionArtifacts(t.Context(), j, "计算结果是 3.14，耗时 1.25 秒。", nil)
	require.NoError(t, err)
	require.Zero(t, f.calls)
}

func TestCollectSessionArtifactsDoesNotTreatDomainsAsFiles(t *testing.T) {
	f := &sessionOutputFixture{}
	r := &Runtime{}
	j := &job{Turn: Turn{Session: TurnSession{SessionID: "sesn-demo"}}, binding: Binding{DeliverArtifacts: true}, connection: Connection{MAClient: f}}
	_, err := r.collectSessionArtifacts(t.Context(), j, "访问 example.com 或 https://example.org/help 即可。", nil)
	require.NoError(t, err)
	require.Zero(t, f.calls)
}

func (f *sessionOutputFixture) ListSessionOutputFiles(_ context.Context, session string) ([]SessionOutputFile, error) {
	f.session = session
	f.calls++
	if f.calls > 1 && f.lateFiles != nil {
		return f.lateFiles, nil
	}
	return f.files, nil
}

func TestCollectSessionArtifactsWaitsForPartiallyIndexedFiles(t *testing.T) {
	first := SessionOutputFile{FileID: "file-one", Path: "/mnt/session/outputs/a.png"}
	second := SessionOutputFile{FileID: "file-two", Path: "/mnt/session/outputs/b.png"}
	f := &sessionOutputFixture{files: []SessionOutputFile{first}, lateFiles: []SessionOutputFile{first, second}}
	r := &Runtime{}
	j := &job{Turn: Turn{Session: TurnSession{SessionID: "sesn-demo"}}, binding: Binding{DeliverArtifacts: true}, connection: Connection{MAClient: f}}
	artifacts, err := r.collectSessionArtifacts(t.Context(), j, "`a.png` 和 `b.png`", nil)
	require.NoError(t, err)
	require.Len(t, artifacts, 2)
	require.Equal(t, 2, f.calls)
}

func TestCollectSessionArtifactsPreservesToolFileIDReferences(t *testing.T) {
	f := &sessionOutputFixture{}
	r := &Runtime{}
	j := &job{Turn: Turn{Session: TurnSession{SessionID: "sesn-demo"}}, binding: Binding{DeliverArtifacts: true}, connection: Connection{MAClient: f}}
	artifact := Artifact{Kind: "image", FileID: "file-tool", FileName: "preview.png"}
	artifacts, err := r.collectSessionArtifacts(t.Context(), j, "前![预览](file-tool)后", []Artifact{artifact})
	require.NoError(t, err)
	require.Equal(t, []Artifact{artifact}, artifacts)
	require.Len(t, j.answerReferences, 1)
	require.Equal(t, "file-tool", j.answerReferences[0].Artifact.FileID)
	require.Zero(t, f.calls, "工具结果已经提供 file_id，无需列 Session 文件")
}

func TestCollectSessionArtifactsMatchesNamesAndDeduplicatesLegacyPath(t *testing.T) {
	f := &sessionOutputFixture{files: []SessionOutputFile{
		{FileID: "file-one", Path: "/mnt/session/outputs/cats/a.png", FileName: "a.png"},
		{FileID: "file-old", Path: "/mnt/session/outputs/old.png", FileName: "old.png"},
	}}
	r := &Runtime{}
	j := &job{Turn: Turn{Session: TurnSession{SessionID: "sesn-demo"}}, binding: Binding{DeliverArtifacts: true}, connection: Connection{MAClient: f}}
	answer := "`/mnt/session/outputs/cats/`\n- `a.png`\n再次引用 `/mnt/session/outputs/cats/a.png`"
	artifacts, err := r.collectSessionArtifacts(t.Context(), j, answer, []Artifact{pathArtifact("/mnt/session/outputs/cats/a.png")})
	require.NoError(t, err)
	require.Len(t, artifacts, 1)
	require.Equal(t, "file-one", artifacts[0].FileID)
	require.Equal(t, "sesn-demo", artifacts[0].SessionID)
	require.Equal(t, "sesn-demo", f.session)
	require.Len(t, j.answerReferences, 2)
}
