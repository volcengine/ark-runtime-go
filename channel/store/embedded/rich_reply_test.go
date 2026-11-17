// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestRichReplyEncryptedRestartPreservesMediaAndUUID(t *testing.T) {
	file := filepath.Join(t.TempDir(), "rich.db")
	codec := &payloadTestCodec{}
	opts := Options{NoSync: true, PayloadCodec: codec, PayloadNamespace: "rich-test", RequirePayloadEncryption: true}
	s, err := openInstalledFixture(file, opts)
	require.NoError(t, err)
	_, err = s.Ingest(t.Context(), conformance.SampleVersion(), conformance.SampleMessage("rich"))
	require.NoError(t, err)
	turn, err := s.LeaseTurn(t.Context(), conformance.Claim("worker"))
	require.NoError(t, err)
	body := core.RichTextBody([]core.RichTextPart{{Type: "text", Text: "secret-rich-answer"}, {Type: "image", ResourceKey: "img_secret_uploaded"}, {Type: "video", ResourceKey: "file_secret_video"}})
	d := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: turn.Turn.ID}, 0, "answer", turn.Turn.BindingID, core.ReplyTarget{ChatID: "chat"}, body)
	require.NoError(t, s.SettleTurn(t.Context(), turn.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{d}))
	require.NoError(t, s.Close())
	raw, err := os.ReadFile(file)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "secret-rich-answer")
	require.NotContains(t, string(raw), "img_secret_uploaded")
	s, err = openInstalledFixture(file, opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	rows, err := s.ListDeliveries(t.Context(), core.DeliveryQuery{})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, body, rows[0].Body)
	require.Equal(t, d.UUID, rows[0].UUID)
}
