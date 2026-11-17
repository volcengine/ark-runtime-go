// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestTextStreamEncryptedRestartAndMetadataNoKMS(t *testing.T) {
	testTextStreamEncryptedRestartAndMetadataNoKMS(t, false)
}
func TestImageStreamEncryptedRestartAndMetadataNoKMS(t *testing.T) {
	testTextStreamEncryptedRestartAndMetadataNoKMS(t, true)
}
func testTextStreamEncryptedRestartAndMetadataNoKMS(t *testing.T, image bool) {
	path := filepath.Join(t.TempDir(), "stream.db")
	c := &payloadTestCodec{}
	opts := Options{NoSync: true, PayloadCodec: c, PayloadNamespace: "stream-test", RequirePayloadEncryption: true}
	s, err := openInstalledFixture(path, opts)
	require.NoError(t, err)
	_, err = s.Ingest(t.Context(), conformance.SampleVersion(), conformance.SampleMessage("stream"))
	require.NoError(t, err)
	turn, err := s.LeaseTurn(t.Context(), conformance.Claim("worker"))
	require.NoError(t, err)
	d := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: turn.Turn.ID}, 0, "answer", turn.Turn.BindingID, core.ReplyTarget{ChatID: "chat"}, core.StreamTextBody("secret-final-answer"))
	if image {
		d.Body.Type = core.BodyImageStream
		d.Body.ImageFallbackText = "`![secret-original](/mnt/session/outputs/private.png)`"
	}
	require.NoError(t, s.SettleTurn(t.Context(), turn.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{d}))
	leased, err := s.LeaseDelivery(t.Context(), "sender", time.Minute)
	require.NoError(t, err)
	seals, opens := c.counts()
	require.NoError(t, s.UpdateTextStream(t.Context(), leased.Lease, core.TextStreamState{Handle: "card", ImagesAsCode: image}))
	a, b := c.counts()
	require.Equal(t, seals, a)
	require.Equal(t, opens, b)
	require.NoError(t, s.Close())
	s, err = openInstalledFixture(path, opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	rows, err := s.ListDeliveries(t.Context(), core.DeliveryQuery{})
	require.NoError(t, err)
	require.Equal(t, "secret-final-answer", rows[0].Body.Text)
	require.Equal(t, "card", rows[0].TextStream.Handle)
	require.Equal(t, image, rows[0].TextStream.ImagesAsCode)
	require.Equal(t, d.Body.ImageFallbackText, rows[0].Body.ImageFallbackText)
}
