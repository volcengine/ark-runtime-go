// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package payload

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestRedactedDeliveryRejectsImageFallbackPlaintext(t *testing.T) {
	d := &core.Delivery{Body: core.DeliveryBody{Type: core.BodyText, ImageFallbackText: "secret original image path"}}
	require.ErrorIs(t, ValidateRedacted(d), ErrInvalid)
}

func TestRedactedDeliveryRejectsRichTextPlaintext(t *testing.T) {
	d := &core.Delivery{Body: core.DeliveryBody{Type: core.BodyText, Parts: []core.RichTextPart{{Type: "text", Text: "secret answer"}}}}
	require.ErrorIs(t, ValidateRedacted(d), ErrInvalid)
}
