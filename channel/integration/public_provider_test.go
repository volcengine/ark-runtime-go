// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/volcengine/ark-runtime-go/channel"
	"github.com/volcengine/ark-runtime-go/channel/internal/handles"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type publicMediaProvider struct {
	uploaded  string
	delivered []channel.RichTextPart
}

func (p *publicMediaProvider) Start(context.Context, func(context.Context, channel.Message) error) error {
	return nil
}
func (p *publicMediaProvider) Deliver(context.Context, channel.Delivery) (string, error) {
	return "plain", nil
}
func (p *publicMediaProvider) UploadInlineMedia(_ context.Context, a channel.Artifact, f channel.FileContent) (channel.RichTextPart, error) {
	data, err := io.ReadAll(f.Body)
	p.uploaded = string(data)
	return channel.RichTextPart{Type: a.Kind, ResourceKey: "image-key"}, err
}
func (p *publicMediaProvider) DeliverRichText(_ context.Context, d channel.Delivery) (string, error) {
	p.delivered = d.Parts
	return "rich-reply", nil
}

func TestPublicProviderBridgesInlineMediaAndRichDelivery(t *testing.T) {
	p := &publicMediaProvider{}
	native := handles.ProviderValue(channel.NewProvider(p))
	rich, ok := native.(core.RichTextDeliverer)
	if !ok {
		t.Fatal("public rich-text implementation is invisible to runtime")
	}
	media, ok := native.(core.InlineMediaUploader)
	if !ok {
		t.Fatal("public media implementation is invisible to runtime")
	}
	body := io.NopCloser(strings.NewReader("pixels"))
	defer func() { _ = body.Close() }()
	part, err := media.UploadInlineMedia(context.Background(), core.Artifact{Kind: "image"}, core.FileContent{Body: body})
	if err != nil {
		t.Fatal(err)
	}
	id, err := rich.DeliverRichText(context.Background(), core.Delivery{Body: core.DeliveryBody{Parts: []core.RichTextPart{part}}})
	if err != nil {
		t.Fatal(err)
	}
	if id != "rich-reply" || p.uploaded != "pixels" || len(p.delivered) != 1 || p.delivered[0].ResourceKey != "image-key" {
		t.Fatalf("media/rich bridge lost data: %q %q %+v", id, p.uploaded, p.delivered)
	}
}
