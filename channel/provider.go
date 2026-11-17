// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package channel

import (
	"context"
	"time"

	"github.com/volcengine/ark-runtime-go/channel/internal/handles"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// Message carries inbound channel facts without persistence or lease state.
type Message struct {
	Metadata          *MessageMetadata `json:"metadata,omitempty"`
	Unsupported       bool             `json:"unsupported,omitempty"`
	TrackReactions    bool             `json:"track_reactions,omitempty"`
	RecalledMessageID string           `json:"recalled_message_id,omitempty"`
	Attachments       []Attachment     `json:"attachments,omitempty"`
	BindingID         string           `json:"binding_id"`
	ProviderEventID   string           `json:"provider_event_id"`
	MessageID         string           `json:"message_id"`
	TenantKey         string           `json:"tenant_key"`
	ChatID            string           `json:"chat_id"`
	ThreadID          string           `json:"thread_id"`
	ParentID          string           `json:"parent_id"`
	ChatType          string           `json:"chat_type"`
	SenderID          string           `json:"sender_id"`
	Text              string           `json:"text"`
	Mentioned         bool             `json:"mentioned"`
	ReceivedAt        time.Time        `json:"received_at"`
}

// ReplyTarget identifies where to send a channel reply.
type ReplyTarget = core.ReplyTarget

// Delivery is the view supplied to a custom Provider without persistence records.
type Delivery struct {
	ID          string
	BindingID   string
	UUID        string
	ReplyTarget ReplyTarget
	Text        string
	Type        string
	Artifact    Artifact
	Parts       []RichTextPart
}

// Provider is the minimal custom-channel interface; all methods must honor ctx.
type Provider interface {
	Start(context.Context, func(context.Context, Message) error) error
	Deliver(context.Context, Delivery) (string, error)
}

// DeliveryFailure distinguishes definite rejection from unknown send outcomes
// to prevent duplicate delivery.
type DeliveryFailure = core.DeliveryFailure

// NewProvider wraps a custom channel in a Runtime handle.
func NewProvider(p Provider) *ProviderHandle {
	if p == nil {
		return nil
	}
	return handles.NewProvider(&providerAdapter{value: p}, nil)
}

type providerAdapter struct{ value Provider }

func (p *providerAdapter) Start(ctx context.Context, accept func(context.Context, core.Message) error) error {
	return p.value.Start(ctx, func(ctx context.Context, m Message) error {
		var v core.Message
		if err := project(m, &v); err != nil {
			return err
		}
		return accept(ctx, v)
	})
}
func (p *providerAdapter) Deliver(ctx context.Context, d core.Delivery) (string, error) {
	return p.value.Deliver(ctx, publicDelivery(d))
}
