// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package channel

import (
	"context"
	"reflect"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// Attachment references a channel resource without persistence metadata.
type Attachment struct {
	Kind     string `json:"kind"`
	Key      string `json:"key"`
	FileName string `json:"file_name,omitempty"`
}

// DownloadedAttachment contains an attachment stream that the caller must close.
type DownloadedAttachment = core.DownloadedAttachment

// FileContent contains a file stream that the caller must close.
type FileContent = core.FileContent

// Artifact identifies an output file without download credentials.
type Artifact = core.Artifact

// RichTextPart contains text or a reference to uploaded media.
type RichTextPart = core.RichTextPart

// ContextMessage is a bounded historical-message view.
type ContextMessage = core.ContextMessage

// HistoryScope identifies the permitted conversation-history range.
type HistoryScope = core.HistoryScope

// HistoryQuery bounds the history scope and read budget.
type HistoryQuery struct {
	Message Message
	Scope   HistoryScope
	Since   time.Time
	Limit   int
}

// ProgressUpdate is a progress-display snapshot.
type ProgressUpdate = core.ProgressUpdate

// ProgressStep describes a tool or thinking step.
type ProgressStep = core.ProgressStep

// ProgressState identifies the progress state.
type ProgressState = core.ProgressState

// ProgressCheckpoint is the delivery cursor for a channel progress card.
type ProgressCheckpoint = core.ProgressCheckpoint

// ApprovalRequest describes a tool approval request.
type ApprovalRequest = core.ApprovalRequest

// ApprovalDecision contains a decision after signature and permission validation.
type ApprovalDecision = core.ApprovalDecision

// Controller receives approval callbacks.
type Controller = core.Controller

// ControlBinder accepts the Runtime approval controller.
type ControlBinder = core.ControlBinder

// ApprovalPrompter optionally displays approval requests.
type ApprovalPrompter = core.ApprovalPrompter

// ProgressReporter optionally displays progress.
type ProgressReporter = core.ProgressReporter

// DurableProgressReporter optionally displays resumable progress.
type DurableProgressReporter = core.DurableProgressReporter

// BotTokenSource optionally provides bot identity credentials.
type BotTokenSource = core.BotTokenSource

// AttachmentFetcher optionally reads channel attachments.
type AttachmentFetcher interface {
	FetchAttachment(context.Context, Message, Attachment) (DownloadedAttachment, error)
}

// HistoryFetcher optionally reads recent conversation context.
type HistoryFetcher interface {
	FetchHistory(context.Context, HistoryQuery) ([]ContextMessage, error)
}

// ReferenceFetcher optionally reads explicitly referenced messages.
type ReferenceFetcher interface {
	FetchReference(context.Context, Message) (ContextMessage, error)
}

// ArtifactDeliverer optionally delivers files.
type ArtifactDeliverer interface {
	DeliverArtifact(context.Context, Delivery, FileContent) (string, error)
}

// InlineImageUploader optionally uploads inline images.
type InlineImageUploader = core.InlineImageUploader

// InlineMediaUploader optionally uploads inline media.
type InlineMediaUploader = core.InlineMediaUploader

func publicMessage(m core.Message) (Message, error) {
	var result Message
	err := project(m, &result)
	return result, err
}
func publicDelivery(d core.Delivery) Delivery {
	return Delivery{ID: d.ID, BindingID: d.BindingID, UUID: d.UUID, ReplyTarget: d.ReplyTarget, Text: d.Body.Text, Type: string(d.Body.Type), Artifact: d.Body.Artifact, Parts: d.Body.Parts}
}
func (p *providerAdapter) BindControl(c core.Controller) {
	if v, ok := p.value.(ControlBinder); ok {
		v.BindControl(c)
	}
}
func (p *providerAdapter) PromptApproval(ctx context.Context, r core.ApprovalRequest) error {
	if v, ok := p.value.(ApprovalPrompter); ok {
		return v.PromptApproval(ctx, r)
	}
	return core.ErrUnsupportedContract
}
func (p *providerAdapter) ReportProgress(ctx context.Context, h string, u core.ProgressUpdate) (string, error) {
	if v, ok := p.value.(ProgressReporter); ok {
		return v.ReportProgress(ctx, h, u)
	}
	return "", core.ErrUnsupportedContract
}
func (p *providerAdapter) ReportDurableProgress(ctx context.Context, c core.ProgressCheckpoint, u core.ProgressUpdate) (core.ProgressCheckpoint, error) {
	if v, ok := p.value.(DurableProgressReporter); ok {
		return v.ReportDurableProgress(ctx, c, u)
	}
	return c, core.ErrUnsupportedContract
}
func (p *providerAdapter) BotToken(ctx context.Context) (string, time.Time, error) {
	if v, ok := p.value.(BotTokenSource); ok {
		return v.BotToken(ctx)
	}
	return "", time.Time{}, core.ErrUnsupportedContract
}
func (p *providerAdapter) FetchAttachment(ctx context.Context, m core.Message, a core.Attachment) (core.DownloadedAttachment, error) {
	if v, ok := p.value.(AttachmentFetcher); ok {
		msg, err := publicMessage(m)
		if err != nil {
			return core.DownloadedAttachment{}, err
		}
		return v.FetchAttachment(ctx, msg, Attachment{Kind: a.Kind, Key: a.Key, FileName: a.FileName})
	}
	return core.DownloadedAttachment{}, core.ErrUnsupportedContract
}
func (p *providerAdapter) FetchHistory(ctx context.Context, q core.HistoryQuery) ([]core.ContextMessage, error) {
	if v, ok := p.value.(HistoryFetcher); ok {
		msg, err := publicMessage(q.Message)
		if err != nil {
			return nil, err
		}
		return v.FetchHistory(ctx, HistoryQuery{Message: msg, Scope: q.Scope, Since: q.Since, Limit: q.Limit})
	}
	return nil, core.ErrUnsupportedContract
}
func (p *providerAdapter) FetchReference(ctx context.Context, m core.Message) (core.ContextMessage, error) {
	if v, ok := p.value.(ReferenceFetcher); ok {
		msg, err := publicMessage(m)
		if err != nil {
			return core.ContextMessage{}, err
		}
		return v.FetchReference(ctx, msg)
	}
	return core.ContextMessage{}, core.ErrUnsupportedContract
}
func (p *providerAdapter) DeliverArtifact(ctx context.Context, d core.Delivery, c core.FileContent) (string, error) {
	if v, ok := p.value.(ArtifactDeliverer); ok {
		return v.DeliverArtifact(ctx, publicDelivery(d), c)
	}
	return "", core.ErrUnsupportedContract
}
func (p *providerAdapter) UploadInlineImage(ctx context.Context, c core.FileContent) (string, error) {
	if v, ok := p.value.(InlineImageUploader); ok {
		return v.UploadInlineImage(ctx, c)
	}
	return "", core.ErrUnsupportedContract
}
func (p *providerAdapter) UploadInlineMedia(ctx context.Context, a core.Artifact, c core.FileContent) (core.RichTextPart, error) {
	if v, ok := p.value.(InlineMediaUploader); ok {
		return v.UploadInlineMedia(ctx, a, c)
	}
	return core.RichTextPart{}, core.ErrUnsupportedContract
}

// SupportsProviderCapability 只向运行时登记调用方实际实现的能力。
func (p *providerAdapter) SupportsProviderCapability(t reflect.Type) bool {
	switch t.Name() {
	case "RichTextDeliverer":
		_, ok := p.value.(RichTextDeliverer)
		return ok
	case "ControlBinder":
		_, ok := p.value.(ControlBinder)
		return ok
	case "ApprovalPrompter":
		_, ok := p.value.(ApprovalPrompter)
		return ok
	case "ProgressReporter":
		_, ok := p.value.(ProgressReporter)
		return ok
	case "DurableProgressReporter":
		_, ok := p.value.(DurableProgressReporter)
		return ok
	case "BotTokenSource":
		_, ok := p.value.(BotTokenSource)
		return ok
	case "AttachmentFetcher":
		_, ok := p.value.(AttachmentFetcher)
		return ok
	case "HistoryFetcher":
		_, ok := p.value.(HistoryFetcher)
		return ok
	case "ReferenceFetcher":
		_, ok := p.value.(ReferenceFetcher)
		return ok
	case "ArtifactDeliverer":
		_, ok := p.value.(ArtifactDeliverer)
		return ok
	case "InlineImageUploader":
		_, ok := p.value.(InlineImageUploader)
		return ok
	case "InlineMediaUploader":
		_, ok := p.value.(InlineMediaUploader)
		return ok
	default:
		return false
	}
}

// MessageMetadata retains message location and mentions without persistence records.
type MessageMetadata = core.MessageMetadata

// Mention identifies a mention in a message.
type Mention = core.Mention

// RichTextDeliverer optionally delivers rich text and can be combined with InlineMediaUploader.
type RichTextDeliverer interface {
	DeliverRichText(context.Context, Delivery) (string, error)
}

func (p *providerAdapter) DeliverRichText(ctx context.Context, d core.Delivery) (string, error) {
	if v, ok := p.value.(RichTextDeliverer); ok {
		return v.DeliverRichText(ctx, publicDelivery(d))
	}
	return "", core.ErrUnsupportedContract
}
