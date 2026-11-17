// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"strings"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

var _ core.MessageReactionReporter = (*Provider)(nil)

func reactionEmoji(state core.MessageReaction) (string, bool) {
	switch state {
	case core.ReactionQueued:
		return "OneSecond", true
	case core.ReactionRunning:
		return "Get", true
	case core.ReactionDone:
		return "DONE", true
	case "":
		return "", true
	}
	return "", false
}

func processingEmoji(emoji string) bool {
	return emoji == "OneSecond" || emoji == "Get" || emoji == "DONE"
}

func reactionGone(code int) bool { return code == 231003 || code == 231005 }

// SyncMessageReaction 只维护本应用的处理表情，先删旧状态再添加新状态；重试先查询远端。
func (p *Provider) SyncMessageReaction(ctx context.Context, target core.ReplyTarget, state core.MessageReaction) error {
	want, valid := reactionEmoji(state)
	if !valid || ctx == nil || p.client == nil || target.MessageID == "" || strings.ContainsAny(target.MessageID, "/\\?#%") {
		return failure{"feishu reaction target invalid", false, false, "invalid_target"}
	}
	ctx, cancel := boundedContext(ctx)
	defer cancel()
	items, gone, err := p.messageReactions(ctx, target.MessageID)
	if err != nil || gone {
		return err
	}
	found := false
	for _, item := range items {
		if item == nil || item.Operator == nil || deref(item.Operator.OperatorType) != "app" || deref(item.Operator.OperatorId) != p.binding.AppID || item.ReactionType == nil {
			continue
		}
		emoji := deref(item.ReactionType.EmojiType)
		if !processingEmoji(emoji) {
			continue
		}
		if emoji == want && !found {
			found = true
			continue
		}
		id := deref(item.ReactionId)
		if id == "" || strings.ContainsAny(id, "/\\?#%") {
			return failure{"feishu reaction response invalid", false, true, "invalid_response"}
		}
		resp, err := p.client.Im.MessageReaction.Delete(ctx, larkim.NewDeleteMessageReactionReqBuilder().MessageId(target.MessageID).ReactionId(id).Build())
		if err != nil {
			return transportFailure(err)
		}
		if resp == nil || resp.ApiResp == nil {
			return failure{"feishu reaction response missing", true, true, "response_missing"}
		}
		if reactionGone(resp.Code) {
			return nil
		}
		if resp.Code == 231011 {
			return failure{"feishu reaction changed during sync", false, true, "reaction_changed"}
		}
		if err := responseFailure(resp.StatusCode, resp.Code, resp.Success(), true); err != nil {
			return err
		}
	}
	if want == "" || found {
		return nil
	}
	body := larkim.NewCreateMessageReactionReqBodyBuilder().ReactionType(larkim.NewEmojiBuilder().EmojiType(want).Build()).Build()
	resp, err := p.client.Im.MessageReaction.Create(ctx, larkim.NewCreateMessageReactionReqBuilder().MessageId(target.MessageID).Body(body).Build())
	if err != nil {
		return transportFailure(err)
	}
	if resp == nil || resp.ApiResp == nil {
		return failure{"feishu reaction response missing", true, true, "response_missing"}
	}
	if reactionGone(resp.Code) {
		return nil
	}
	if resp.Code == 231015 {
		return failure{"feishu reaction request processing", false, true, "reaction_processing"}
	}
	return responseFailure(resp.StatusCode, resp.Code, resp.Success(), resp.Data != nil && deref(resp.Data.ReactionId) != "")
}

func (p *Provider) messageReactions(ctx context.Context, messageID string) ([]*larkim.MessageReaction, bool, error) {
	var items []*larkim.MessageReaction
	page := ""
	seen := map[string]bool{"": true}
	for range 10 {
		req := larkim.NewListMessageReactionReqBuilder().MessageId(messageID).PageSize(50).PageToken(page).Build()
		resp, err := p.client.Im.MessageReaction.List(ctx, req)
		if err != nil {
			return nil, false, transportFailure(err)
		}
		if resp == nil || resp.ApiResp == nil {
			return nil, false, failure{"feishu reaction response missing", false, true, "response_missing"}
		}
		if reactionGone(resp.Code) {
			return nil, true, nil
		}
		if err := responseFailure(resp.StatusCode, resp.Code, resp.Success(), resp.Data != nil); err != nil {
			return nil, false, err
		}
		items = append(items, resp.Data.Items...)
		if resp.Data.HasMore == nil || !*resp.Data.HasMore {
			return items, false, nil
		}
		page = deref(resp.Data.PageToken)
		if page == "" || seen[page] {
			break
		}
		seen[page] = true
	}
	return nil, false, failure{"feishu reaction page limit", false, false, "reaction_page_limit"}
}
