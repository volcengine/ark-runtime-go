// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"time"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	cardkit "github.com/larksuite/oapi-sdk-go/v3/service/cardkit/v1"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// CardKitLimiter 由宿主共享，按60ms间隔保守满足1000次/分钟和50次/秒。
type CardKitLimiter struct{ limiter COTLimiter }

func (l *CardKitLimiter) wait(ctx context.Context) error {
	return l.limiter.wait(ctx, 60*time.Millisecond)
}

const finalAnswerElement = "final_answer"

func streamPrintStep(length int) int { return max(1, (length+39)/40) }

func (p *Provider) createTextStream(ctx context.Context, text string, length int) (string, error) {
	if err := p.cardKitLimiter.wait(ctx); err != nil {
		return "", err
	}
	card := map[string]any{
		"schema": "2.0",
		"config": map[string]any{"streaming_mode": true, "summary": map[string]string{"content": "回复中…"},
			"streaming_config": map[string]any{"print_frequency_ms": map[string]int{"default": 50}, "print_step": map[string]int{"default": streamPrintStep(length)}, "print_strategy": "fast"}},
		"body": map[string]any{"elements": []map[string]any{{"tag": "markdown", "element_id": finalAnswerElement, "content": text}}},
	}
	raw, err := json.Marshal(card)
	if err != nil {
		return "", failure{"invalid result card", false, false, "invalid_stream_card"}
	}
	resp, err := p.client.Cardkit.V1.Card.Create(ctx, cardkit.NewCreateCardReqBuilder().Body(cardkit.NewCreateCardReqBodyBuilder().Type("card_json").Data(string(raw)).Build()).Build())
	if err != nil {
		return "", transportFailure(err)
	}
	if resp == nil || resp.ApiResp == nil {
		return "", failure{"result card response missing", true, false, "response_missing"}
	}
	valid := resp.Data != nil && resp.Data.CardId != nil && validCOTID(*resp.Data.CardId) && len(*resp.Data.CardId) <= 20
	if err := cardKitFailure(resp.ApiResp, resp.Code, valid); err != nil {
		return "", err
	}
	return *resp.Data.CardId, nil
}

func (p *Provider) writeTextStream(ctx context.Context, cardID, text string, sequence uint64, uuid string) error {
	if err := p.cardKitLimiter.wait(ctx); err != nil {
		return err
	}
	body := cardkit.NewContentCardElementReqBodyBuilder().Content(text).Sequence(int(sequence)).Uuid(uuid).Build()
	resp, err := p.client.Cardkit.V1.CardElement.Content(ctx, cardkit.NewContentCardElementReqBuilder().CardId(cardID).ElementId(finalAnswerElement).Body(body).Build())
	if err != nil {
		return transportFailure(err)
	}
	if resp == nil || resp.ApiResp == nil {
		return failure{"result content response missing", true, false, "response_missing"}
	}
	return cardKitFailure(resp.ApiResp, resp.Code, true)
}

func (p *Provider) configureTextStream(ctx context.Context, state core.TextStreamState, uuid string, open bool, summary string) error {
	if err := p.cardKitLimiter.wait(ctx); err != nil {
		return err
	}
	config := map[string]any{"streaming_mode": open}
	if !open {
		config["summary"] = map[string]string{"content": progressText(summary, 50)}
	}
	raw, _ := json.Marshal(map[string]any{"config": config})
	body := cardkit.NewSettingsCardReqBodyBuilder().Settings(string(raw)).Sequence(int(state.Sequence)).Uuid(uuid).Build()
	resp, err := p.client.Cardkit.V1.Card.Settings(ctx, cardkit.NewSettingsCardReqBuilder().CardId(state.Handle).Body(body).Build())
	if err != nil {
		return transportFailure(err)
	}
	if resp == nil || resp.ApiResp == nil {
		return failure{"result settings response missing", true, false, "response_missing"}
	}
	return cardKitFailure(resp.ApiResp, resp.Code, true)
}

func cardKitFailure(resp *larkcore.ApiResp, code int, valid bool) error {
	var result struct {
		Code *int `json:"code"`
	}
	if resp == nil || json.Unmarshal(resp.RawBody, &result) != nil || result.Code == nil {
		return failure{"cardkit result unknown", true, false, "response_missing"}
	}
	switch code {
	case 200570:
		return failure{"cardkit image rejected", false, false, "invalid_image"}
	case 200850, 300309:
		return failure{"cardkit stream closed", false, true, "stream_closed"}
	case 300120, 200810:
		// CardKit 的内部错误和交互占用可在保留卡片、递增 sequence 后重试。
		return failure{"cardkit temporarily unavailable", false, true, "cardkit_unavailable"}
	}
	return responseFailure(resp.StatusCode, code, code == 0, valid)
}
