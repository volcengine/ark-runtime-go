// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Video input example: uploads a local video file via the Files API,
// waits for processing to finish, then asks the model to describe each
// second of the video. Round 2 chains a follow-up question via
// PreviousResponseID so the model can reason over the same video without
// re-uploading.
//
// Run with:
//
//	ARK_API_KEY=... go run ./examples/responses/video
//
// Expects ./ark_vlm_video_input.mp4 in the working directory.
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/file"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/responses"
)

const modelName = "seed-2-0-lite-260428"

func main() {
	client := arkruntime.NewByteplusClientWithApiKey(os.Getenv("ARK_API_KEY"))
	ctx := context.Background()

	fileID, err := uploadAndAwait(ctx, client, "ark_vlm_video_input.mp4")
	if err != nil {
		fmt.Printf("upload error: %v\n", err)
		return
	}

	// Round 1: video + text prompt asking for a per-second description.
	round1Msg := responses.ItemEasyMessage{
		Role: responses.NewOptMessageRole(responses.MessageRoleUser),
		Content: responses.NewContentItemArrayMessageContent([]responses.ContentItem{
			{
				OneOf: responses.NewContentItemVideoContentItemSum(responses.ContentItemVideo{
					Type:   responses.ContentItemVideoTypeInputVideo,
					FileID: responses.NewOptString(fileID),
				}),
			},
			{
				OneOf: responses.NewContentItemTextContentItemSum(responses.ContentItemText{
					Type: responses.ContentItemTextTypeInputText,
					Text: "请逐帧给出视频中每一秒的描述",
				}),
			},
		}),
	}
	req := &responses.ResponsesRequest{
		Model: modelName,
		Input: responses.NewInputItemArrayResponsesInput([]responses.InputItem{
			{OneOf: responses.NewItemEasyMessageInputItemSum(round1Msg)},
		}),
	}

	fmt.Println("----- round 1: per-second description -----")
	responseID, err := streamAndCaptureID(ctx, client, req)
	if err != nil {
		fmt.Printf("round 1 stream error: %v\n", err)
		return
	}

	// Round 2: chain via PreviousResponseID. No need to re-send the video.
	fmt.Println("\n----- round 2: ask a follow-up about the same video -----")
	req.Input = responses.NewStringResponsesInput("上述对话中有什么？")
	req.PreviousResponseID = responses.NewOptString(responseID)
	if _, err := streamAndCaptureID(ctx, client, req); err != nil {
		fmt.Printf("round 2 stream error: %v\n", err)
		return
	}
	fmt.Println()
}

// uploadAndAwait uploads a local file and polls until processing finishes.
func uploadAndAwait(ctx context.Context, client *arkruntime.Client, path string) (string, error) {
	data, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", path, err)
	}
	defer data.Close()

	fmt.Println("----- upload video -----")
	meta, err := client.UploadFile(ctx, &file.FileCreateRequest{
		Purpose: file.PurposeUserData,
	}, data)
	if err != nil {
		return "", fmt.Errorf("upload: %w", err)
	}

	ready, err := client.WaitForFileProcessing(ctx, meta.ID, arkruntime.WaitForFileProcessingOptions{})
	if err != nil {
		return "", fmt.Errorf("wait %s: %w", meta.ID, err)
	}
	fmt.Printf("video ready: id=%s status=%s\n", ready.ID, ready.Status)
	return ready.ID, nil
}

// streamAndCaptureID prints reasoning + text deltas and returns the
// response_id from the first ResponseCreated event so the caller can chain
// a follow-up turn via PreviousResponseID.
func streamAndCaptureID(
	ctx context.Context,
	client *arkruntime.Client,
	req *responses.ResponsesRequest,
) (string, error) {
	stream, err := client.CreateResponsesStream(ctx, req)
	if err != nil {
		return "", err
	}
	var responseID string
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return responseID, nil
		}
		if err != nil {
			return responseID, err
		}
		switch event.OneOf.Type {
		case responses.ResponseCreatedEventResponseStreamEventSum:
			responseID = event.OneOf.ResponseCreatedEvent.Response.ID
		case responses.ResponseReasoningSummaryTextDeltaEventResponseStreamEventSum:
			fmt.Print(event.OneOf.ResponseReasoningSummaryTextDeltaEvent.Delta.Or(""))
		case responses.ResponseTextDeltaEventResponseStreamEventSum:
			fmt.Print(event.OneOf.ResponseTextDeltaEvent.Delta.Or(""))
		case responses.ResponseTextDoneEventResponseStreamEventSum:
			fmt.Printf("\n[text done] %s\n", event.OneOf.ResponseTextDoneEvent.Text.Or(""))
		}
	}
}
