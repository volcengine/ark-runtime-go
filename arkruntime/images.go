// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"errors"
	"net/http"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/images"
	"github.com/volcengine/ark-runtime-go/arkruntime/utils"
)

// GenerateImages calls POST /images/generations (non-streaming) and returns
// the generated images. For streaming partial-image events, see
// GenerateImagesStreaming.
func (c *Client) GenerateImages(
	ctx context.Context,
	request *images.CreateImageGenerationRequest,
	setters ...requestOption,
) (res *images.ImageGenerationResponseWithHeader, err error) {
	if request == nil {
		err = errors.New("missing required request body")
		return
	}
	request.SetStream(images.NewOptBool(false))
	path := "/images/generations"
	setters = append(setters, withBody(request))
	res = &images.ImageGenerationResponseWithHeader{}
	err = c.Do(ctx, http.MethodPost, c.fullURL(path), resourceTypeEndpoint, request.Model, res, setters...)
	return
}

// GenerateImagesStreaming calls POST /images/generations with stream=true
// and returns a reader yielding typed ImageGenerationStreamEvent frames.
// Caller is responsible for closing the returned reader.
func (c *Client) GenerateImagesStreaming(
	ctx context.Context,
	request *images.CreateImageGenerationRequest,
	setters ...requestOption,
) (stream *utils.ImageGenerationStreamReader, err error) {
	if request == nil {
		err = errors.New("missing required request body")
		return
	}
	// Force stream=true regardless of what the caller set on the request.
	request.SetStream(images.NewOptBool(true))
	path := "/images/generations"
	setters = append(setters, withBody(request))
	stream, err = c.ImageGenerationStreamRequestDo(ctx, http.MethodPost, c.fullURL(path), request.Model, setters...)
	return
}
