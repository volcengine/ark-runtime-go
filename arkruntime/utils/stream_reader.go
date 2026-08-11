// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"

	"github.com/volcengine/ark-runtime-go/arkruntime/model"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/chat"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/images"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/responses"
)

var (
	headerData  = []byte("data:")
	errorPrefix = []byte(`{"error":`)
)

type ChatCompletionStreamReader struct {
	EmptyMessagesLimit uint
	IsFinished         bool

	Reader         *bufio.Reader
	Response       *http.Response
	ErrAccumulator ErrorAccumulator
	Unmarshaler    Unmarshaler

	model.HttpHeader
}

type ResponsesStreamReader struct {
	ChatCompletionStreamReader
	Decoder *EventStreamDecoder
}

func (stream *ResponsesStreamReader) Recv() (response *responses.ResponseStreamEvent, err error) {
	if stream.IsFinished {
		err = io.EOF
		return
	}

	response, err = stream.processLines()
	return
}

func (stream *ResponsesStreamReader) processLines() (*responses.ResponseStreamEvent, error) {
	var (
		emptyMessagesCount uint
	)

	for stream.Decoder.Next() {

		// trimedLine is trimed with header and followed space (if exists)
		if bytes.HasPrefix(stream.Decoder.Event().Data, errorPrefix) {
			writeErr := stream.ErrAccumulator.Write(stream.Decoder.Event().Data)
			if writeErr != nil {
				return nil, writeErr
			}
			emptyMessagesCount++
			if emptyMessagesCount > stream.EmptyMessagesLimit {
				return nil, model.ErrTooManyEmptyStreamMessages
			}
			if respErr := stream.unmarshalError(); respErr != nil {
				return nil, fmt.Errorf("error, %w", respErr.Error)
			}
			continue
		}

		if bytes.HasPrefix(stream.Decoder.Event().Data, []byte("[DONE]")) {
			// In this case we don't break because we still want to iterate through the full stream.
			stream.IsFinished = true
			return nil, io.EOF
		}

		response := &responses.ResponseStreamEvent{}
		unmarshalErr := stream.Unmarshaler.Unmarshal(stream.Decoder.Event().Data, response)
		if unmarshalErr != nil {
			return nil, unmarshalErr
		}

		return response, nil
	}
	return nil, stream.Decoder.Err()
}

// ChatGenStreamReader streams ark-apis generated *chat.ChatCompletionStreamResponse
// chunks. Reuses the byte-reading machinery from ChatCompletionStreamReader
// (embedded) and overrides Recv / processLines to unmarshal into the gen type.
type ChatGenStreamReader struct {
	ChatCompletionStreamReader
}

func (stream *ChatGenStreamReader) Recv() (response *chat.ChatCompletionStreamResponse, err error) {
	if stream.IsFinished {
		err = io.EOF
		return
	}

	response, err = stream.processLines()
	return
}

func (stream *ChatGenStreamReader) processLines() (*chat.ChatCompletionStreamResponse, error) {
	var (
		emptyMessagesCount uint
		hasErrorPrefix     bool
	)

	for {
		rawLine, readErr := stream.Reader.ReadBytes('\n')
		if readErr != nil || hasErrorPrefix {
			respErr := stream.unmarshalError()
			if respErr != nil {
				return nil, respErr.Error
			}
			return nil, readErr
		}

		noSpaceLine := bytes.TrimSpace(rawLine)
		trimedLine := bytes.TrimSpace(bytes.TrimPrefix(noSpaceLine, headerData))
		if bytes.HasPrefix(trimedLine, errorPrefix) {
			hasErrorPrefix = true
		}
		if !bytes.HasPrefix(noSpaceLine, headerData) || hasErrorPrefix {
			if hasErrorPrefix {
				noSpaceLine = bytes.TrimPrefix(noSpaceLine, headerData)
			}
			writeErr := stream.ErrAccumulator.Write(noSpaceLine)
			if writeErr != nil {
				return nil, writeErr
			}
			emptyMessagesCount++
			if emptyMessagesCount > stream.EmptyMessagesLimit {
				return nil, model.ErrTooManyEmptyStreamMessages
			}
			continue
		}

		if string(trimedLine) == "[DONE]" {
			stream.IsFinished = true
			return nil, io.EOF
		}

		response := &chat.ChatCompletionStreamResponse{}
		unmarshalErr := stream.Unmarshaler.Unmarshal(trimedLine, response)
		if unmarshalErr != nil {
			return nil, unmarshalErr
		}
		return response, nil
	}
}

func (stream *ChatCompletionStreamReader) unmarshalError() (errResp *model.ErrorResponse) {
	errBytes := stream.ErrAccumulator.Bytes()
	if len(errBytes) == 0 {
		return
	}

	err := stream.Unmarshaler.Unmarshal(errBytes, &errResp)
	if err != nil {
		errResp = nil
	}

	if errResp != nil && errResp.Error != nil {
		if stream.Header() != nil {
			errResp.Error.RequestId = stream.Header().Get(model.ClientRequestHeader)
		}
	}

	return
}

func (stream *ChatCompletionStreamReader) Close() error {
	return stream.Response.Body.Close()
}

// ImageGenerationStreamReader streams ark-apis generated
// *images.ImageGenerationStreamEvent frames. Each SSE frame on
// /images/generations is `event: <type>\ndata: <json>\n\n` — we ignore
// the `event:` line (the JSON in `data:` already carries `type`) and
// unmarshal the `data:` payload into the gen type.
type ImageGenerationStreamReader struct {
	ChatCompletionStreamReader
}

func (stream *ImageGenerationStreamReader) Recv() (response *images.ImageGenerationStreamEvent, err error) {
	if stream.IsFinished {
		err = io.EOF
		return
	}

	response, err = stream.processLines()
	return
}

func (stream *ImageGenerationStreamReader) processLines() (*images.ImageGenerationStreamEvent, error) {
	var (
		emptyMessagesCount uint
		hasErrorPrefix     bool
	)

	for {
		rawLine, readErr := stream.Reader.ReadBytes('\n')
		if readErr != nil || hasErrorPrefix {
			respErr := stream.unmarshalError()
			if respErr != nil {
				return nil, respErr.Error
			}
			return nil, readErr
		}

		noSpaceLine := bytes.TrimSpace(rawLine)
		trimedLine := bytes.TrimSpace(bytes.TrimPrefix(noSpaceLine, headerData))
		if bytes.HasPrefix(trimedLine, errorPrefix) {
			hasErrorPrefix = true
		}
		if !bytes.HasPrefix(noSpaceLine, headerData) || hasErrorPrefix {
			if hasErrorPrefix {
				noSpaceLine = bytes.TrimPrefix(noSpaceLine, headerData)
			}
			writeErr := stream.ErrAccumulator.Write(noSpaceLine)
			if writeErr != nil {
				return nil, writeErr
			}
			emptyMessagesCount++
			if emptyMessagesCount > stream.EmptyMessagesLimit {
				return nil, model.ErrTooManyEmptyStreamMessages
			}
			continue
		}

		if string(trimedLine) == "[DONE]" {
			stream.IsFinished = true
			return nil, io.EOF
		}

		response := &images.ImageGenerationStreamEvent{}
		unmarshalErr := stream.Unmarshaler.Unmarshal(trimedLine, response)
		if unmarshalErr != nil {
			return nil, unmarshalErr
		}
		return response, nil
	}
}
