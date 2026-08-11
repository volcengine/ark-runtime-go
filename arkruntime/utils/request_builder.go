// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"bytes"
	"context"
	"io"
	"net/http"
)

type RequestBuilder interface {
	Build(ctx context.Context, method, url string, body interface{}, header http.Header) (*http.Request, error)
}

type HTTPRequestBuilder struct {
	marshaller Marshaller
}

func NewRequestBuilder() *HTTPRequestBuilder {
	return &HTTPRequestBuilder{
		marshaller: &JSONMarshaller{},
	}
}

func (b *HTTPRequestBuilder) Build(
	ctx context.Context,
	method string,
	url string,
	body interface{},
	header http.Header,
) (req *http.Request, err error) {
	var bodyReader io.Reader
	contentType := "application/json"

	if body != nil {
		if v, ok := body.(io.Reader); ok { // already marshalled
			bodyReader = v
		} else if v, ok := body.([]byte); ok { // already marshalled
			bodyReader = bytes.NewBuffer(v)
		} else { // json
			var reqBytes []byte
			reqBytes, err = b.marshaller.Marshal(body)
			if err != nil {
				return
			}
			bodyReader = bytes.NewBuffer(reqBytes)
		}
	}

	req, err = http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return
	}
	if header != nil {
		req.Header = header
	}
	// Only set Content-Type if the caller didn't already specify one
	// (e.g. UploadFile sets multipart/form-data via withContentType).
	if bodyReader != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", contentType)
	}

	return
}
