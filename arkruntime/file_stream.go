// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"
	"sync"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/file"
	"github.com/volcengine/ark-runtime-go/arkruntime/pkg/apiform"
)

// FileUploadOptions controls the binary part of a streaming upload. A zero
// MaxBytes leaves the size policy to the source and server. The upload method takes ownership of its source.
type FileUploadOptions struct {
	FileName    string
	ContentType string
	MaxBytes    int64
}

// ErrUploadTooLarge reports that a streaming source exceeded its configured limit.
var ErrUploadTooLarge = errors.New("file upload exceeds limit")

// fileUploadForm keeps multipart metadata separate from file content.
type fileUploadForm struct {
	prefix, suffix []byte
	contentType    string
	maxBytes       int64
}

// ContentType includes the frozen multipart boundary.
func (f *fileUploadForm) ContentType() string { return f.contentType }

// Body creates a new stream over the frozen metadata and a caller-owned source.
func (f *fileUploadForm) Body(source io.Reader) (io.ReadCloser, error) {
	if source == nil {
		return nil, errors.New("invalid streaming upload")
	}
	if f.maxBytes > 0 {
		source = &uploadLimitReader{source: source, left: f.maxBytes}
	}
	return io.NopCloser(io.MultiReader(bytes.NewReader(f.prefix), source, bytes.NewReader(f.suffix))), nil
}

// newFileUploadForm freezes metadata without opening or reading the file source.
func newFileUploadForm(request *file.FileCreateRequest, options FileUploadOptions) (*fileUploadForm, error) {
	if request == nil || options.MaxBytes < 0 {
		return nil, errors.New("invalid streaming upload")
	}
	name := options.FileName
	if name == "" {
		name = "file"
	}
	if strings.ContainsAny(name, "\r\n\x00") {
		return nil, errors.New("invalid upload filename")
	}
	contentType := options.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	if strings.IndexFunc(contentType, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
		return nil, errors.New("invalid upload content type")
	}
	if _, _, err := mime.ParseMediaType(contentType); err != nil {
		return nil, errors.New("invalid upload content type")
	}
	var metadata bytes.Buffer
	writer := multipart.NewWriter(&metadata)
	if err := apiform.Marshal(request, writer); err != nil {
		return nil, err
	}
	header := textproto.MIMEHeader{}
	header.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": "file", "filename": name}))
	header.Set("Content-Type", contentType)
	if _, err := writer.CreatePart(header); err != nil {
		return nil, err
	}
	prefix := append([]byte(nil), metadata.Bytes()...)
	metadata.Reset()
	if err := writer.Close(); err != nil {
		return nil, err
	}
	suffix := append([]byte(nil), metadata.Bytes()...)
	return &fileUploadForm{prefix: prefix, suffix: suffix, contentType: writer.FormDataContentType(), maxBytes: options.MaxBytes}, nil
}

type uploadLimitReader struct {
	source io.Reader
	left   int64
}

func (r *uploadLimitReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.left == 0 {
		var probe [1]byte
		n, err := r.source.Read(probe[:])
		if n > 0 {
			return 0, ErrUploadTooLarge
		}
		return 0, err
	}
	if int64(len(p)) > r.left {
		p = p[:r.left]
	}
	n, err := r.source.Read(p)
	r.left -= int64(n)
	return n, err
}

// UploadFileStream uploads without buffering file content. It uses the configured
// HTTP client and takes ownership of source, closing it exactly once
// on completion, failure or cancellation. source.Close must promptly unblock Read;
// use an independently cancellable reader for asynchronous producers.
func (c *Client) UploadFileStream(ctx context.Context, request *file.FileCreateRequest, source io.ReadCloser, options FileUploadOptions, setters ...requestOption) (*file.FileObject, error) {
	form, err := newFileUploadForm(request, options)
	if err != nil {
		if source != nil {
			_ = source.Close()
		}
		return nil, err
	}
	return c.uploadFileForm(ctx, form, source, setters...)
}

// uploadFileForm sends multipart metadata and a source through the SDK decoder.
func (c *Client) uploadFileForm(ctx context.Context, form *fileUploadForm, source io.ReadCloser, setters ...requestOption) (*file.FileObject, error) {
	if source == nil {
		return nil, errors.New("invalid streaming upload")
	}
	owned := &streamingUploadBody{source: source}
	defer func() { _ = owned.Close() }()
	if ctx == nil || form == nil {
		return nil, errors.New("upload context required")
	}
	body, err := form.Body(source)
	if err != nil {
		return nil, err
	}
	owned.reader = body
	req, requestErr := c.newRequest(ctx, http.MethodPost, c.fullURL(filePrefix), "", "", append(setters, withBody(owned), withContentType(form.ContentType()))...)
	if requestErr != nil {
		return nil, requestErr
	}
	req.Header.Set("Accept", "application/json")
	stop, exited := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(exited)
		select {
		case <-ctx.Done():
			_ = owned.Close()
		case <-stop:
		}
	}()
	defer func() { close(stop); <-exited }()
	result := &file.FileObjectResponse{}
	if err := c.sendRequest(c.config.HTTPClient, req, result); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	return &result.FileObject, nil
}

// streamingUploadBody allows net/http and the cancellation watcher to release
// the producer concurrently without closing a caller-supplied source twice.
type streamingUploadBody struct {
	reader io.Reader
	source io.ReadCloser
	once   sync.Once
	err    error
}

func (b *streamingUploadBody) Read(p []byte) (int, error) { return b.reader.Read(p) }
func (b *streamingUploadBody) Close() error {
	b.once.Do(func() { b.err = b.source.Close() })
	return b.err
}
