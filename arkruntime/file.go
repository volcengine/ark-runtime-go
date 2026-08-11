// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/file"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/responses"
)

const (
	filePrefix   = "/files"
	fileScheme   = "file"
	pollInterval = 3 * time.Second
	maxWaitTime  = 10 * time.Minute
)

// WaitForFileProcessingOptions configures WaitForFileProcessing behavior.
type WaitForFileProcessingOptions struct {
	// PollInterval between retrievals. Defaults to 3s when zero.
	PollInterval time.Duration
	// MaxWait deadline. Defaults to 10m when zero.
	MaxWait time.Duration
}

func (c *Client) UploadFile(ctx context.Context, request *file.FileCreateRequest, fileReader io.Reader, setters ...requestOption) (response *file.FileObject, err error) {
	form := &file.UploadForm{File: fileReader, Request: *request}
	body, contentType, merr := form.MarshalMultipart()
	if merr != nil {
		return nil, merr
	}
	opts := append(setters,
		withBody(bytes.NewReader(body)),
		withContentType(contentType),
	)
	wrap := &file.FileObjectResponse{}
	err = c.Do(ctx, http.MethodPost, c.fullURL(filePrefix), "", "", wrap, opts...)
	if err != nil {
		return nil, err
	}
	response = &wrap.FileObject
	return
}

func (c *Client) RetrieveFile(ctx context.Context, fileID string, setters ...requestOption) (response *file.FileObject, err error) {
	if fileID == "" {
		err = errors.New("missing required file_id parameter")
		return
	}
	opts := append(setters, withBody(nil))
	wrap := &file.FileObjectResponse{}
	err = c.Do(ctx, http.MethodGet, c.fullURL(filePrefix+"/"+fileID), "", "", wrap, opts...)
	if err != nil {
		return nil, err
	}
	response = &wrap.FileObject
	return
}

func (c *Client) ListFiles(ctx context.Context, request *file.FilesListParams, setters ...requestOption) (response *file.FileListResponse, err error) {
	q, qerr := file.URLQuery(request)
	if qerr != nil {
		return nil, qerr
	}
	u := c.fullURL(filePrefix)
	if encoded := q.Encode(); encoded != "" {
		u = u + "?" + encoded
	}
	opts := append(setters, withBody(nil))
	wrap := &file.FileListResponseWrapper{}
	err = c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...)
	if err != nil {
		return nil, err
	}
	response = &wrap.FileListResponse
	return
}

func (c *Client) DeleteFile(ctx context.Context, fileID string, setters ...requestOption) (response *file.FileDeleted, err error) {
	if fileID == "" {
		err = errors.New("missing required file_id parameter")
		return
	}
	opts := append(setters, withBody(nil))
	wrap := &file.FileDeletedResponse{}
	err = c.Do(ctx, http.MethodDelete, c.fullURL(filePrefix+"/"+fileID), "", "", wrap, opts...)
	if err != nil {
		return nil, err
	}
	response = &wrap.FileDeleted
	return
}

// WaitForFileProcessing polls RetrieveFile until status is terminal
// (active|failed) or the deadline elapses.
func (c *Client) WaitForFileProcessing(ctx context.Context, fileID string, opts WaitForFileProcessingOptions, setters ...requestOption) (*file.FileObject, error) {
	pi := opts.PollInterval
	if pi == 0 {
		pi = pollInterval
	}
	mw := opts.MaxWait
	if mw == 0 {
		mw = maxWaitTime
	}
	deadline := time.Now().Add(mw)

	fm, err := c.RetrieveFile(ctx, fileID, setters...)
	if err != nil {
		return nil, err
	}
	for fm.Status == file.StatusProcessing {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("wait_for_processing: file %s did not finish within %s", fileID, mw)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(pi):
		}
		fm, err = c.RetrieveFile(ctx, fileID, setters...)
		if err != nil {
			return nil, err
		}
	}
	return fm, nil
}

func (c *Client) preprocessResponseInput(ctx context.Context, input *responses.ResponsesInput) error {
	return c.preprocessResponseMultiModal(ctx, input)
}

func (c *Client) preprocessResponseMultiModal(ctx context.Context, input *responses.ResponsesInput) error {
	items, ok := input.GetInputItemArray()
	if !ok || len(items) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, maxWaitTime)
	defer cancel()

	type msgSlot struct {
		itemIdx  int
		message  responses.ItemEasyMessage
		contents []responses.ContentItem
	}
	var slots []*msgSlot
	wg := sync.WaitGroup{}
	for i := range items {
		msg, ok := items[i].OneOf.GetItemEasyMessage()
		if !ok {
			continue
		}
		contentItems, ok := msg.Content.GetContentItemArray()
		if !ok {
			continue
		}
		slot := &msgSlot{itemIdx: i, message: msg, contents: contentItems}
		slots = append(slots, slot)
		for j := range slot.contents {
			contentItem := &slot.contents[j]
			if multiModalFile, err := getMultiModalFile(contentItem); err == nil && multiModalFile != nil {
				wg.Add(1)
				go func(contentItem *responses.ContentItem, multiModalFile io.ReadCloser) {
					defer wg.Done()
					defer multiModalFile.Close() //nolint:errcheck // best-effort close in goroutine
					if err := c.preprocessResponseFile(ctx, contentItem, multiModalFile); err != nil {
						cancel()
					}
				}(contentItem, multiModalFile)
			}
		}
	}
	wg.Wait()
	for _, slot := range slots {
		slot.message.Content.SetContentItemArray(slot.contents)
		items[slot.itemIdx].OneOf.SetItemEasyMessage(slot.message)
	}
	input.SetInputItemArray(items)
	return nil
}

func (c *Client) preprocessResponseFile(ctx context.Context, contentItem *responses.ContentItem, fileReader io.Reader) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("preprocessResponseContentItem recovered from panic: %v", r)
		}
	}()
	fileMeta, err := c.UploadFile(ctx, &file.FileCreateRequest{Purpose: file.PurposeUserData}, fileReader)
	if err != nil {
		return err
	}
	fileMeta, err = c.WaitForFileProcessing(ctx, fileMeta.ID, WaitForFileProcessingOptions{})
	if err != nil {
		return err
	}

	if video, ok := contentItem.OneOf.GetContentItemVideo(); ok && len(video.VideoURL) > 0 {
		video.VideoURL = ""
		video.FileID = responses.NewOptString(fileMeta.ID)
		contentItem.OneOf.SetContentItemVideo(video)
	} else if img, ok := contentItem.OneOf.GetContentItemImage(); ok && img.ImageURL.IsSet() && len(img.ImageURL.Value) > 0 {
		img.ImageURL.Reset()
		img.FileID = responses.NewOptString(fileMeta.ID)
		contentItem.OneOf.SetContentItemImage(img)
	} else if f, ok := contentItem.OneOf.GetContentItemFile(); ok && f.FileURL.IsSet() && len(f.FileURL.Value) > 0 {
		f.FileURL.Reset()
		f.FileID = responses.NewOptString(fileMeta.ID)
		contentItem.OneOf.SetContentItemFile(f)
	}
	return nil
}

// getMultiModalFile returns an io.ReadCloser (or nil if the content item has
// no file:// URL). Callers MUST defer Close on a non-nil result; otherwise the
// underlying *os.File handle leaks until GC finalizes it.
func getMultiModalFile(contentItem *responses.ContentItem) (io.ReadCloser, error) {
	if video, ok := contentItem.OneOf.GetContentItemVideo(); ok && len(video.VideoURL) > 0 {
		return parseFile(video.VideoURL)
	} else if img, ok := contentItem.OneOf.GetContentItemImage(); ok && img.ImageURL.IsSet() && len(img.ImageURL.Value) > 0 {
		return parseFile(img.ImageURL.Value)
	} else if f, ok := contentItem.OneOf.GetContentItemFile(); ok && f.FileURL.IsSet() && len(f.FileURL.Value) > 0 {
		return parseFile(f.FileURL.Value)
	}
	return nil, nil
}

func parseFile(rawURL string) (io.ReadCloser, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != fileScheme {
		return nil, nil
	}
	var path string
	if u.Host != "" && u.Host != "localhost" {
		path = fmt.Sprintf(`%s%s`, u.Host, u.Path)
	} else {
		path = u.Path
	}
	if runtime.GOOS == "windows" {
		if len(path) >= 3 && strings.HasPrefix(path, "/") && path[2] == ':' {
			path = path[1:]
		}
	}
	return os.Open(path)
}
