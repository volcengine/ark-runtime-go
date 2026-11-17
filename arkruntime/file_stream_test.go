// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/file"
)

type streamTestTransport func(*http.Request) (*http.Response, error)

func (f streamTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type observedUploadReader struct {
	started *bool
	read    bool
}

func (r *observedUploadReader) Read(p []byte) (int, error) {
	if !*r.started {
		return 0, errors.New("file read before transport started")
	}
	if r.read {
		return 0, io.EOF
	}
	r.read = true
	return copy(p, "payload"), io.EOF
}

func TestUploadFileStreamReadsOnlyWhenTransportStarts(t *testing.T) {
	started := false
	source := &observedUploadReader{started: &started}
	sentinel := errors.New("fixture transport complete")
	transport := streamTestTransport(func(req *http.Request) (*http.Response, error) {
		started = true
		if req.GetBody != nil {
			t.Fatal("streaming writes must not be automatically replayable")
		}
		form, err := req.MultipartReader()
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for {
			part, err := form.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(part)
			if err != nil {
				t.Fatal(err)
			}
			if part.FormName() == "file" {
				found = true
				if part.FileName() != "photo.png" || string(data) != "payload" || part.Header.Get("Content-Type") != "image/png" {
					t.Fatalf("invalid file part: %s %s %q", part.FileName(), part.Header.Get("Content-Type"), data)
				}
			}
		}
		if !found {
			t.Fatal("missing file")
		}
		return nil, sentinel
	})
	c := NewClientWithApiKey("fixture", WithHTTPClient(&http.Client{Transport: transport}), WithRetryTimes(0))
	_, err := c.UploadFileStream(context.Background(), &file.FileCreateRequest{Purpose: file.PurposeUserData}, source, FileUploadOptions{FileName: "photo.png", ContentType: "image/png"})
	if err == nil || !strings.Contains(err.Error(), sentinel.Error()) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStreamingUploadEnforcesExactFileLimit(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		wantError  bool
	}{{"at limit", "12345", false}, {"over limit", "123456", true}} {
		t.Run(tc.name, func(t *testing.T) {
			form, err := newFileUploadForm(&file.FileCreateRequest{Purpose: file.PurposeUserData}, FileUploadOptions{MaxBytes: 5})
			if err != nil {
				t.Fatal(err)
			}
			body, err := form.Body(strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			defer body.Close()
			_, err = io.ReadAll(body)
			if errors.Is(err, ErrUploadTooLarge) != tc.wantError {
				t.Fatalf("unexpected limit result: %v", err)
			}
		})
	}
}

func (r *observedUploadReader) Close() error { return nil }

type closeCountSource struct {
	io.ReadCloser
	closes atomic.Int32
}

func (s *closeCountSource) Close() error { s.closes.Add(1); return s.ReadCloser.Close() }

func TestUploadFileStreamCancellationUnblocksReadAndClosesOnce(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()
	source := &closeCountSource{ReadCloser: reader}
	c := NewClientWithApiKey("fixture", WithHTTPClient(&http.Client{Transport: streamTestTransport(func(r *http.Request) (*http.Response, error) {
		_, err := io.Copy(io.Discard, r.Body)
		if err == nil {
			err = errors.New("fixture transport complete")
		}
		return nil, err
	})}))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := c.UploadFileStream(ctx, &file.FileCreateRequest{Purpose: file.PurposeUserData}, source, FileUploadOptions{})
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("cancellation lost: %v", err)
		}
	case <-time.After(2 * time.Second):
		_ = writer.Close()
		<-done
		t.Error("deadline did not interrupt source read")
	}
	if source.closes.Load() != 1 {
		t.Errorf("source closed %d times", source.closes.Load())
	}
}

const uploadFileResponse = `{"object":"file","id":"file-1","purpose":"user_data","filename":"file","created_at":0,"expire_at":-1,"status":"active"}`

func TestUploadFileStreamClosesSourceExactlyOnceOnSuccessAndFailure(t *testing.T) {
	for _, status := range []int{201, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			source := &closeCountSource{ReadCloser: io.NopCloser(strings.NewReader("payload"))}
			c := NewClientWithApiKey("fixture", WithHTTPClient(&http.Client{Transport: streamTestTransport(func(r *http.Request) (*http.Response, error) {
				_, _ = io.Copy(io.Discard, r.Body)
				_ = r.Body.Close()
				_ = r.Body.Close()
				body := uploadFileResponse
				if status == 503 {
					body = `{"error":{"code":"busy","message":"busy"}}`
				}
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			})}))
			_, err := c.UploadFileStream(context.Background(), &file.FileCreateRequest{Purpose: file.PurposeUserData}, source, FileUploadOptions{})
			if (err != nil) != (status != 201) {
				t.Errorf("unexpected status handling: %v", err)
			}
			if source.closes.Load() != 1 {
				t.Fatalf("source closed %d times", source.closes.Load())
			}
		})
	}
}

func TestMultipartRejectsControlCharactersInContentType(t *testing.T) {
	_, err := newFileUploadForm(&file.FileCreateRequest{Purpose: file.PurposeUserData}, FileUploadOptions{ContentType: "text/plain\r\n\r\n"})
	if err == nil {
		t.Fatal("accepted MIME header injection")
	}
}
