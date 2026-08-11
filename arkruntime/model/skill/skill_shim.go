// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package skill

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/url"

	"github.com/volcengine/ark-runtime-go/arkruntime/model"
)

// SkillResponse wraps Skill so it satisfies model.Response.
type SkillResponse struct {
	Skill
	model.HttpHeader
}

// UploadForm pairs the multipart metadata (display_title) with the binary
// zip file part. The typespec-generated CreateSkillRequest describes the
// wire fields; the binary `files` part is appended here at multipart build
// time (matches the server-side `files` / `files[]` field name).
type UploadForm struct {
	// File is the zip content to upload. Required.
	File io.Reader

	// FileName is the multipart filename attribute (visible to server).
	// Optional; defaults to "skill.zip" when empty.
	FileName string

	// DisplayTitle is the optional user-visible title for this skill.
	DisplayTitle string
}

// MarshalMultipart writes DisplayTitle as a form part and appends the
// zip File as the `files` binary part.
func (u *UploadForm) MarshalMultipart() (data []byte, contentType string, err error) {
	buf := bytes.NewBuffer(nil)
	writer := multipart.NewWriter(buf)

	if u.DisplayTitle != "" {
		if err = writer.WriteField("display_title", u.DisplayTitle); err != nil {
			_ = writer.Close()
			return nil, "", err
		}
	}

	if u.File == nil {
		_ = writer.Close()
		return nil, "", errNilFile
	}
	name := u.FileName
	if name == "" {
		name = "skill.zip"
	}
	part, perr := writer.CreateFormFile("files", name)
	if perr != nil {
		_ = writer.Close()
		return nil, "", perr
	}
	if _, err = io.Copy(part, u.File); err != nil {
		_ = writer.Close()
		return nil, "", err
	}

	if err = writer.Close(); err != nil {
		return nil, "", err
	}
	return buf.Bytes(), writer.FormDataContentType(), nil
}

// PathEscape hides url.PathEscape at the shim boundary.
func PathEscape(s string) string { return url.PathEscape(s) }

// errNilFile is a sentinel — kept private so callers use SkillClient.CreateSkill's
// own errors.New wrapping (with the same textual message).
var errNilFile = errNilFileValue{}

type errNilFileValue struct{}

func (errNilFileValue) Error() string { return "missing required file part" }
