// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package envinit

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	selfhosted "github.com/volcengine/ark-runtime-go/arkruntime/selfhosted"
)

type fakeAPI struct {
	data []byte
}

func (f fakeAPI) PollWork(context.Context, selfhosted.PollWorkRequest) (*selfhosted.WorkItem, error) {
	return nil, nil
}
func (f fakeAPI) AckWork(context.Context, selfhosted.AckWorkRequest) error { return nil }
func (f fakeAPI) HeartbeatWork(context.Context, selfhosted.HeartbeatWorkRequest) (*selfhosted.HeartbeatResponse, error) {
	return &selfhosted.HeartbeatResponse{}, nil
}
func (f fakeAPI) StopWork(context.Context, selfhosted.StopWorkRequest) error { return nil }
func (f fakeAPI) GetSession(context.Context, selfhosted.GetSessionRequest) (*selfhosted.Session, error) {
	return nil, nil
}
func (f fakeAPI) ListEvents(context.Context, selfhosted.ListEventsRequest) (*selfhosted.ListEventsResponse, error) {
	return &selfhosted.ListEventsResponse{}, nil
}
func (f fakeAPI) SendEvent(context.Context, selfhosted.SendEventRequest) error { return nil }
func (f fakeAPI) OpenSkill(context.Context, selfhosted.OpenSkillRequest) (*selfhosted.SkillContent, error) {
	return &selfhosted.SkillContent{Body: io.NopCloser(bytes.NewReader(f.data)), ContentLength: int64(len(f.data))}, nil
}

type nilSkillAPI struct {
	fakeAPI
}

func (f nilSkillAPI) OpenSkill(context.Context, selfhosted.OpenSkillRequest) (*selfhosted.SkillContent, error) {
	return nil, nil
}

type resolvingAPI struct {
	fakeAPI
	resolved selfhosted.SkillRef
}

func (f resolvingAPI) ResolveSkill(context.Context, selfhosted.SkillRef) (selfhosted.SkillRef, error) {
	return f.resolved, nil
}

func TestSetupRejectsNilSession(t *testing.T) {
	init := New(fakeAPI{}, Options{Workdir: t.TempDir()})
	if err := init.Setup(context.Background(), nil); err == nil {
		t.Fatal("expected nil session error")
	}
}

func TestSetupSkipsEmptySkillContent(t *testing.T) {
	root := t.TempDir()
	init := New(nilSkillAPI{}, Options{Workdir: root})
	session := &selfhosted.Session{ID: "sess_1", Skills: []selfhosted.SkillRef{{Name: "demo", ID: "sk_1"}}}
	if err := init.Setup(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "skills", "demo")); !os.IsNotExist(err) {
		t.Fatalf("skill dir should not be installed, err=%v", err)
	}
}

func TestSetupInstallsZipSkill(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("SKILL.md")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	init := New(fakeAPI{data: buf.Bytes()}, Options{Workdir: root})
	session := &selfhosted.Session{ID: "sess_1", Skills: []selfhosted.SkillRef{{Name: "demo", ID: "sk_1"}}}
	if err := init.Setup(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "skills", "demo", "SKILL.md"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Fatalf("skill = %q", got)
	}
	retained := filepath.Join(root, "skills", "retained")
	if err := os.MkdirAll(retained, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := init.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "skills", "demo")); !os.IsNotExist(err) {
		t.Fatalf("installed skill should be removed, err=%v", err)
	}
	if _, err := os.Stat(retained); err != nil {
		t.Fatalf("unmanaged skills entry should remain: %v", err)
	}
}

func TestReplaceSkillDirRollsBackWhenCommitFails(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "demo")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatal(err)
	}
	oldPath := filepath.Join(target, "SKILL.md")
	if err := os.WriteFile(oldPath, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := replaceSkillDir(filepath.Join(root, "missing"), target); err == nil {
		t.Fatal("expected commit failure")
	}
	data, err := os.ReadFile(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "old" {
		t.Fatalf("old skill data=%q", data)
	}
}

func TestSetupInstallsSkillUnderResolvedMetadataName(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("SKILL.md")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	api := resolvingAPI{
		fakeAPI: fakeAPI{data: buf.Bytes()},
		resolved: selfhosted.SkillRef{
			Name:    "canonical-skill-name",
			SkillID: "skill-1",
			Type:    "custom",
			Version: "1",
		},
	}
	init := New(api, Options{Workdir: root})
	session := &selfhosted.Session{ID: "sess_1", Skills: []selfhosted.SkillRef{{SkillID: "skill-1", Type: "custom", Version: "1"}}}
	if err := init.Setup(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "skills", "canonical-skill-name", "SKILL.md")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "skills", "skill-1")); !os.IsNotExist(err) {
		t.Fatalf("skill id fallback directory should not exist, err=%v", err)
	}
}

func TestSetupFallsBackToSkillIDForUnsafeName(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("SKILL.md")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	init := New(fakeAPI{data: buf.Bytes()}, Options{Workdir: root})
	session := &selfhosted.Session{ID: "sess_1", Skills: []selfhosted.SkillRef{{Name: "../demo", ID: "sk_1"}}}
	if err := init.Setup(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "skills", "sk_1", "SKILL.md")); err != nil {
		t.Fatal(err)
	}
}

func TestSetupFlattensSingleRootZipAndPreservesExecutableMode(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	skillHeader := &zip.FileHeader{Name: "wrapped/SKILL.md", Method: zip.Deflate}
	skillHeader.SetMode(0o644)
	w, err := zw.CreateHeader(skillHeader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	runHeader := &zip.FileHeader{Name: "wrapped/bin/run.sh", Method: zip.Deflate}
	runHeader.SetMode(0o755)
	w, err = zw.CreateHeader(runHeader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("#!/bin/sh\n")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	init := New(fakeAPI{data: buf.Bytes()}, Options{Workdir: root})
	session := &selfhosted.Session{ID: "sess_1", Skills: []selfhosted.SkillRef{{Name: "demo", ID: "sk_1"}}}
	if err := init.Setup(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "skills", "demo", "SKILL.md")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "skills", "demo", "wrapped", "SKILL.md")); !os.IsNotExist(err) {
		t.Fatalf("single root directory was not flattened, err=%v", err)
	}
	stat, err := os.Stat(filepath.Join(root, "skills", "demo", "bin", "run.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if stat.Mode().Perm()&0o111 == 0 {
		t.Fatalf("run.sh mode = %s, want executable bit", stat.Mode().Perm())
	}
}

func TestSetupSkipsZipTraversal(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	if _, err := zw.Create("../escape"); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	init := New(fakeAPI{data: buf.Bytes()}, Options{Workdir: root})
	session := &selfhosted.Session{ID: "sess_1", Skills: []selfhosted.SkillRef{{Name: "demo", ID: "sk_1"}}}
	if err := init.Setup(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "escape")); !os.IsNotExist(err) {
		t.Fatalf("archive escaped workdir, err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "skills", "demo")); !os.IsNotExist(err) {
		t.Fatalf("skill dir should not be installed, err=%v", err)
	}
}

func TestSafeSkillNameRejectsUnsafeComponent(t *testing.T) {
	tests := []string{
		"../escape",
		`nested\escape`,
		".",
		"..",
		"demo\nname",
		"demo name",
		"中文",
	}
	for _, tt := range tests {
		if _, err := safeSkillName(tt); err == nil {
			t.Fatalf("safeSkillName(%q) succeeded", tt)
		}
	}
}

func TestExtractZipEnforcesActualExtractedBytes(t *testing.T) {
	target := filepath.Join(t.TempDir(), "large.txt")
	written, err := writeFile(target, bytes.NewBufferString("0123456789"), 0o600, 4)
	if err == nil || !strings.Contains(err.Error(), "skill extracted content too large") {
		t.Fatalf("writeFile() bytes=%d error=%v", written, err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("partial output was not removed: %v", err)
	}
}

func TestExtractZipEnforcesEntryLimit(t *testing.T) {
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for _, name := range []string{"one.txt", "two.txt"} {
		entry, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write([]byte(name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "skill.zip")
	if err := os.WriteFile(path, buffer.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	initializer := New(fakeAPI{}, Options{
		Workdir:           t.TempDir(),
		MaxArchiveEntries: 1,
	})
	err := initializer.extractZip(path, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "too many entries") {
		t.Fatalf("extractZip() error = %v", err)
	}
}
