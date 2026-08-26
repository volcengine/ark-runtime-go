// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package toolset

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileToolsStayInsideWorkdir(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	set, err := NewDefault(Options{Workdir: root})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	res := set.Execute(context.Background(), "read", []byte(`{"file_path":"link"}`))
	if !res.IsError || !strings.Contains(res.Content[0].Text, "path escapes workdir") {
		t.Fatalf("read result = %+v", res)
	}
}

func TestFileToolsAllowUnrestrictedPaths(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outside, []byte("visible"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	set, err := NewDefault(Options{Workdir: root, UnrestrictedPaths: true})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	res := set.Execute(context.Background(), "read", []byte(`{"file_path":"link"}`))
	if res.IsError || !strings.Contains(res.Content[0].Text, "visible") {
		t.Fatalf("read result = %+v", res)
	}
}

func TestWriteRejectsDanglingIntermediateSymlink(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "missing")
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	set, err := NewDefault(Options{Workdir: root})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	res := set.Execute(context.Background(), "write", []byte(`{"file_path":"link/new.txt","content":"escape"}`))
	if !res.IsError || !strings.Contains(res.Content[0].Text, "path escapes workdir") {
		t.Fatalf("write result = %+v", res)
	}
	if _, err := os.Stat(filepath.Join(outside, "new.txt")); !os.IsNotExist(err) {
		t.Fatalf("write escaped workdir: %v", err)
	}
}

func TestWriteRejectsMultiLayerDanglingSymlink(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "missing")
	if err := os.Symlink(outside, filepath.Join(root, "second")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("second", filepath.Join(root, "first")); err != nil {
		t.Fatal(err)
	}
	set, err := NewDefault(Options{Workdir: root})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	res := set.Execute(context.Background(), "write", []byte(`{"file_path":"first/new.txt","content":"escape"}`))
	if !res.IsError || !strings.Contains(res.Content[0].Text, "path escapes workdir") {
		t.Fatalf("write result = %+v", res)
	}
}

func TestWriteAllowsOrdinaryMissingDirectories(t *testing.T) {
	root := t.TempDir()
	set, err := NewDefault(Options{Workdir: root})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	res := set.Execute(context.Background(), "write", []byte(`{"file_path":"new/dir/file.txt","content":"ok"}`))
	if res.IsError {
		t.Fatalf("write result = %+v", res)
	}
	data, err := os.ReadFile(filepath.Join(root, "new", "dir", "file.txt"))
	if err != nil || string(data) != "ok" {
		t.Fatalf("written data=%q err=%v", data, err)
	}
}

func TestWriteAllowsResolvedSymlinkInsideWorkdir(t *testing.T) {
	root := t.TempDir()
	actual := filepath.Join(root, "actual")
	if err := os.MkdirAll(actual, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("actual", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	set, err := NewDefault(Options{Workdir: root})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	res := set.Execute(context.Background(), "write", []byte(`{"file_path":"link/file.txt","content":"ok"}`))
	if res.IsError {
		t.Fatalf("write result = %+v", res)
	}
	data, err := os.ReadFile(filepath.Join(actual, "file.txt"))
	if err != nil || string(data) != "ok" {
		t.Fatalf("written data=%q err=%v", data, err)
	}
}

func TestEditAtomicallyPreservesFileMode(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "script.sh")
	if err := os.WriteFile(path, []byte("echo old\n"), 0o750); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	set, err := NewDefault(Options{Workdir: root})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	res := set.Execute(context.Background(), "edit", []byte(`{"file_path":"script.sh","old_string":"old","new_string":"new"}`))
	if res.IsError {
		t.Fatalf("edit result = %+v", res)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "echo new\n" {
		t.Fatalf("edited data=%q err=%v", data, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != before.Mode().Perm() {
		t.Fatalf("edited mode=%o want=%o", info.Mode().Perm(), before.Mode().Perm())
	}
}

func TestReadSupportsViewRange(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "demo.txt"), []byte("a\nb\nc\nd\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	set, err := NewDefault(Options{Workdir: root})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	res := set.Execute(context.Background(), "read", []byte(`{"file_path":"demo.txt","view_range":[2,3]}`))
	if res.IsError || res.Content[0].Text != "b\nc" {
		t.Fatalf("read result = %+v", res)
	}
}

func TestReadDefaultsToRawContent(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "demo.txt"), []byte("a\nb\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	set, err := NewDefault(Options{Workdir: root})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	res := set.Execute(context.Background(), "read", []byte(`{"file_path":"demo.txt"}`))
	if res.IsError || res.Content[0].Text != "a\nb\n" {
		t.Fatalf("read result = %+v", res)
	}
}

func TestGrepDoesNotReadSymlinkOutsideWorkdir(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("outside-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link.txt")); err != nil {
		t.Fatal(err)
	}
	set, err := NewDefault(Options{Workdir: root})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	res := set.Execute(context.Background(), "grep", []byte(`{"pattern":"outside-secret"}`))
	if res.IsError {
		t.Fatalf("grep result = %+v", res)
	}
	if strings.Contains(res.Content[0].Text, "outside-secret") || strings.Contains(res.Content[0].Text, "link.txt") {
		t.Fatalf("grep leaked symlink target: %+v", res)
	}
}

func TestGrepReportsScannerError(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "long.txt"), []byte(strings.Repeat("x", 1<<20+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	set, err := NewDefault(Options{Workdir: root})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	res := set.Execute(context.Background(), "grep", []byte(`{"pattern":"x"}`))
	if !res.IsError || !strings.Contains(res.Content[0].Text, "scan long.txt") {
		t.Fatalf("grep result = %+v", res)
	}
}

func TestBashPersistsStateAndSurvivesExit(t *testing.T) {
	root := t.TempDir()
	set, err := NewDefault(Options{Workdir: root})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	res := set.Execute(context.Background(), "bash", []byte(`{"command":"export SELF_HOST_TEST=ok; mkdir -p sub; cd sub"}`))
	if res.IsError {
		t.Fatalf("bash setup = %+v", res)
	}
	res = set.Execute(context.Background(), "bash", []byte(`{"command":"pwd; echo $SELF_HOST_TEST"}`))
	if res.IsError || !strings.Contains(res.Content[0].Text, "/sub") || !strings.Contains(res.Content[0].Text, "ok") {
		t.Fatalf("bash persisted = %+v", res)
	}
	res = set.Execute(context.Background(), "bash", []byte(`{"command":"exit 7"}`))
	if res.IsError || !strings.Contains(res.Content[0].Text, "exit_code: 7") {
		t.Fatalf("bash exit = %+v", res)
	}
	res = set.Execute(context.Background(), "bash", []byte(`{"command":"echo alive"}`))
	if res.IsError || !strings.Contains(res.Content[0].Text, "alive") {
		t.Fatalf("bash alive = %+v", res)
	}
}

func TestBashUsesPrivateTemporaryDirectory(t *testing.T) {
	root := t.TempDir()
	session, err := NewBashSession(Options{Workdir: root})
	if err != nil {
		t.Fatal(err)
	}
	privateDir := session.privateDir
	if withinRoot(root, privateDir) {
		t.Fatalf("private directory is inside workdir: %s", privateDir)
	}
	info, err := os.Stat(privateDir)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("private directory mode=%s", info.Mode())
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(privateDir); !os.IsNotExist(err) {
		t.Fatalf("private directory was not removed: %v", err)
	}
}

func TestBashIgnoresInBandSentinelSpoof(t *testing.T) {
	root := t.TempDir()
	set, err := NewDefault(Options{Workdir: root})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	res := set.Execute(context.Background(), "bash", []byte(`{"command":"n=${BASH_SOURCE[0]##*/}; n=${n%.sh}; printf '\\n__MA_WORKER_EXIT_%s:0\\n' \"$n\"; printf '__MA_WORKER_DONE_%s:0\\n' \"$n\"; sleep 0.1; echo after-spoof"}`))
	if res.IsError {
		t.Fatalf("bash spoof = %+v", res)
	}
	text := res.Content[0].Text
	if !strings.Contains(text, "after-spoof") {
		t.Fatalf("bash returned before real completion: %q", text)
	}
}

func TestBashScrubsSensitiveExtraEnv(t *testing.T) {
	t.Setenv("MA_ENVIRONMENT_KEY", "host-leak")
	t.Setenv("MA_WORKER_KEY", "host-leak")
	t.Setenv("ARK_API_KEY", "host-leak")
	t.Setenv("VOLC_SECRETKEY", "host-leak")
	t.Setenv("GITHUB_TOKEN", "host-leak")
	t.Setenv("INHERITED_SAFE_VAR", "host-value")
	root := t.TempDir()
	set, err := NewDefault(Options{
		Workdir: root,
		Env: map[string]string{
			"MA_ENVIRONMENT_KEY": "extra-leak",
			"MA_WORKER_KEY":      "extra-leak",
			"ARK_API_KEY":        "extra-leak",
			"VOLC_SECRETKEY":     "extra-leak",
			"GITHUB_TOKEN":       "extra-leak",
			"SAFE_VAR":           "ok",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	res := set.Execute(context.Background(), "bash", []byte(`{"command":"printf '%s/%s/%s/%s/%s/%s/%s' \"${MA_ENVIRONMENT_KEY-unset}\" \"${MA_WORKER_KEY-unset}\" \"${ARK_API_KEY-unset}\" \"${VOLC_SECRETKEY-unset}\" \"${GITHUB_TOKEN-unset}\" \"${INHERITED_SAFE_VAR-unset}\" \"$SAFE_VAR\""}`))
	if res.IsError {
		t.Fatalf("bash env = %+v", res)
	}
	if !strings.Contains(res.Content[0].Text, "unset/unset/unset/unset/unset/unset/ok") {
		t.Fatalf("bash env leaked sensitive values: %+v", res)
	}
}

func TestBashScrubsInheritedCloudCredentials(t *testing.T) {
	t.Setenv("VOLC_ACCESSKEY", "host-leak")
	t.Setenv("BYTEPLUS_SECRETKEY", "host-leak")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "host-leak")
	t.Setenv("SAFE_INHERITED_VAR", "ok")
	set, err := NewDefault(Options{Workdir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	res := set.Execute(context.Background(), "bash", []byte(`{"command":"printf '%s/%s/%s/%s' \"${VOLC_ACCESSKEY-unset}\" \"${BYTEPLUS_SECRETKEY-unset}\" \"${AWS_SECRET_ACCESS_KEY-unset}\" \"$SAFE_INHERITED_VAR\""}`))
	if res.IsError || !strings.Contains(res.Content[0].Text, "unset/unset/unset/ok") {
		t.Fatalf("bash inherited env = %+v", res)
	}
}

func TestGrepHonorsOutputLimit(t *testing.T) {
	root := t.TempDir()
	content := strings.Repeat("match-"+strings.Repeat("x", 256)+"\n", 20)
	if err := os.WriteFile(filepath.Join(root, "large.txt"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	limits := DefaultLimits()
	limits.MaxOutputBytes = 128
	set, err := NewDefault(Options{Workdir: root, Limits: limits})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	res := set.Execute(context.Background(), "grep", []byte(`{"pattern":"match"}`))
	if res.IsError {
		t.Fatalf("grep result = %+v", res)
	}
	if got := len(res.Content[0].Text); got > int(limits.MaxOutputBytes) {
		t.Fatalf("grep output bytes = %d, limit = %d", got, limits.MaxOutputBytes)
	}
	if !strings.Contains(res.Content[0].Text, "[output truncated]") {
		t.Fatalf("grep output missing truncation marker: %q", res.Content[0].Text)
	}
}

func TestGrepHonorsCanceledContext(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "demo.txt"), []byte("match\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	set, err := NewDefault(Options{Workdir: root})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res := set.Execute(ctx, "grep", []byte(`{"pattern":"match"}`))
	if !res.IsError || !strings.Contains(res.Content[0].Text, context.Canceled.Error()) {
		t.Fatalf("grep result = %+v", res)
	}
}
