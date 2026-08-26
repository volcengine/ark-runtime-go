// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
// Package envinit 准备 self-hosted worker 的 session 执行环境。
package envinit

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/volcengine/ark-runtime-go/arkruntime/internal/selfhostedlog"
	selfhosted "github.com/volcengine/ark-runtime-go/arkruntime/selfhosted"
)

const (
	defaultMaxArchiveBytes   = 128 << 20
	defaultMaxExtractedBytes = 512 << 20
	defaultMaxArchiveEntries = 10_000
	parentDirName            = ".."
)

// Options 是 session 环境初始化配置。
type Options struct {
	Workdir           string
	SkillsDir         string
	MaxArchiveBytes   int64
	MaxExtractedBytes int64
	MaxArchiveEntries int
	Logger            *log.Logger
}

// Initializer 执行 workdir 与 skills 初始化。
type Initializer struct {
	api    selfhosted.API
	opts   Options
	logger *selfhostedlog.Logger
}

// New 创建环境初始化器。
func New(api selfhosted.API, opts Options) *Initializer {
	if opts.MaxArchiveBytes <= 0 {
		opts.MaxArchiveBytes = defaultMaxArchiveBytes
	}
	if opts.MaxExtractedBytes <= 0 {
		opts.MaxExtractedBytes = defaultMaxExtractedBytes
	}
	if opts.MaxArchiveEntries <= 0 {
		opts.MaxArchiveEntries = defaultMaxArchiveEntries
	}
	if opts.SkillsDir == "" {
		opts.SkillsDir = filepath.Join(opts.Workdir, "skills")
	}
	return &Initializer{api: api, opts: opts, logger: selfhostedlog.New(opts.Logger)}
}

// Setup 创建 workdir 并安装 session 绑定的 skills。
func (i *Initializer) Setup(ctx context.Context, session *selfhosted.Session) error {
	if session == nil {
		return errors.New("session must not be nil")
	}
	if i.opts.Workdir == "" {
		return errors.New("workdir must not be empty")
	}
	if err := os.MkdirAll(i.opts.Workdir, 0o755); err != nil {
		return fmt.Errorf("create workdir: %w", err)
	}
	if err := os.MkdirAll(i.opts.SkillsDir, 0o755); err != nil {
		return fmt.Errorf("create skills dir: %w", err)
	}
	for _, skill := range session.SkillRefs() {
		if err := i.installSkill(ctx, session.ID, skill); err != nil {
			i.logger.Warn("failed to install skill", "session_id", session.ID, "skill", skill.NameValue(), "version", skill.Version, "err", err)
		}
	}
	return nil
}

func (i *Initializer) installSkill(ctx context.Context, sessionID string, skill selfhosted.SkillRef) error {
	if resolver, ok := i.api.(selfhosted.SkillResolver); ok && strings.TrimSpace(skill.IDValue()) != "" {
		resolved, err := resolver.ResolveSkill(ctx, skill)
		if err != nil {
			return fmt.Errorf("resolve skill %s: %w", skill.IDValue(), err)
		}
		skill = resolved
	}
	name, err := safeSkillDirName(skill)
	if err != nil {
		return err
	}
	i.logger.Info("install skill", "session_id", sessionID, "skill", name, "version", skill.Version)
	content, err := i.api.OpenSkill(ctx, selfhosted.OpenSkillRequest{SessionID: sessionID, Skill: skill})
	if err != nil {
		return fmt.Errorf("download skill %s: %w", name, err)
	}
	if content == nil || content.Body == nil {
		return fmt.Errorf("download skill %s: empty content", name)
	}
	defer func() { _ = content.Body.Close() }()

	archivePath, err := i.copyArchive(name, content.Body)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(archivePath) }()

	tmp, err := os.MkdirTemp(i.opts.SkillsDir, "."+name+"-*")
	if err != nil {
		return fmt.Errorf("create skill temp dir: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(tmp)
		}
	}()
	if err := i.extractArchive(archivePath, tmp); err != nil {
		return err
	}
	source, err := installSourceDir(tmp)
	if err != nil {
		return err
	}
	target := filepath.Join(i.opts.SkillsDir, name)
	backup, err := replaceSkillDir(source, target)
	if err != nil {
		return fmt.Errorf("commit skill %s: %w", name, err)
	}
	committed = true
	if source != tmp {
		_ = os.RemoveAll(tmp)
	}
	if backup != "" {
		if err := os.RemoveAll(backup); err != nil {
			i.logger.Warn("remove old skill backup failed", "session_id", sessionID, "skill", name, "path", backup, "err", err)
		}
	}
	return nil
}

func replaceSkillDir(source, target string) (string, error) {
	if _, err := os.Lstat(target); errors.Is(err, os.ErrNotExist) {
		return "", os.Rename(source, target)
	} else if err != nil {
		return "", err
	}
	backup, err := os.MkdirTemp(filepath.Dir(target), "."+filepath.Base(target)+"-backup-*")
	if err != nil {
		return "", err
	}
	if err := os.Remove(backup); err != nil {
		return "", err
	}
	if err := os.Rename(target, backup); err != nil {
		return "", err
	}
	if err := os.Rename(source, target); err != nil {
		if rollbackErr := os.Rename(backup, target); rollbackErr != nil {
			return "", fmt.Errorf("replace skill: %w; rollback: %v", err, rollbackErr)
		}
		return "", err
	}
	return backup, nil
}

func safeSkillDirName(skill selfhosted.SkillRef) (string, error) {
	for _, candidate := range []string{skill.Name, skill.DisplayName, skill.IDValue()} {
		name, err := safeSkillName(candidate)
		if err == nil {
			return name, nil
		}
	}
	return "", fmt.Errorf("invalid skill name: %q", skill.Name)
}

func (i *Initializer) copyArchive(name string, body io.Reader) (string, error) {
	tmp, err := os.CreateTemp("", "ark-skill-"+name+"-*")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	defer func() { _ = tmp.Close() }()
	n, err := io.Copy(tmp, io.LimitReader(body, i.opts.MaxArchiveBytes+1))
	if err != nil {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("copy skill archive: %w", err)
	}
	if n > i.opts.MaxArchiveBytes {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("skill archive too large: %d bytes", n)
	}
	return tmpName, nil
}

func (i *Initializer) extractArchive(archivePath, dst string) error {
	f, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	magic := make([]byte, 4)
	n, _ := io.ReadFull(f, magic)
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	switch {
	case n >= 4 && magic[0] == 'P' && magic[1] == 'K':
		return i.extractZip(archivePath, dst)
	case n >= 2 && magic[0] == 0x1f && magic[1] == 0x8b:
		return i.extractTarGz(f, dst)
	default:
		return errors.New("unsupported skill archive format")
	}
}

func (i *Initializer) extractZip(archivePath, dst string) error {
	zr, err := zip.OpenReader(archivePath)
	if err != nil {
		return fmt.Errorf("open zip: %w", err)
	}
	defer func() { _ = zr.Close() }()
	if len(zr.File) > i.opts.MaxArchiveEntries {
		return fmt.Errorf("skill archive contains too many entries: %d", len(zr.File))
	}
	var total int64
	for _, file := range zr.File {
		target, err := safeJoin(dst, file.Name)
		if err != nil {
			return err
		}
		if file.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}
		if file.Mode()&os.ModeType != 0 {
			return fmt.Errorf("unsupported zip entry type: %s", file.Name)
		}
		remaining := i.opts.MaxExtractedBytes - total
		if remaining < 0 || file.UncompressedSize64 > uint64(remaining) {
			return fmt.Errorf("skill extracted content too large: more than %d bytes", i.opts.MaxExtractedBytes)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		rc, err := file.Open()
		if err != nil {
			return err
		}
		written, err := writeFile(target, rc, file.Mode(), remaining)
		if err != nil {
			_ = rc.Close()
			return err
		}
		_ = rc.Close()
		total += written
	}
	return nil
}

func (i *Initializer) extractTarGz(r io.Reader, dst string) error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("open gzip: %w", err)
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	var total int64
	entries := 0
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read tar: %w", err)
		}
		entries++
		if entries > i.opts.MaxArchiveEntries {
			return fmt.Errorf("skill archive contains too many entries: %d", entries)
		}
		target, err := safeJoin(dst, header.Name)
		if err != nil {
			return err
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg, 0:
			remaining := i.opts.MaxExtractedBytes - total
			if remaining < 0 || header.Size < 0 || header.Size > remaining {
				return fmt.Errorf("skill extracted content too large: more than %d bytes", i.opts.MaxExtractedBytes)
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			written, err := writeFile(target, tr, os.FileMode(header.Mode).Perm(), remaining)
			if err != nil {
				return err
			}
			total += written
		default:
			return fmt.Errorf("unsupported tar entry type: %s", header.Name)
		}
	}
}

func writeFile(target string, r io.Reader, mode os.FileMode, maxBytes int64) (int64, error) {
	if mode == 0 {
		mode = 0o644
	}
	f, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode.Perm())
	if err != nil {
		return 0, err
	}
	written, err := io.Copy(f, io.LimitReader(r, maxBytes+1))
	if written > maxBytes {
		_ = f.Close()
		_ = os.Remove(target)
		return written, fmt.Errorf("skill extracted content too large: more than %d bytes", maxBytes)
	}
	if err != nil {
		_ = f.Close()
		return written, err
	}
	if err := f.Close(); err != nil {
		return written, err
	}
	return written, os.Chmod(target, mode.Perm())
}

func safeSkillName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("skill name must not be empty")
	}
	if !isSafeNameComponent(name) {
		return "", fmt.Errorf("invalid skill name: %q", name)
	}
	return name, nil
}

func isSafeNameComponent(name string) bool {
	if name == "" || name == "." || name == parentDirName {
		return false
	}
	for _, c := range name {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
			continue
		}
		if c == '.' || c == '_' || c == '-' {
			continue
		}
		return false
	}
	return true
}

func safeJoin(root, name string) (string, error) {
	if name == "" || filepath.IsAbs(name) {
		return "", fmt.Errorf("invalid archive path: %s", name)
	}
	clean := filepath.Clean(name)
	if clean == "." || clean == parentDirName || strings.HasPrefix(clean, parentDirName+string(os.PathSeparator)) {
		return "", fmt.Errorf("archive path escapes skill dir: %s", name)
	}
	target := filepath.Join(root, clean)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == parentDirName || strings.HasPrefix(rel, parentDirName+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("archive path escapes skill dir: %s", name)
	}
	return target, nil
}

func installSourceDir(tmp string) (string, error) {
	entries, err := os.ReadDir(tmp)
	if err != nil {
		return "", err
	}
	if len(entries) != 1 || !entries[0].IsDir() {
		return tmp, nil
	}
	return filepath.Join(tmp, entries[0].Name()), nil
}
