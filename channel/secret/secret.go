// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package secret resolves environment and local-file secret references at adapter boundaries.
package secret

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const maxFileBytes = 64 * 1024

var errInvalidRef = errors.New("invalid secret reference")

// Validate checks SecretRef syntax without reading secrets or accessing the network.
func Validate(ref string) error {
	switch {
	case strings.HasPrefix(ref, "memory:"):
		name := strings.TrimPrefix(ref, "memory:")
		if name == "" || len(name) > 512 || strings.ContainsAny(name, "\x00\r\n\t ") {
			return errInvalidRef
		}
	case strings.HasPrefix(ref, "env:"):
		name := strings.TrimPrefix(ref, "env:")
		if name == "" || !validEnvName(name) {
			return errInvalidRef
		}
	case strings.HasPrefix(ref, "file:"):
		path := strings.TrimPrefix(ref, "file:")
		if path == "" || !filepath.IsAbs(path) {
			return errInvalidRef
		}
	default:
		return errInvalidRef
	}
	return nil
}

func validEnvName(name string) bool {
	for i, c := range name {
		if c == '_' || c >= 'A' && c <= 'Z' || i > 0 && c >= '0' && c <= '9' {
			continue
		}
		return false
	}
	return true
}

// Resolver is explicitly injected and resolves secrets only when constructing
// a channel client.
type Resolver interface {
	Resolve(string) (string, error)
}

// EnvFileResolver resolves secrets from the process environment or restricted local files.
type EnvFileResolver struct{}

// Resolve returns plaintext to an adapter and omits the reference, path, and
// contents from error messages.
func (EnvFileResolver) Resolve(ref string) (string, error) {
	if err := Validate(ref); err != nil {
		return "", err
	}
	if strings.HasPrefix(ref, "env:") {
		value, ok := os.LookupEnv(strings.TrimPrefix(ref, "env:"))
		if !ok || value == "" {
			return "", errors.New("secret environment variable unavailable")
		}
		return value, nil
	}
	if !strings.HasPrefix(ref, "file:") {
		return "", errInvalidRef
	}
	path := strings.TrimPrefix(ref, "file:")
	return resolveFile(path, openNoFollow)
}

func resolveFile(path string, open func(string) (*os.File, error)) (string, error) {
	entry, err := os.Lstat(path)
	if err != nil || !entry.Mode().IsRegular() {
		return "", errors.New("secret file unavailable")
	}
	if mode := entry.Mode().Perm(); mode != 0o400 && mode != 0o600 {
		return "", errors.New("secret file permissions invalid")
	}
	if entry.Size() > maxFileBytes {
		return "", errors.New("secret file too large")
	}
	file, err := open(path)
	if err != nil {
		return "", errors.New("secret file unavailable")
	}
	defer func() { _ = file.Close() }()
	actual, err := file.Stat()
	if err != nil || !actual.Mode().IsRegular() || actual.Mode().Perm() != entry.Mode().Perm() || !os.SameFile(entry, actual) {
		return "", errors.New("secret file changed")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxFileBytes+1))
	if err != nil || len(data) > maxFileBytes {
		return "", errors.New("secret file unreadable")
	}
	value := strings.TrimRight(string(data), "\r\n")
	if value == "" {
		return "", errors.New("secret file empty")
	}
	return value, nil
}

func openNoFollow(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}
