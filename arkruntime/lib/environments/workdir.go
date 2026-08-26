// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package environments

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
)

func sessionWorkdir(root, sessionID string) (string, error) {
	if root == "" {
		return "", errors.New("workdir root must not be empty")
	}
	name := sessionWorkdirName(sessionID)
	target := filepath.Join(root, name)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", errors.New("session workdir escapes root")
	}
	return target, nil
}

func sessionWorkdirName(sessionID string) string {
	if isSafeWorkdirName(sessionID) {
		return sessionID
	}
	sum := sha256.Sum256([]byte(sessionID))
	return "session-" + hex.EncodeToString(sum[:])
}

func isSafeWorkdirName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	clean := filepath.Clean(name)
	if clean != name || strings.Contains(name, "/") || strings.Contains(name, `\`) {
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
