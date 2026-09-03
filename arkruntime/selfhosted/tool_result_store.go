// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package selfhosted

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	fileToolResultStateStarted = "started"
	fileToolResultStateResult  = "result"
	fileToolResultStateSent    = "sent"
)

// FileToolResultStore 用本地文件持久化 tool_use 执行状态。
type FileToolResultStore struct {
	dir string
}

type fileToolResultRecord struct {
	CallID    string `json:"call_id"`
	State     string `json:"state"`
	Event     Event  `json:"event"`
	Result    Event  `json:"result,omitempty"`
	UpdatedAt string `json:"updated_at"`
}

// NewFileToolResultStore 在 workdir 下创建 tool result 持久化 store。
func NewFileToolResultStore(workdir string) (*FileToolResultStore, error) {
	return newFileToolResultStore(workdir, "")
}

// NewSessionFileToolResultStore 在 workdir 下创建按 session 隔离的 tool result 持久化 store。
func NewSessionFileToolResultStore(workdir, sessionID string) (*FileToolResultStore, error) {
	if sessionID == "" {
		return nil, errors.New("session id must not be empty")
	}
	return newFileToolResultStore(workdir, toolResultStoreSessionDir(sessionID))
}

func newFileToolResultStore(workdir, sessionDir string) (*FileToolResultStore, error) {
	if workdir == "" {
		return nil, errors.New("workdir must not be empty")
	}
	dir := filepath.Join(workdir, ".ma_self_hosted_worker", "tool_ledger")
	if sessionDir != "" {
		dir = filepath.Join(dir, sessionDir)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create tool result store: %w", err)
	}
	return &FileToolResultStore{dir: dir}, nil
}

func toolResultStoreSessionDir(sessionID string) string {
	if sessionID != "." && sessionID != ".." {
		safe := true
		for _, c := range sessionID {
			if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
				c == '.' || c == '_' || c == '-' {
				continue
			}
			safe = false
			break
		}
		if safe {
			return sessionID
		}
	}
	sum := sha256.Sum256([]byte(sessionID))
	return "session-" + hex.EncodeToString(sum[:])
}

// Recover 恢复未回写的 tool result 和已经完成回写的 tool_use。
func (s *FileToolResultStore) Recover() (map[string]Event, map[string]bool, error) {
	pending := map[string]Event{}
	processed := map[string]bool{}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasPrefix(entry.Name(), ".tool-result-") {
			_ = os.Remove(filepath.Join(s.dir, entry.Name()))
			continue
		}
		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		record, err := s.readPath(filepath.Join(s.dir, entry.Name()))
		if err != nil {
			return nil, nil, err
		}
		switch record.State {
		case fileToolResultStateSent:
			processed[record.CallID] = true
		case fileToolResultStateResult:
			pending[record.CallID] = record.Result
		case fileToolResultStateStarted:
			record.Result = unknownToolExecutionResult(record.CallID, record.Event)
			record.State = fileToolResultStateResult
			if err := s.write(record); err != nil {
				return nil, nil, err
			}
			pending[record.CallID] = record.Result
		default:
			return nil, nil, fmt.Errorf("unknown tool result state %q for call %s", record.State, record.CallID)
		}
	}
	return pending, processed, nil
}

// Begin 记录一个 tool_use 即将开始执行，并返回是否已有可复用结果。
func (s *FileToolResultStore) Begin(callID string, event Event) (ToolCallStoreDecision, error) {
	if callID == "" {
		return ToolCallStoreDecision{}, errors.New("call id must not be empty")
	}
	record, err := s.read(callID)
	if err == nil {
		switch record.State {
		case fileToolResultStateSent:
			return ToolCallStoreDecision{Sent: true}, nil
		case fileToolResultStateResult:
			return ToolCallStoreDecision{Result: record.Result}, nil
		case fileToolResultStateStarted:
			record.Result = unknownToolExecutionResult(record.CallID, record.Event)
			record.State = fileToolResultStateResult
			if err := s.write(record); err != nil {
				return ToolCallStoreDecision{}, err
			}
			return ToolCallStoreDecision{Result: record.Result}, nil
		default:
			return ToolCallStoreDecision{}, fmt.Errorf("unknown tool result state %q for call %s", record.State, callID)
		}
	}
	if !errors.Is(err, os.ErrNotExist) {
		return ToolCallStoreDecision{}, err
	}
	return ToolCallStoreDecision{}, s.write(fileToolResultRecord{
		CallID:    callID,
		State:     fileToolResultStateStarted,
		Event:     event,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
}

// SaveResult 持久化 tool_use 结果。
func (s *FileToolResultStore) SaveResult(callID string, result Event) error {
	record, err := s.read(callID)
	if err != nil {
		return err
	}
	record.State = fileToolResultStateResult
	record.Result = result
	record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return s.write(record)
}

// MarkSent 标记 tool result 已经成功回写到控制面。
func (s *FileToolResultStore) MarkSent(callID string) error {
	record, err := s.read(callID)
	if err != nil {
		return err
	}
	record.State = fileToolResultStateSent
	record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return s.write(record)
}

func (s *FileToolResultStore) read(callID string) (fileToolResultRecord, error) {
	return s.readPath(s.path(callID))
}

func (s *FileToolResultStore) readPath(path string) (fileToolResultRecord, error) {
	var record fileToolResultRecord
	data, err := os.ReadFile(path)
	if err != nil {
		return record, err
	}
	if err := json.Unmarshal(data, &record); err != nil {
		return record, fmt.Errorf("decode tool result store %s: %w", path, err)
	}
	if record.CallID == "" {
		return record, fmt.Errorf("tool result store %s missing call_id", path)
	}
	return record, nil
}

func (s *FileToolResultStore) write(record fileToolResultRecord) error {
	if record.CallID == "" {
		return errors.New("call id must not be empty")
	}
	record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	target := s.path(record.CallID)
	tmp, err := os.CreateTemp(s.dir, ".tool-result-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, target); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	dir, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}

func (s *FileToolResultStore) path(callID string) string {
	sum := sha256.Sum256([]byte(callID))
	return filepath.Join(s.dir, hex.EncodeToString(sum[:])+".json")
}

func unknownToolExecutionResult(callID string, event Event) Event {
	content := []ContentBlock{{
		Type: "text",
		Text: "tool execution state is unknown after worker restart; refusing to re-execute this tool_use to avoid duplicate side effects",
	}}
	if event.Type == EventTypeAgentCustomToolUse {
		return NewUserCustomToolResultEvent(callID, content, true, event.SessionThreadID)
	}
	return NewUserToolResultEvent(callID, content, true, event.SessionThreadID)
}

var _ ToolResultStore = (*FileToolResultStore)(nil)
