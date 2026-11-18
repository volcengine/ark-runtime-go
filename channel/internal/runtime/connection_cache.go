// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrConnectionCacheFull 表示缓存容量均被正在使用的连接占用，调用方应有界重试。
var ErrConnectionCacheFull = errors.New("connection cache full")

type connectionEntry struct {
	connection Connection
	references int
	idleAt     time.Time
}

// ConnectionCache 按不可变绑定键缓存连接，合并并发加载且不淘汰使用中的连接。
// 每次 Acquire 必须配对 Release；权威 enabled/epoch 核验必须在缓存外完成。
type ConnectionCache struct {
	mu       sync.Mutex
	capacity int
	idleTTL  time.Duration
	entries  map[string]*connectionEntry
	loading  map[string]chan struct{}
}

// NewConnectionCache 创建有容量和空闲淘汰边界的连接缓存。
func NewConnectionCache(capacity int, idleTTL time.Duration) (*ConnectionCache, error) {
	if capacity <= 0 || idleTTL <= 0 {
		return nil, ErrRuntimeConfig
	}
	return &ConnectionCache{capacity: capacity, idleTTL: idleTTL, entries: make(map[string]*connectionEntry), loading: make(map[string]chan struct{})}, nil
}

// Acquire 返回引用计数租约；load 返回的 Release 在淘汰时调用。
func (c *ConnectionCache) Acquire(ctx context.Context, key string, load func(context.Context) (Connection, error)) (Connection, error) {
	if key == "" || load == nil {
		return Connection{}, ErrRuntimeConfig
	}
	for {
		if err := ctx.Err(); err != nil {
			return Connection{}, err
		}
		c.mu.Lock()
		if pending := c.loading[key]; pending != nil {
			c.mu.Unlock()
			select {
			case <-pending:
				continue
			case <-ctx.Done():
				return Connection{}, ctx.Err()
			}
		}
		if entry := c.entries[key]; entry != nil {
			if entry.references == 0 && time.Since(entry.idleAt) >= c.idleTTL {
				delete(c.entries, key)
				c.mu.Unlock()
				releaseConnection(entry.connection)
				continue
			}
			result := c.lease(entry)
			c.mu.Unlock()
			return result, nil
		}
		var retired []Connection
		now := time.Now()
		for k, entry := range c.entries {
			if entry.references == 0 && now.Sub(entry.idleAt) >= c.idleTTL {
				delete(c.entries, k)
				retired = append(retired, entry.connection)
			}
		}
		if len(c.entries)+len(c.loading) >= c.capacity {
			oldest := ""
			var idle time.Time
			for k, entry := range c.entries {
				if entry.references == 0 && (oldest == "" || entry.idleAt.Before(idle)) {
					oldest = k
					idle = entry.idleAt
				}
			}
			if oldest != "" {
				retired = append(retired, c.entries[oldest].connection)
				delete(c.entries, oldest)
			} else {
				c.mu.Unlock()
				for _, v := range retired {
					releaseConnection(v)
				}
				return Connection{}, ErrConnectionCacheFull
			}
		}
		pending := make(chan struct{})
		c.loading[key] = pending
		c.mu.Unlock()
		for _, v := range retired {
			releaseConnection(v)
		}
		value, err := load(ctx)
		c.mu.Lock()
		delete(c.loading, key)
		var result Connection
		if err == nil {
			entry := &connectionEntry{connection: value}
			c.entries[key] = entry
			result = c.lease(entry)
		}
		close(pending)
		c.mu.Unlock()
		if err != nil {
			releaseConnection(value)
		}
		return result, err
	}
}

func (c *ConnectionCache) lease(entry *connectionEntry) Connection {
	entry.references++
	value := entry.connection
	var once sync.Once
	value.Release = func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			entry.references--
			if entry.references == 0 {
				entry.idleAt = time.Now()
			}
		})
	}
	return value
}

// PurgeIdle 释放所有未使用连接；使用中的连接保留至下一次清理。
func (c *ConnectionCache) PurgeIdle() {
	c.mu.Lock()
	var retired []Connection
	for key, entry := range c.entries {
		if entry.references == 0 {
			delete(c.entries, key)
			retired = append(retired, entry.connection)
		}
	}
	c.mu.Unlock()
	for _, entry := range retired {
		releaseConnection(entry)
	}
}
