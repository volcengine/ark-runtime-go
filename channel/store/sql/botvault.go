// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/go-sql-driver/mysql"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// 本文件实现 BotVaultStore：per-principal Vault 创建协调。
// 所有判定都在对 reservation 行加 FOR UPDATE（MySQL）或写事务串行（SQLite）后进行，
// 保证多副本下同一 (principal,name) 只有一个在途 POST；记录不含任何密钥。

func (s *store) botVaultTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	return s.runTx(ctx, fn)
}

func (s *store) loadReservation(ctx context.Context, tx *sql.Tx, principal, name string) (*core.BotVaultReservation, error) {
	return loadOne[core.BotVaultReservation](ctx, tx, "SELECT data FROM "+s.t.botVaults+" WHERE principal = ? AND name = ?"+s.dialect.RowLockSuffix, principal, name)
}

func insertReservation(ctx context.Context, tx *sql.Tx, table string, r core.BotVaultReservation, nowNano int64) error {
	insert := "INSERT INTO " + table + " (principal, name, state, fence_owner, fence, lease_expires_at, vault_id, created_at, updated_at, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	_, err := tx.ExecContext(ctx, insert, r.Principal, r.Name, r.State, r.Owner, r.Fence, nowNano, r.VaultID, nowNano, nowNano, mustEncode(r))
	if reservationDuplicate(err) {
		return core.ErrVaultNameConflict
	}
	return err
}

func reservationDuplicate(err error) bool {
	var failure *mysql.MySQLError
	if errors.As(err, &failure) {
		return failure.Number == 1062
	}
	var sqliteFailure interface{ Code() int }
	return errors.As(err, &sqliteFailure) && (sqliteFailure.Code() == 1555 || sqliteFailure.Code() == 2067)
}

func saveReservation(ctx context.Context, tx *sql.Tx, table string, r core.BotVaultReservation, nowNano int64) error {
	_, err := tx.ExecContext(ctx, "UPDATE "+table+" SET state = ?, fence_owner = ?, fence = ?, lease_expires_at = ?, vault_id = ?, updated_at = ?, data = ? WHERE principal = ? AND name = ?",
		r.State, r.Owner, r.Fence, nowNano, r.VaultID, nowNano, mustEncode(r), r.Principal, r.Name)
	return err
}

// ReserveBotVault 见 core.BotVaultStore。
func (s *store) ReserveBotVault(ctx context.Context, principal, name, owner string, ttl time.Duration) (core.BotVaultReservation, error) {
	if principal == "" || name == "" || owner == "" || ttl <= 0 {
		return core.BotVaultReservation{}, core.ErrInvalid
	}
	var result core.BotVaultReservation
	err := s.botVaultTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		existing, err := s.loadReservation(ctx, tx, principal, name)
		if err != nil {
			return err
		}
		now := s.now(ctx)
		if existing == nil {
			result = core.BotVaultReservation{Principal: principal, Name: name, State: core.BotVaultIntentPending, Owner: owner, Fence: 1, LeaseExpiresAt: now.Add(ttl), CreatedAt: now, UpdatedAt: now}
			return insertReservation(ctx, tx, s.t.botVaults, result, nanos(now))
		}
		switch {
		case existing.State == core.BotVaultIntentConfirmed:
			// Winner 已确认：不重复 POST，调用方按 VaultID 走查找/校验。
			result = *existing
			return nil
		case existing.Active(now):
			// 有效 fence 持有中：同 owner 幂等返回（重试/续跑），其他 owner 拒绝。
			if existing.Owner == owner {
				result = *existing
				return nil
			}
			return core.ErrVaultNameConflict
		default:
			// fence 已过期：当前持有者疑似崩溃，接管并推进 fence。旧 POST 结果未知，
			// 保留 PostAttempted 与 VaultID（若已按名查到），由新 owner 走只查找分支，绝不二次 POST。
			taken := *existing
			taken.State = core.BotVaultIntentPending
			taken.Owner = owner
			taken.Fence++
			taken.LeaseExpiresAt = now.Add(ttl)
			taken.UpdatedAt = now
			if err := saveReservation(ctx, tx, s.t.botVaults, taken, nanos(now)); err != nil {
				return err
			}
			result = taken
			return nil
		}
	})
	return result, err
}

// RenewBotVaultFence 见 core.BotVaultStore。
func (s *store) RenewBotVaultFence(ctx context.Context, principal, name, owner string, fence uint64, ttl time.Duration) (core.BotVaultReservation, error) {
	if principal == "" || name == "" || owner == "" || fence == 0 || ttl <= 0 {
		return core.BotVaultReservation{}, core.ErrInvalid
	}
	var result core.BotVaultReservation
	err := s.botVaultTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		existing, err := s.loadReservation(ctx, tx, principal, name)
		if err != nil {
			return err
		}
		if existing == nil || existing.State != core.BotVaultIntentPending || !existing.Holds(owner, fence) {
			return core.ErrLeaseLost
		}
		now := s.now(ctx)
		existing.LeaseExpiresAt = now.Add(ttl)
		existing.UpdatedAt = now
		if err := saveReservation(ctx, tx, s.t.botVaults, *existing, nanos(now)); err != nil {
			return err
		}
		result = *existing
		return nil
	})
	return result, err
}

// MarkBotVaultPosted 见 core.BotVaultStore；只更新 data 中的 PostAttempted 标记。
func (s *store) MarkBotVaultPosted(ctx context.Context, principal, name, owner string, fence uint64) (core.BotVaultReservation, error) {
	if principal == "" || name == "" || owner == "" || fence == 0 {
		return core.BotVaultReservation{}, core.ErrInvalid
	}
	var result core.BotVaultReservation
	err := s.botVaultTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		existing, err := s.loadReservation(ctx, tx, principal, name)
		if err != nil {
			return err
		}
		if existing == nil || existing.State != core.BotVaultIntentPending || !existing.Holds(owner, fence) {
			return core.ErrLeaseLost
		}
		now := s.now(ctx)
		existing.PostAttempted = true
		existing.UpdatedAt = now
		if err := saveReservation(ctx, tx, s.t.botVaults, *existing, nanos(now)); err != nil {
			return err
		}
		result = *existing
		return nil
	})
	return result, err
}

// ConfirmBotVault 见 core.BotVaultStore。
func (s *store) ConfirmBotVault(ctx context.Context, principal, name, owner string, fence uint64, vaultID string) (core.BotVaultReservation, error) {
	if principal == "" || name == "" || owner == "" || fence == 0 || vaultID == "" {
		return core.BotVaultReservation{}, core.ErrInvalid
	}
	var result core.BotVaultReservation
	err := s.botVaultTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		existing, err := s.loadReservation(ctx, tx, principal, name)
		if err != nil {
			return err
		}
		if existing == nil || existing.Principal != principal {
			return core.ErrVaultPrincipalMismatch
		}
		if existing.State == core.BotVaultIntentConfirmed {
			// 幂等：同一 Vault 归属重复确认直接返回；不同 Vault 占了同 key 是确定性冲突。
			if existing.VaultID == vaultID {
				result = *existing
				return nil
			}
			return core.ErrConflict
		}
		if !existing.Holds(owner, fence) {
			return core.ErrLeaseLost
		}
		now := s.now(ctx)
		existing.State = core.BotVaultIntentConfirmed
		existing.VaultID = vaultID
		existing.LeaseExpiresAt = now
		existing.UpdatedAt = now
		if err := saveReservation(ctx, tx, s.t.botVaults, *existing, nanos(now)); err != nil {
			return err
		}
		result = *existing
		return nil
	})
	return result, err
}

// RejectBotVault 见 core.BotVaultStore；只能解除自己持有的 pending 意图。
func (s *store) RejectBotVault(ctx context.Context, principal, name, owner string, fence uint64) error {
	if principal == "" || name == "" || owner == "" || fence == 0 {
		return core.ErrInvalid
	}
	return s.botVaultTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		existing, err := s.loadReservation(ctx, tx, principal, name)
		if err != nil {
			return err
		}
		if existing == nil {
			return core.ErrNotFound
		}
		if existing.State != core.BotVaultIntentPending {
			return core.ErrConflict
		}
		if !existing.Holds(owner, fence) {
			return core.ErrLeaseLost
		}
		_, err = tx.ExecContext(ctx, "DELETE FROM "+s.t.botVaults+" WHERE principal = ? AND name = ?", principal, name)
		return err
	})
}

// GetBotVaultReservation 见 core.BotVaultStore。
func (s *store) GetBotVaultReservation(ctx context.Context, principal, name string) (*core.BotVaultReservation, error) {
	if principal == "" || name == "" {
		return nil, core.ErrInvalid
	}
	r, err := loadOne[core.BotVaultReservation](ctx, s.db, "SELECT data FROM "+s.t.botVaults+" WHERE principal = ? AND name = ?", principal, name)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, nil
	}
	return r, nil
}
