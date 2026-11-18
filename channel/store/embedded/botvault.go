// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"context"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// bucketBotVaults 保存 schema 5 的 per-principal Bot Vault 创建协调记录，键为 principal\x00name。
var bucketBotVaults = []byte("bot_vault_reservations")

func botVaultKey(principal, name string) []byte {
	return key([]byte(principal), []byte(name))
}

func (s *store) updateBotVault(ctx context.Context, fn func(storeTx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.update(ctx, fn)
}

func (s *store) viewBotVault(ctx context.Context, fn func(storeTx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.view(ctx, fn)
}

// ReserveBotVault 见 core.BotVaultStore；bbolt 写事务天然串行，语义与 SQL 实现一致。
func (s *store) ReserveBotVault(ctx context.Context, principal, name, owner string, ttl time.Duration) (core.BotVaultReservation, error) {
	if principal == "" || name == "" || owner == "" || ttl <= 0 {
		return core.BotVaultReservation{}, core.ErrInvalid
	}
	var result core.BotVaultReservation
	err := s.updateBotVault(ctx, func(tx storeTx) error {
		bucket := tx.Bucket(bucketBotVaults)
		if bucket == nil {
			return errSchema
		}
		now := s.now()
		k := botVaultKey(principal, name)
		var existing *core.BotVaultReservation
		if data := bucket.Get(k); data != nil {
			var r core.BotVaultReservation
			if err := core.DecodeStored(data, &r); err != nil {
				return err
			}
			existing = &r
		}
		switch {
		case existing == nil:
			result = core.BotVaultReservation{Principal: principal, Name: name, State: core.BotVaultIntentPending, Owner: owner, Fence: 1, LeaseExpiresAt: now.Add(ttl), CreatedAt: now, UpdatedAt: now}
		case existing.State == core.BotVaultIntentConfirmed:
			result = *existing
			return nil
		case existing.Active(now):
			if existing.Owner == owner {
				result = *existing
				return nil
			}
			return core.ErrVaultNameConflict
		default:
			// fence 已过期：接管并推进 fence；保留 PostAttempted（旧 POST 结果未知），
			// 新 owner 只能查找，不能再次 POST。
			taken := *existing
			taken.State = core.BotVaultIntentPending
			taken.Owner = owner
			taken.Fence++
			taken.LeaseExpiresAt = now.Add(ttl)
			taken.UpdatedAt = now
			result = taken
		}
		return putJSON(tx, bucketBotVaults, k, result)
	})
	return result, err
}

// RenewBotVaultFence 见 core.BotVaultStore。
func (s *store) RenewBotVaultFence(ctx context.Context, principal, name, owner string, fence uint64, ttl time.Duration) (core.BotVaultReservation, error) {
	if principal == "" || name == "" || owner == "" || fence == 0 || ttl <= 0 {
		return core.BotVaultReservation{}, core.ErrInvalid
	}
	var result core.BotVaultReservation
	err := s.updateBotVault(ctx, func(tx storeTx) error {
		bucket := tx.Bucket(bucketBotVaults)
		if bucket == nil {
			return errSchema
		}
		existing, err := getJSON[core.BotVaultReservation](tx, bucketBotVaults, botVaultKey(principal, name))
		if err != nil {
			return err
		}
		if existing == nil || existing.State != core.BotVaultIntentPending || !existing.Holds(owner, fence) {
			return core.ErrLeaseLost
		}
		existing.LeaseExpiresAt = s.now().Add(ttl)
		existing.UpdatedAt = s.now()
		result = *existing
		return putJSON(tx, bucketBotVaults, botVaultKey(principal, name), result)
	})
	return result, err
}

// MarkBotVaultPosted 见 core.BotVaultStore。
func (s *store) MarkBotVaultPosted(ctx context.Context, principal, name, owner string, fence uint64) (core.BotVaultReservation, error) {
	if principal == "" || name == "" || owner == "" || fence == 0 {
		return core.BotVaultReservation{}, core.ErrInvalid
	}
	var result core.BotVaultReservation
	err := s.updateBotVault(ctx, func(tx storeTx) error {
		bucket := tx.Bucket(bucketBotVaults)
		if bucket == nil {
			return errSchema
		}
		existing, err := getJSON[core.BotVaultReservation](tx, bucketBotVaults, botVaultKey(principal, name))
		if err != nil {
			return err
		}
		if existing == nil || existing.State != core.BotVaultIntentPending || !existing.Holds(owner, fence) {
			return core.ErrLeaseLost
		}
		existing.PostAttempted = true
		existing.UpdatedAt = s.now()
		result = *existing
		return putJSON(tx, bucketBotVaults, botVaultKey(principal, name), result)
	})
	return result, err
}

// ConfirmBotVault 见 core.BotVaultStore。
func (s *store) ConfirmBotVault(ctx context.Context, principal, name, owner string, fence uint64, vaultID string) (core.BotVaultReservation, error) {
	if principal == "" || name == "" || owner == "" || fence == 0 || vaultID == "" {
		return core.BotVaultReservation{}, core.ErrInvalid
	}
	var result core.BotVaultReservation
	err := s.updateBotVault(ctx, func(tx storeTx) error {
		bucket := tx.Bucket(bucketBotVaults)
		if bucket == nil {
			return errSchema
		}
		existing, err := getJSON[core.BotVaultReservation](tx, bucketBotVaults, botVaultKey(principal, name))
		if err != nil {
			return err
		}
		if existing == nil || existing.Principal != principal {
			return core.ErrVaultPrincipalMismatch
		}
		if existing.State == core.BotVaultIntentConfirmed {
			if existing.VaultID == vaultID {
				result = *existing
				return nil
			}
			return core.ErrConflict
		}
		if !existing.Holds(owner, fence) {
			return core.ErrLeaseLost
		}
		now := s.now()
		existing.State = core.BotVaultIntentConfirmed
		existing.VaultID = vaultID
		existing.LeaseExpiresAt = now
		existing.UpdatedAt = now
		result = *existing
		return putJSON(tx, bucketBotVaults, botVaultKey(principal, name), result)
	})
	return result, err
}

// RejectBotVault 见 core.BotVaultStore。
func (s *store) RejectBotVault(ctx context.Context, principal, name, owner string, fence uint64) error {
	if principal == "" || name == "" || owner == "" || fence == 0 {
		return core.ErrInvalid
	}
	return s.updateBotVault(ctx, func(tx storeTx) error {
		bucket := tx.Bucket(bucketBotVaults)
		if bucket == nil {
			return errSchema
		}
		k := botVaultKey(principal, name)
		existing, err := getJSON[core.BotVaultReservation](tx, bucketBotVaults, k)
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
		return bucket.Delete(k)
	})
}

// GetBotVaultReservation 见 core.BotVaultStore。
func (s *store) GetBotVaultReservation(ctx context.Context, principal, name string) (*core.BotVaultReservation, error) {
	if principal == "" || name == "" {
		return nil, core.ErrInvalid
	}
	var result *core.BotVaultReservation
	err := s.viewBotVault(ctx, func(tx storeTx) error {
		r, err := getJSON[core.BotVaultReservation](tx, bucketBotVaults, botVaultKey(principal, name))
		if err != nil {
			return err
		}
		result = r
		return nil
	})
	return result, err
}
