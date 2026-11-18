// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"

	"go.etcd.io/bbolt"

	"github.com/volcengine/ark-runtime-go/channel"
	"github.com/volcengine/ark-runtime-go/channel/internal/handles"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

// 契约错误与 core 中的哨兵是同一个值，errors.Is 对两者均成立。
var (
	// errSchema 表示文件缺少当前实现要求的数据库结构。
	errSchema = storage.ErrEmbeddedSchema
	// errInvalid 表示调用参数不满足 store 契约。
	errInvalid = core.ErrInvalid
)

var (
	bucketMeta            = []byte("meta")
	bucketVersions        = []byte("binding_versions")
	bucketConversations   = []byte("conversations")
	bucketMessages        = []byte("messages")
	bucketMsgByProviderID = []byte("msg_by_provider_id")
	bucketMsgByEventID    = []byte("msg_by_event_id")
	bucketEventsByMsg     = []byte("event_ids_by_message")
	bucketTurns           = []byte("turns")
	bucketTurnReady       = []byte("turn_ready")
	bucketOpenTurns       = []byte("open_turns")
	bucketTurnsByConv     = []byte("open_turns_by_conversation")
	bucketTurnsByVersion  = []byte("turns_by_version")
	bucketSettledTurns    = []byte("settled_turns")
	bucketControls        = []byte("controls")
	bucketControlsByTime  = []byte("controls_by_time")
	bucketInteractions    = []byte("interactions")
	bucketInteractionTurn = []byte("interactions_by_turn")
	bucketDeliveries      = []byte("deliveries")
	bucketDeliveryReady   = []byte("delivery_ready")
	bucketDeliveryBySrc   = []byte("deliveries_by_source")
	bucketDeliveryUUID    = []byte("delivery_uuid")
	bucketDeliveryOpen    = []byte("open_deliveries")
	bucketDeliveryFailed  = []byte("failed_deliveries")
	keySeq                = []byte("seq")
)

var allBuckets = [][]byte{bucketPayloads, bucketPayloadRefs, bucketRemoteOperations, bucketRejected, bucketQuotas, bucketQueueDeadline, bucketHistory, bucketSnapshots, bucketBotVaults, bucketVersions, bucketConversations, bucketMessages, bucketMsgByProviderID, bucketMsgByEventID, bucketEventsByMsg, bucketTurns, bucketTurnReady, bucketOpenTurns, bucketTurnsByConv, bucketTurnsByVersion, bucketSettledTurns, bucketControls, bucketControlsByTime, bucketInteractions, bucketInteractionTurn, bucketDeliveries, bucketDeliveryReady, bucketDeliveryBySrc, bucketDeliveryUUID, bucketDeliveryOpen, bucketDeliveryFailed}

// Options 指定可信时钟和打开文件时等待独占锁的时限。
type Options struct {
	PayloadCodec             codec.Codec
	PayloadNamespace         string
	RequirePayloadEncryption bool
	// MaxChannelOpenTurns 限制同一 Channel 跨 epoch 的未结算轮次，0 关闭。
	MaxChannelOpenTurns int
	// MaxRejectedMessages 是拒绝去重记录总预算，0 关闭持久拒绝。
	MaxRejectedMessages int
	// RejectNoticeInterval 限制同会话拒绝通知频率，0 使用一分钟。
	RejectNoticeInterval time.Duration

	Clock        func() time.Time
	LockTimeout  time.Duration
	MaxOpenTurns int
	// MaxMessageBytes 是单条消息正文的字节上限，默认 32768。
	MaxMessageBytes int
	// QueueTTL 是排队时限：Turn 入队时按 received_at + QueueTTL 写入 queue_deadline；0 不限。
	QueueTTL time.Duration
	ReadOnly bool
	// NoSync 跳过每次提交的 fsync。进程或机器崩溃时可能丢失最近提交的状态，只允许用于测试，
	// 让依赖时序的用例不受磁盘刷盘延迟影响；生产配置不暴露此选项。
	NoSync bool
}

// store 是 bbolt 实现的 core.Store。
type store struct {
	pathLock             *os.File
	payloadCodec         codec.Codec
	payloadReady         bool
	payloadNamespace     string
	db                   *bbolt.DB
	clock                func() time.Time
	maxChannelOpenTurns  int
	maxRejectedMessages  int
	rejectNoticeInterval time.Duration
	maxOpenTurns         int
	maxMessageBytes      int
	queueTTL             time.Duration
}

var (
	_ core.Store    = (*store)(nil)
	_ core.Exporter = (*store)(nil)
)

// Open opens an explicitly installed database; the caller must close the handle.
func Open(path string, options Options) (*channel.Store, error) {
	s, err := open(path, options)
	if err != nil {
		return nil, err
	}
	return handles.NewStore(s), nil
}

// open 只读校验已有数据库结构与载荷策略，不创建文件或执行迁移。
func open(path string, options Options) (*store, error) {
	return openStore(path, options, false)
}

func openStore(path string, options Options, install bool) (*store, error) {
	if options.RequirePayloadEncryption && options.PayloadCodec == nil || options.PayloadCodec != nil && options.PayloadNamespace == "" || path == "" || options.LockTimeout < 0 || options.MaxChannelOpenTurns < 0 || options.MaxRejectedMessages < 0 || options.RejectNoticeInterval < 0 || options.MaxOpenTurns < 0 || options.MaxMessageBytes < 0 || options.QueueTTL < 0 {
		return nil, errInvalid
	}
	if !install {
		if _, err := os.Stat(path); err != nil {
			return nil, err
		}
	}
	if options.LockTimeout == 0 {
		options.LockTimeout = 500 * time.Millisecond
	}
	if info, err := os.Stat(path); err == nil && info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("database permissions: %w", errInvalid)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	s := &store{payloadCodec: options.PayloadCodec, payloadNamespace: options.PayloadNamespace, clock: options.Clock, maxChannelOpenTurns: options.MaxChannelOpenTurns, maxRejectedMessages: options.MaxRejectedMessages, rejectNoticeInterval: options.RejectNoticeInterval, maxOpenTurns: options.MaxOpenTurns, maxMessageBytes: options.MaxMessageBytes, queueTTL: options.QueueTTL}
	if s.clock == nil {
		s.clock = time.Now
	}
	if s.rejectNoticeInterval == 0 {
		s.rejectNoticeInterval = time.Minute
	}
	if s.maxOpenTurns == 0 {
		s.maxOpenTurns = 1000
	}
	if s.maxMessageBytes == 0 {
		s.maxMessageBytes = 32768
	}
	pathLock, err := lockStorePath(path, options.ReadOnly, options.LockTimeout)
	if err != nil {
		return nil, err
	}
	s.pathLock = pathLock
	opened := false
	defer func() {
		if !opened {
			_ = unlockStorePath(pathLock)
		}
	}()
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: options.LockTimeout, ReadOnly: options.ReadOnly, NoSync: options.NoSync, NoFreelistSync: options.NoSync})
	if err != nil {
		return nil, err
	}
	s.db = db
	if install {
		err = db.Update(func(tx *bbolt.Tx) error {
			// Install 仅初始化全新空库，不补齐或转换任何已有内容。
			if err := tx.ForEach(func([]byte, *bbolt.Bucket) error { return errSchema }); err != nil {
				return err
			}
			meta, err := tx.CreateBucket(bucketMeta)
			if err != nil {
				return err
			}
			for _, name := range append(allBuckets, bucketUserAuth) {
				if _, err := tx.CreateBucket(name); err != nil {
					return err
				}
			}
			if s.payloadCodec != nil {
				if err := meta.Put(keyPayloadNamespace, []byte(s.payloadNamespace)); err != nil {
					return err
				}
				return meta.Put(keyPayloadMode, []byte("encrypted-v1"))
			}
			return nil
		})
	} else {
		err = db.View(func(tx *bbolt.Tx) error {
			if tx.Bucket(bucketMeta) == nil {
				return errSchema
			}
			for _, name := range allBuckets {
				if tx.Bucket(name) == nil {
					return errSchema
				}
			}
			return s.checkPayloadOpen(tx)
		})
	}
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	s.payloadReady = true
	opened = true
	return s, nil
}

// Close 关闭数据文件。
func (s *store) Close() error {
	err := s.db.Close()
	lockErr := unlockStorePath(s.pathLock)
	s.pathLock = nil
	if err != nil {
		return err
	}
	return lockErr
}

func (s *store) now() time.Time { return s.clock().UTC() }

// ---------------------------------------------------------------------------
// 编码与索引键

func be64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func nanos(t time.Time) uint64 {
	if t.IsZero() || t.UnixNano() < 0 {
		return 0
	}
	return uint64(t.UnixNano())
}

func key(parts ...[]byte) []byte { return bytes.Join(parts, []byte{0}) }

func timeKey(at uint64, id string) []byte { return append(be64(at), []byte(id)...) }

func getJSON[T any](tx storeTx, bucket, id []byte) (*T, error) {
	data := tx.Bucket(bucket).Get(id)
	if data == nil {
		return nil, nil
	}
	var value T
	if err := core.DecodeStored(data, &value); err != nil {
		return nil, err
	}
	// 分页身份来自可信存储键，未知协议的业务 ID 形态不影响游标推进。
	switch record := any(&value).(type) {
	case *turnRecord:
		if record.IsOpaqueStored() {
			record.ID = string(id)
		}
	case *deliveryRecord:
		if record.IsOpaqueStored() {
			record.ID = string(id)
		}
	}
	return &value, nil
}

func putJSON(tx storeTx, bucket, id []byte, value any) error {
	if protected, ok := tx.(*payloadTx); ok {
		return protected.put(bucket, id, value)
	}
	data, err := core.EncodeStored(value)
	if err != nil {
		return err
	}
	return tx.Bucket(bucket).Put(id, data)
}

// turnRecord 是 Turn 的存储形态：附带由存储层维护的队首标记与写入时计算的可领取时间。
type turnRecord struct {
	core.Turn
	Head     bool   `json:"_head,omitempty"`
	Eligible uint64 `json:"_eligible,omitempty"`
}

// deliveryRecord 是 Delivery 的存储形态。
type deliveryRecord struct {
	core.Delivery
	Head     bool   `json:"_head,omitempty"`
	Eligible uint64 `json:"_eligible,omitempty"`
}

func getTurn(tx storeTx, id string) (*turnRecord, error) {
	return getJSON[turnRecord](tx, bucketTurns, []byte(id))
}

func getDelivery(tx storeTx, id string) (*deliveryRecord, error) {
	return getJSON[deliveryRecord](tx, bucketDeliveries, []byte(id))
}

func convKey(conversation core.RouteKey, seq uint64) []byte {
	return append(append([]byte(conversation), 0), be64(seq)...)
}

func sourcePrefix(source core.DeliverySource) []byte {
	return append([]byte(source.String()), 0)
}

func sourceKey(source core.DeliverySource, seq int) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(seq))
	return append(sourcePrefix(source), b...)
}

// writeTurn 写入 Turn 并按新旧状态差异维护全部索引。old 为 nil 表示插入。
func (s *store) writeTurn(tx storeTx, old *turnRecord, next turnRecord) error {
	if old != nil && queueExpiry(*old) != 0 {
		if err := tx.Bucket(bucketQueueDeadline).Delete(timeKey(queueExpiry(*old), old.ID)); err != nil {
			return err
		}
	}
	if at := queueExpiry(next); at != 0 {
		if err := tx.Bucket(bucketQueueDeadline).Put(timeKey(at, next.ID), []byte(next.ID)); err != nil {
			return err
		}
	}

	if old != nil && old.Head {
		if err := tx.Bucket(bucketTurnReady).Delete(timeKey(old.Eligible, old.ID)); err != nil {
			return err
		}
	}
	next.Eligible = nanos(next.EligibleAt(s.now()))
	open := next.Status.Open()
	if !open {
		next.Head = false
	}
	if next.Head {
		if err := tx.Bucket(bucketTurnReady).Put(timeKey(next.Eligible, next.ID), []byte(next.ID)); err != nil {
			return err
		}
	}
	id := []byte(next.ID)
	switch {
	case old == nil:
		if err := tx.Bucket(bucketOpenTurns).Put(id, nil); err != nil {
			return err
		}
		if err := tx.Bucket(bucketTurnsByConv).Put(convKey(next.ConversationID, next.Seq), id); err != nil {
			return err
		}
		if err := tx.Bucket(bucketTurnsByVersion).Put(key([]byte(next.BindingID), []byte(next.BindingVersion), id), nil); err != nil {
			return err
		}
	case old.Status.Open() && !open:
		if err := tx.Bucket(bucketOpenTurns).Delete(id); err != nil {
			return err
		}
		if err := tx.Bucket(bucketTurnsByConv).Delete(convKey(next.ConversationID, next.Seq)); err != nil {
			return err
		}
		if err := tx.Bucket(bucketSettledTurns).Put(timeKey(nanos(next.Outcome.SettledAt), next.ID), nil); err != nil {
			return err
		}
	}
	return putJSON(tx, bucketTurns, id, next)
}

// leasedTurn 读取 lease 当前有效的 Turn。
func (s *store) leasedTurn(tx storeTx, lease core.Lease) (*turnRecord, error) {
	record, err := getTurn(tx, lease.ID)
	if err != nil {
		return nil, err
	}
	if record == nil || !record.Holds(lease, s.now()) {
		return nil, core.ErrLeaseLost
	}
	return record, nil
}

// promoteTurnHead 让会话中 seq 最小的未结算 Turn 成为队首。
func (s *store) promoteTurnHead(tx storeTx, conversation core.RouteKey) error {
	prefix := append([]byte(conversation), 0)
	k, v := tx.Bucket(bucketTurnsByConv).Cursor().Seek(prefix)
	if k == nil || !bytes.HasPrefix(k, prefix) {
		return nil
	}
	record, err := getTurn(tx, string(v))
	if err != nil {
		return err
	}
	if record == nil {
		return core.ErrConflict
	}
	if record.Head {
		return nil
	}
	old := *record
	record.Head = true
	return s.writeTurn(tx, &old, *record)
}

// writeDelivery 写入投递并维护索引。old 为 nil 表示插入。
func (s *store) writeDelivery(tx storeTx, old *deliveryRecord, next deliveryRecord) error {
	if old != nil && old.Head {
		if err := tx.Bucket(bucketDeliveryReady).Delete(timeKey(old.Eligible, old.ID)); err != nil {
			return err
		}
	}
	next.Eligible = nanos(next.EligibleAt())
	if next.Status.Terminal() {
		next.Head = false
	}
	if next.Head {
		if err := tx.Bucket(bucketDeliveryReady).Put(timeKey(next.Eligible, next.ID), []byte(next.ID)); err != nil {
			return err
		}
	}
	id := []byte(next.ID)
	if old == nil {
		if err := tx.Bucket(bucketDeliveryBySrc).Put(sourceKey(next.Source, next.Seq), id); err != nil {
			return err
		}
		if err := tx.Bucket(bucketDeliveryUUID).Put([]byte(next.UUID), id); err != nil {
			return err
		}
	}
	if next.Status.Terminal() {
		if err := tx.Bucket(bucketDeliveryOpen).Delete(id); err != nil {
			return err
		}
	} else if err := tx.Bucket(bucketDeliveryOpen).Put(id, nil); err != nil {
		return err
	}
	if next.Status == core.DeliveryFailed {
		if err := tx.Bucket(bucketDeliveryFailed).Put(id, nil); err != nil {
			return err
		}
	} else if err := tx.Bucket(bucketDeliveryFailed).Delete(id); err != nil {
		return err
	}
	return putJSON(tx, bucketDeliveries, id, next)
}

// promoteDeliveryHead 让来源中 Seq 最小的未终态投递成为队首。
func (s *store) promoteDeliveryHead(tx storeTx, source core.DeliverySource) error {
	prefix := sourcePrefix(source)
	c := tx.Bucket(bucketDeliveryBySrc).Cursor()
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		record, err := getDelivery(tx, string(v))
		if err != nil {
			return err
		}
		if record == nil {
			return core.ErrConflict
		}
		if record.Status.Terminal() {
			continue
		}
		if record.Head {
			return nil
		}
		old := *record
		record.Head = true
		return s.writeDelivery(tx, &old, *record)
	}
	return nil
}

func (s *store) insertDeliveries(tx storeTx, source core.DeliverySource, deliveries []core.Delivery) error {
	if len(deliveries) == 0 {
		return nil
	}
	now := s.now()
	seen := map[string]bool{}
	for _, delivery := range deliveries {
		if !delivery.ValidNew(source) || seen[delivery.ID] || seen["uuid:"+delivery.UUID] || seen[fmt.Sprint("seq:", delivery.Seq)] {
			return core.ErrConflict
		}
		seen[delivery.ID], seen["uuid:"+delivery.UUID], seen[fmt.Sprint("seq:", delivery.Seq)] = true, true, true
		if tx.Bucket(bucketDeliveries).Get([]byte(delivery.ID)) != nil || tx.Bucket(bucketDeliveryUUID).Get([]byte(delivery.UUID)) != nil {
			return core.ErrConflict
		}
		delivery.Payloads = nil
		delivery.CreatedAt = now
		delivery.Lease = core.Lease{ID: delivery.ID}
		if err := s.writeDelivery(tx, nil, deliveryRecord{Delivery: delivery}); err != nil {
			return err
		}
	}
	return s.promoteDeliveryHead(tx, source)
}

func (s *store) nextSeq(tx storeTx) (uint64, error) {
	meta := tx.Bucket(bucketMeta)
	var seq uint64
	if current := meta.Get(keySeq); len(current) == 8 {
		seq = binary.BigEndian.Uint64(current)
	}
	seq++
	return seq, meta.Put(keySeq, be64(seq))
}

// ---------------------------------------------------------------------------
// IngestStore

// Ingest 在一个事务内完成去重、配置版本登记、会话创建、消息写入，以及 Turn 入队或控制命令生效。
func (s *store) Ingest(ctx context.Context, version core.BindingVersion, message core.Message) (core.IngestResult, error) {
	var result core.IngestResult
	if !version.Valid() || core.ValidateMessage(version.Content, message, s.maxMessageBytes) != nil {
		return result, core.ErrInvalid
	}
	err := s.update(ctx, func(tx storeTx) error {
		operationErr := func() error {
			if protected, ok := tx.(*payloadTx); ok {
				protected.binding = &version.Content
			}
			result = core.IngestResult{}
			now := s.now()
			if !message.AdmitUntil.IsZero() && !now.Before(message.AdmitUntil) {
				return core.ErrAdmissionExpired
			}
			if message.RecalledMessageID != "" {
				originalID := tx.Bucket(bucketMsgByProviderID).Get(key([]byte(message.BindingID), []byte(message.RecalledMessageID)))
				if originalID == nil {
					result.Ignored = true
					return nil
				}
				original, err := getJSON[core.InboundMessage](tx, bucketMessages, originalID)
				if err != nil {
					return err
				}
				if original == nil {
					return core.ErrConflict
				}
				rejected, err := s.rejectedResult(tx, *original)
				if err != nil {
					return err
				}
				if rejected != nil {
					result.Ignored = true
					return nil
				}
				message, err = core.ResolveRecall(message, *original)
				if err != nil {
					return err
				}
			}
			inbound := core.NewInboundMessage(message, now)
			inbound.BindingVersion = version.Version
			providerKey := key([]byte(inbound.BindingID), []byte(inbound.ProviderMessageID))
			eventKey := key([]byte(inbound.BindingID), []byte(inbound.ProviderEventID))
			alias := tx.Bucket(bucketMsgByEventID).Get(eventKey)
			if existingID := tx.Bucket(bucketMsgByProviderID).Get(providerKey); existingID != nil {
				existing, err := getJSON[core.InboundMessage](tx, bucketMessages, existingID)
				if err != nil {
					return err
				}
				if existing == nil || existing.ContentHash != inbound.ContentHash || alias != nil && !bytes.Equal(alias, existingID) {
					return core.ErrConflict
				}
				if alias == nil {
					r, err := s.rejectedResult(tx, *existing)
					if err != nil {
						return err
					}
					if r != nil {
						if err = s.reserveRejection(tx, *existing, r.Control.ID, r.Reply.ID, inbound.ProviderEventID, now); err != nil {
							return err
						}
					}
					if err := putEventAlias(tx, eventKey, string(existingID)); err != nil {
						return err
					}
				}
				var err2 error
				result, err2 = s.existingResult(tx, *existing)
				return err2
			}
			if alias != nil {
				return core.ErrConflict
			}
			reject := false
			if inbound.Kind == core.MessageUser {
				var err error
				reject, err = s.channelAtCapacity(tx, version.Content)
				if err != nil {
					return err
				}
				reject = reject || s.openTurnsAtLeast(tx, s.maxOpenTurns)
			}
			seq, err := s.nextSeq(tx)
			if err != nil {
				return err
			}
			versionKey := key([]byte(version.BindingID), []byte(version.Version))
			if tx.Bucket(bucketVersions).Get(versionKey) == nil {
				stored := version
				stored.CreatedAt = now
				if err := putJSON(tx, bucketVersions, versionKey, stored); err != nil {
					return err
				}
			}
			if tx.Bucket(bucketConversations).Get([]byte(inbound.ConversationID)) == nil {
				conversation := core.Conversation{ID: inbound.ConversationID, BindingID: message.BindingID, Scope: core.ConversationScope{TenantKey: message.TenantKey, ChatType: message.ChatType, ChatID: message.ChatID, ThreadID: message.ThreadID}, Session: core.SessionBinding{State: core.SessionNone}, UpdatedAt: now, Revision: 1}
				if err := putJSON(tx, bucketConversations, []byte(conversation.ID), conversation); err != nil {
					return err
				}
			}
			inbound.ID, inbound.Seq = fmt.Sprintf("msg:%020d", seq), seq
			if s.payloadCodec != nil {
				inbound.ID = payloadMessageID(inbound.BindingID, inbound.ProviderMessageID)
			}
			if err := putJSON(tx, bucketMessages, []byte(inbound.ID), inbound); err != nil {
				return err
			}
			if err := tx.Bucket(bucketMsgByProviderID).Put(providerKey, []byte(inbound.ID)); err != nil {
				return err
			}
			if err := putEventAlias(tx, eventKey, inbound.ID); err != nil {
				return err
			}
			result.Message = inbound
			if inbound.Kind == core.MessageUser {
				if reject {
					return s.persistRejected(tx, inbound, version, now, &result)
				}
				id := fmt.Sprintf("turn:%020d", seq)
				turn := core.Turn{WorkKind: core.InboundWorkKind(version.Content, message.ChatType), Snapshot: core.NewTurnSnapshot(version.Content, message.AdmitUntil), ID: id, Seq: seq, ConversationID: inbound.ConversationID, MessageID: inbound.ID, BindingID: version.BindingID, BindingVersion: version.Version, InputEventID: core.InputEventIDFor(inbound.ContentHash), CreatedAt: now, Status: core.TurnQueued, Lease: core.Lease{ID: id}, Revision: 1}
				if s.queueTTL > 0 {
					turn.QueueDeadlineAt = inbound.ReceivedAt.Add(s.queueTTL)
				}
				prefix := append([]byte(turn.ConversationID), 0)
				k, _ := tx.Bucket(bucketTurnsByConv).Cursor().Seek(prefix)
				head := k == nil || !bytes.HasPrefix(k, prefix)
				if err := s.writeTurn(tx, nil, turnRecord{Turn: turn, Head: head}); err != nil {
					return err
				}
				if message.TrackReactions {
					reaction := core.NewMessageReactionDelivery(turn, message)
					if err := s.insertDeliveries(tx, reaction.Source, []core.Delivery{reaction}); err != nil {
						return err
					}
				}
				result.Turn = &turn
				return nil
			}
			return s.applyControl(tx, inbound, version, seq, now, &result)
		}()
		if operationErr == nil {
			operationErr = s.captureIngestMetadata(tx, &result)
		}
		return operationErr
	})
	if err == nil {
		err = s.hydrateIngest(ctx, &result)
	}
	return result, err
}

// putEventAlias 记录渠道事件 ID 到消息的映射，并维护反向索引供清理时删除。
func putEventAlias(tx storeTx, eventKey []byte, messageID string) error {
	if err := tx.Bucket(bucketMsgByEventID).Put(eventKey, []byte(messageID)); err != nil {
		return err
	}
	return tx.Bucket(bucketEventsByMsg).Put(key([]byte(messageID), eventKey), nil)
}

func (s *store) openTurnsAtLeast(tx storeTx, limit int) bool {
	count := 0
	c := tx.Bucket(bucketOpenTurns).Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		if count++; count >= limit {
			return true
		}
	}
	return false
}

func (s *store) existingResult(tx storeTx, message core.InboundMessage) (core.IngestResult, error) {
	result := core.IngestResult{Message: message, Duplicate: true}
	r, err := s.rejectedResult(tx, message)
	if err != nil {
		return result, err
	}
	if r != nil {
		return *r, nil
	}
	if message.Kind == core.MessageUser {
		record, err := getTurn(tx, fmt.Sprintf("turn:%020d", message.Seq))
		if err != nil || record == nil {
			return result, err
		}
		result.Turn = &record.Turn
		return result, nil
	}
	control, err := getJSON[core.ControlRequest](tx, bucketControls, []byte(s.controlID(message)))
	if err != nil || control == nil {
		return result, err
	}
	result.Control = control
	if control.TargetTurnID != "" {
		record, err := getTurn(tx, control.TargetTurnID)
		if err != nil {
			return result, err
		}
		if record != nil {
			result.Stopped = &record.Turn
		}
	}
	return result, nil
}

// applyControl 让控制命令在入站事务内生效。
func (s *store) applyControl(tx storeTx, message core.InboundMessage, version core.BindingVersion, seq uint64, now time.Time, result *core.IngestResult) error {
	control := core.ControlRequest{ID: s.controlID(message), Seq: seq, MessageID: message.ID, ConversationID: message.ConversationID, Kind: core.ControlStop, CreatedAt: now}
	if message.Message.Unsupported {
		control.Kind = core.ControlUnsupportedMessage
	}
	if control.Kind == core.ControlStop {
		prefix := append([]byte(message.ConversationID), 0)
		if k, v := tx.Bucket(bucketTurnsByConv).Cursor().Seek(prefix); k != nil && bytes.HasPrefix(k, prefix) {
			record, err := getTurn(tx, string(v))
			if err != nil {
				return err
			}
			if record == nil {
				return core.ErrConflict
			}
			old := *record
			record.Turn = record.RequestStop(now)
			if message.Message.RecalledMessageID != "" {
				replyTarget := core.ReplyTargetFor(message.Message, version.Content)
				record.StopReplyTarget = &replyTarget
			}
			if err := s.writeTurn(tx, &old, *record); err != nil {
				return err
			}
			control.TargetTurnID = record.ID
			stopped := record.Turn
			result.Stopped = &stopped
		}
	}
	if err := putJSON(tx, bucketControls, []byte(control.ID), control); err != nil {
		return err
	}
	if err := tx.Bucket(bucketControlsByTime).Put(timeKey(nanos(now), control.ID), nil); err != nil {
		return err
	}
	result.Control = &control
	if control.TargetTurnID != "" {
		return nil
	}
	if core.ReplyTargetFor(message.Message, version.Content).Mode == "none" {
		return nil
	}
	text := core.ReplyNoActiveTurn
	if control.Kind == core.ControlUnsupportedMessage {
		text = core.ReplyUnsupportedMessage
	}
	reply := core.NewControlReply(control, message, version.Content, text)
	reply.Snapshot = core.NewTurnSnapshot(version.Content, message.Message.AdmitUntil)
	if err := s.insertDeliveries(tx, reply.Source, []core.Delivery{reply}); err != nil {
		return err
	}
	stored, err := getDelivery(tx, reply.ID)
	if err != nil || stored == nil {
		return err
	}
	result.Reply = &stored.Delivery
	return nil
}

// ---------------------------------------------------------------------------
// TurnStore

// LeaseTurn 领取就绪索引中最早的队首 Turn。
func (s *store) LeaseTurn(ctx context.Context, options core.ClaimOptions) (*core.TurnLease, error) {
	if !options.Valid() {
		return nil, core.ErrInvalid
	}
	var leased *core.TurnLease
	err := s.update(ctx, func(tx storeTx) error {
		leased = nil
		now := s.now()
		var record *turnRecord
		for _, bucket := range [][]byte{bucketQueueDeadline, bucketTurnReady} {
			c := tx.Bucket(bucket).Cursor()
			for k, v := c.First(); k != nil && binary.BigEndian.Uint64(k[:8]) <= nanos(now); k, v = c.Next() {
				ok, err := supportedTurn(tx, string(v), options.UserAuth)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
				candidate, err := getTurn(tx, string(v))
				if err != nil {
					return err
				}
				if candidate == nil || !candidate.Status.Open() {
					return core.ErrConflict
				}
				if !options.Stage.Accepts(candidate.Phase) {
					continue
				}
				if bytes.Equal(bucket, bucketQueueDeadline) {
					prefix := append([]byte(candidate.ConversationID), 0)
					hk, hid := tx.Bucket(bucketTurnsByConv).Cursor().Seek(prefix)
					if hk != nil && bytes.HasPrefix(hk, prefix) {
						supported, e := supportedTurn(tx, string(hid), options.UserAuth)
						if e != nil {
							return e
						}
						if !supported {
							continue
						}
					}
				}
				record = candidate
				break
			}
			if record != nil {
				break
			}
		}
		if record == nil {
			return nil
		}
		old := *record
		record.Turn = record.Claim(options.Owner, now, options.TTL, options.MessageDeadline)
		if old.Phase == core.PhaseNone && !old.QueueDeadlineAt.IsZero() && !now.Before(old.QueueDeadlineAt) {
			record.Schedule.StartedAt = old.Schedule.StartedAt
			record.Schedule.DeadlineAt = old.Schedule.DeadlineAt
		}
		if err := s.writeTurn(tx, &old, *record); err != nil {
			return err
		}
		message, err := getJSON[core.InboundMessage](tx, bucketMessages, []byte(record.MessageID))
		if err != nil {
			return err
		}
		version, err := getJSON[core.BindingVersion](tx, bucketVersions, key([]byte(record.BindingID), []byte(record.BindingVersion)))
		if err != nil {
			return err
		}
		conversation, err := getJSON[core.Conversation](tx, bucketConversations, []byte(record.ConversationID))
		if err != nil {
			return err
		}
		if message == nil || version == nil || conversation == nil {
			return core.ErrConflict
		}
		leased = &core.TurnLease{Turn: record.Turn, Message: *message, Binding: *version, Conversation: *conversation}
		return nil
	})
	if err == nil {
		err = s.hydrateTurnLease(ctx, leased)
	}
	return leased, err
}

func (s *store) mutateTurn(ctx context.Context, lease core.Lease, fn func(core.Turn) (core.Turn, error)) (core.Turn, error) {
	var result core.Turn
	err := s.update(ctx, func(tx storeTx) error {
		record, err := s.leasedTurn(tx, lease)
		if err != nil {
			return err
		}
		next, err := fn(record.Turn)
		if err != nil {
			return err
		}
		old := *record
		record.Turn = next
		result = next
		if err := s.writeTurn(tx, &old, *record); err != nil {
			return err
		}
		return nil
	})
	return result, err
}

// RenewTurn 使用当前 fence 延长租约。
func (s *store) RenewTurn(ctx context.Context, lease core.Lease, ttl time.Duration) (core.Lease, error) {
	if ttl <= 0 {
		return core.Lease{}, core.ErrInvalid
	}
	turn, err := s.mutateTurn(ctx, lease, func(turn core.Turn) (core.Turn, error) {
		turn.Lease.ExpiresAt = s.now().Add(ttl)
		turn.Revision++
		return turn, nil
	})
	return turn.Lease, err
}

// ReleaseTurn 不改变进度地交还租约。
func (s *store) ReleaseTurn(ctx context.Context, lease core.Lease) error {
	_, err := s.mutateTurn(ctx, lease, func(turn core.Turn) (core.Turn, error) { return turn.Release(), nil })
	return err
}

// UpdateTurn 按迁移规则推进 Turn。
func (s *store) UpdateTurn(ctx context.Context, lease core.Lease, update core.TurnUpdate) (core.Turn, error) {
	return s.updateRemoteTurn(ctx, lease, update)
}

// WaitTurn 释放租约，到点后重新可领取。
func (s *store) WaitTurn(ctx context.Context, lease core.Lease, wait core.TurnWait) error {
	_, err := s.mutateTurn(ctx, lease, func(turn core.Turn) (core.Turn, error) { return turn.Wait(wait) })
	return err
}

// SettleTurn 把终态、投递、交互过期与会话队首推进在一个事务内提交。
func (s *store) SettleTurn(ctx context.Context, lease core.Lease, outcome core.TurnOutcome, deliveries []core.Delivery) error {
	if !outcome.Status.Terminal() {
		return core.ErrInvalid
	}
	return s.update(ctx, func(tx storeTx) error {
		return s.settleUserTurn(tx, lease, outcome, deliveries)
	})
}

func (s *store) settleUserTurn(tx storeTx, lease core.Lease, outcome core.TurnOutcome, deliveries []core.Delivery) error {
	record, err := s.leasedTurn(tx, lease)
	if err != nil {
		return err
	}
	now := s.now()
	settled, err := record.Settle(outcome, now)
	if err != nil {
		return err
	}
	if record.WorkKind == core.WorkKindUserAuth {
		deliveries = append([]core.Delivery(nil), deliveries...)
		for i := range deliveries {
			deliveries[i].Snapshot = record.Snapshot
		}
	}
	for _, delivery := range deliveries {
		if delivery.BindingID != record.BindingID {
			return core.ErrConflict
		}
	}
	deliveries = core.ApplyStopReplyTarget(record.Turn, deliveries)
	replies, progress, err := core.SplitTurnDeliveries(record.ID, deliveries)
	if err != nil {
		return err
	}
	if err := s.insertDeliveries(tx, core.DeliverySource{Kind: core.SourceTurn, ID: record.ID}, replies); err != nil {
		return err
	}
	if err := s.insertDeliveries(tx, core.DeliverySource{Kind: core.SourceProgress, ID: record.ID}, progress); err != nil {
		return err
	}
	prefix := append([]byte(record.ID), 0)
	c := tx.Bucket(bucketInteractionTurn).Cursor()
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		interaction, err := getJSON[core.Interaction](tx, bucketInteractions, k[len(prefix):])
		if err != nil {
			return err
		}
		if interaction == nil {
			continue
		}
		if expired, changed := interaction.Expire(now); changed {
			if err := putJSON(tx, bucketInteractions, []byte(expired.ID), expired); err != nil {
				return err
			}
		}
	}
	old := *record
	record.Turn = settled
	if err := s.writeTurn(tx, &old, *record); err != nil {
		return err
	}
	return s.promoteTurnHead(tx, record.ConversationID)
}

// GetTurn 读取 Turn 的最新状态。
func (s *store) GetTurn(ctx context.Context, id string) (*core.Turn, error) {
	var result *core.Turn
	err := s.view(ctx, func(tx storeTx) error {
		record, err := getTurn(tx, id)
		if record != nil {
			result = &record.Turn
		}
		return err
	})
	if err == nil {
		err = s.hydrate(ctx, result)
	}
	return result, err
}

// ---------------------------------------------------------------------------
// ConversationStore

// GetConversation 读取会话。
func (s *store) GetConversation(ctx context.Context, id core.RouteKey) (*core.Conversation, error) {
	var result *core.Conversation
	err := s.view(ctx, func(tx storeTx) error {
		var err error
		result, err = getJSON[core.Conversation](tx, bucketConversations, []byte(id))
		return err
	})
	return result, err
}

// BindSession 把持有租约的 Turn 所在会话绑定到已确认的 Session。
func (s *store) BindSession(ctx context.Context, lease core.Lease, binding core.SessionBinding) (core.Conversation, error) {
	var result core.Conversation
	err := s.update(ctx, func(tx storeTx) error {
		record, err := s.leasedTurn(tx, lease)
		if err != nil {
			return err
		}
		if binding.BindingVersion != record.BindingVersion {
			return core.ErrInvalid
		}
		conversation, err := getJSON[core.Conversation](tx, bucketConversations, []byte(record.ConversationID))
		if err != nil {
			return err
		}
		if conversation == nil {
			return core.ErrConflict
		}
		if err := conversation.Session.CheckBind(binding); err != nil {
			return err
		}
		if conversation.Session != binding {
			conversation.Session, conversation.UpdatedAt = binding, s.now()
			conversation.Revision++
			if err := putJSON(tx, bucketConversations, []byte(conversation.ID), conversation); err != nil {
				return err
			}
		}
		result = *conversation
		remoteTurn := record.Turn
		remoteTurn.Session.Generation = binding.Generation
		remoteTurn.Session.SessionID = binding.SessionID
		if err := s.applyRemoteTurn(tx, remoteTurn, core.TurnUpdate{RemoteKind: core.RemoteCreate, RemoteProof: &core.RemoteProof{Kind: core.RemoteCreated, Reference: binding.SessionID}}); err != nil {
			return err
		}
		if err := s.recordHistory(tx, record.ConversationID, binding); err != nil {
			return err
		}
		return nil
	})
	return result, err
}

// ResetConversation 让会话的下一条消息新建 Session。
func (s *store) ResetConversation(ctx context.Context, id core.RouteKey) (*core.Conversation, error) {
	var result *core.Conversation
	err := s.update(ctx, func(tx storeTx) error {
		conversation, err := getJSON[core.Conversation](tx, bucketConversations, []byte(id))
		if err != nil || conversation == nil {
			result = nil
			return err
		}
		if next := conversation.Session.Reset(); next != conversation.Session {
			conversation.Session, conversation.UpdatedAt = next, s.now()
			conversation.Revision++
			if err := putJSON(tx, bucketConversations, []byte(conversation.ID), conversation); err != nil {
				return err
			}
		}
		result = conversation
		return nil
	})
	return result, err
}

// ---------------------------------------------------------------------------
// InteractionStore

// PutInteraction 幂等登记交互。
func (s *store) PutInteraction(ctx context.Context, lease core.Lease, interaction core.Interaction) (core.Interaction, error) {
	var result core.Interaction
	if !interaction.ValidNew() {
		return result, core.ErrInvalid
	}
	var original *core.Interaction
	if s.payloadCodec != nil {
		var err error
		original, err = s.GetInteraction(ctx, interaction.ID)
		if err != nil {
			return result, err
		}
	}
	err := s.update(ctx, func(tx storeTx) error {
		result = core.Interaction{}
		record, err := s.leasedTurn(tx, lease)
		if err != nil {
			return err
		}
		if interaction.TurnID != record.ID {
			return core.ErrInvalid
		}
		existing, err := getJSON[core.Interaction](tx, bucketInteractions, []byte(interaction.ID))
		if err != nil {
			return err
		}
		if existing == nil {
			interaction.Payloads = nil
			result = interaction
			if err := tx.Bucket(bucketInteractionTurn).Put(key([]byte(interaction.TurnID), []byte(interaction.ID)), nil); err != nil {
				return err
			}
			return putJSON(tx, bucketInteractions, []byte(interaction.ID), interaction)
		}
		if s.payloadCodec != nil {
			if err = restoreInteraction(existing, original); err != nil {
				return err
			}
		}
		merged, changed, err := existing.Register(interaction)
		if err != nil {
			return err
		}
		result = merged
		if changed {
			return putJSON(tx, bucketInteractions, []byte(merged.ID), merged)
		}
		return nil
	})
	if err == nil && s.payloadCodec != nil {
		stored, readErr := s.GetInteraction(ctx, interaction.ID)
		if readErr != nil {
			return core.Interaction{}, readErr
		}
		if stored == nil {
			return core.Interaction{}, core.ErrConflict
		}
		result = *stored
	}
	return result, err
}

// DecideInteraction 以 CAS 方式决定交互。
func (s *store) DecideInteraction(ctx context.Context, id string, decision core.InteractionDecision) (core.Interaction, error) {
	var result core.Interaction
	var decideErr error
	var original *core.Interaction
	if s.payloadCodec != nil {
		var err error
		original, err = s.GetInteraction(ctx, id)
		if err != nil {
			return result, err
		}
	}
	err := s.update(ctx, func(tx storeTx) error {
		existing, err := getJSON[core.Interaction](tx, bucketInteractions, []byte(id))
		if err != nil {
			return err
		}
		if existing == nil {
			return core.ErrNotFound
		}
		if s.payloadCodec != nil {
			if err = restoreInteraction(existing, original); err != nil {
				return err
			}
		}
		decided, changed, err := existing.Decide(decision, s.now())
		result, decideErr = decided, err
		if changed {
			return putJSON(tx, bucketInteractions, []byte(id), decided)
		}
		return nil
	})
	if err != nil {
		return core.Interaction{}, err
	}
	if s.payloadCodec != nil {
		stored, err := s.GetInteraction(ctx, id)
		if err != nil {
			return core.Interaction{}, err
		}
		if stored == nil {
			return core.Interaction{}, core.ErrConflict
		}
		result = *stored
	}
	return result, decideErr
}

// GetInteraction 读取交互。
func (s *store) GetInteraction(ctx context.Context, id string) (*core.Interaction, error) {
	var result *core.Interaction
	err := s.view(ctx, func(tx storeTx) error {
		var err error
		result, err = getJSON[core.Interaction](tx, bucketInteractions, []byte(id))
		return err
	})
	if err == nil && result != nil {
		err = s.hydrate(ctx, result)
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// DeliveryStore

// LeaseDelivery 领取就绪索引中最早的来源队首投递。
func (s *store) LeaseDelivery(ctx context.Context, owner string, ttl time.Duration) (*core.Delivery, error) {
	if owner == "" || ttl <= 0 {
		return nil, core.ErrInvalid
	}
	if s.payloadCodec != nil {
		return s.leaseEncryptedDelivery(ctx, owner, ttl)
	}
	var claimed *core.Delivery
	err := s.update(ctx, func(tx storeTx) error {
		claimed = nil
		now := s.now()
		c := tx.Bucket(bucketDeliveryReady).Cursor()
		var record *deliveryRecord
		for k, v := c.First(); k != nil && binary.BigEndian.Uint64(k[:8]) <= nanos(now); k, v = c.Next() {
			ok, e := supportedDelivery(tx, string(v))
			if e != nil {
				return e
			}
			if !ok {
				continue
			}
			r, e := getDelivery(tx, string(v))
			if e != nil {
				return e
			}
			if r == nil || r.Status.Terminal() || !r.Head {
				return core.ErrConflict
			}
			record = r
			break
		}
		if record == nil {
			return nil
		}
		old := *record
		record.Delivery = record.Claim(owner, now, ttl)
		next := record.Delivery
		claimed = &next
		return s.writeDelivery(tx, &old, *record)
	})
	if err == nil {
		err = s.hydrate(ctx, claimed)
	}
	return claimed, err
}

// RenewDelivery 使用当前 fence 核验并延长投递租约。
func (s *store) RenewDelivery(ctx context.Context, lease core.Lease, ttl time.Duration) (core.Lease, error) {
	if ttl <= 0 {
		return core.Lease{}, core.ErrInvalid
	}
	var renewed core.Lease
	err := s.update(ctx, func(tx storeTx) error {
		record, err := getDelivery(tx, lease.ID)
		if err != nil {
			return err
		}
		if record == nil || !record.Holds(lease, s.now()) {
			return core.ErrLeaseLost
		}
		old := *record
		record.Lease.ExpiresAt = s.now().Add(ttl)
		renewed = record.Lease
		return s.writeDelivery(tx, &old, *record)
	})
	return renewed, err
}

// CompleteDelivery 保存一次投递结果。
func (s *store) CompleteDelivery(ctx context.Context, lease core.Lease, result core.DeliveryResult) error {
	return s.update(ctx, func(tx storeTx) error {
		record, err := getDelivery(tx, lease.ID)
		if err != nil {
			return err
		}
		if record == nil || !record.Holds(lease, s.now()) {
			return core.ErrLeaseLost
		}
		completed, err := record.Complete(result)
		if err != nil {
			return err
		}
		old := *record
		record.Delivery = completed
		if err := s.writeDelivery(tx, &old, *record); err != nil {
			return err
		}
		if completed.Status.Terminal() {
			return s.promoteDeliveryHead(tx, completed.Source)
		}
		return nil
	})
}

// RetryDelivery 把 failed 投递恢复为 pending。
func (s *store) RetryDelivery(ctx context.Context, id string) (*core.Delivery, error) {
	var result *core.Delivery
	err := s.update(ctx, func(tx storeTx) error {
		record, err := getDelivery(tx, id)
		if err != nil {
			return err
		}
		if record == nil {
			return core.ErrNotFound
		}
		retried, err := record.Retry()
		if err != nil {
			return err
		}
		old := *record
		record.Delivery = retried
		if err := s.writeDelivery(tx, &old, *record); err != nil {
			return err
		}
		result = &retried
		return s.promoteDeliveryHead(tx, retried.Source)
	})
	if err == nil {
		err = s.hydrate(ctx, result)
	}
	return result, err
}

// ---------------------------------------------------------------------------
// AdminStore

// Stats 遍历未结算与未终态索引汇总队列状态。
func (s *store) Stats(ctx context.Context) (core.QueueStats, error) {
	var stats core.QueueStats
	err := s.view(ctx, func(tx storeTx) error {
		stats = core.QueueStats{}
		c := tx.Bucket(bucketOpenTurns).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			record, err := getTurn(tx, string(k))
			if err != nil {
				return err
			}
			if record == nil {
				continue
			}
			switch record.Status {
			case core.TurnQueued:
				stats.QueuedTurns++
			case core.TurnRunning:
				stats.RunningTurns++
			case core.TurnWaiting:
				stats.WaitingTurns++
			}
			if stats.OldestOpenTurnAt.IsZero() || record.CreatedAt.Before(stats.OldestOpenTurnAt) {
				stats.OldestOpenTurnAt = record.CreatedAt
			}
		}
		stats.OpenDeliveries = countKeys(tx.Bucket(bucketDeliveryOpen))
		stats.FailedDeliveries = countKeys(tx.Bucket(bucketDeliveryFailed))
		return nil
	})
	return stats, err
}

func countKeys(bucket *bbolt.Bucket) int {
	count := 0
	c := bucket.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		count++
	}
	return count
}

// pageIDs 从 index（键为实体 ID）中按 ID 升序取 after 之后满足 keep 的最多 limit 个实体。
func pageIDs[T any](tx storeTx, index, entities []byte, after string, limit int, keep func(T) bool) ([]T, error) {
	var result []T
	c := tx.Bucket(index).Cursor()
	k, _ := c.Seek([]byte(after))
	if k != nil && string(k) == after {
		k, _ = c.Next()
	}
	for ; k != nil && len(result) < limit; k, _ = c.Next() {
		value, err := getJSON[T](tx, entities, k)
		if err != nil {
			return nil, err
		}
		if value != nil && keep(*value) {
			result = append(result, *value)
		}
	}
	return result, nil
}

// ListTurns 分页列出 Turn。
func (s *store) ListTurns(ctx context.Context, query core.TurnQuery) ([]core.Turn, error) {
	limit, err := core.PageLimit(query.Limit)
	if err != nil {
		return nil, err
	}
	var result []core.Turn
	err = s.view(ctx, func(tx storeTx) error {
		index := bucketTurns
		if query.OpenOnly {
			index = bucketOpenTurns
		}
		records, err := pageIDs(tx, index, bucketTurns, query.After, limit, func(r turnRecord) bool {
			return query.ConversationID == "" || r.ConversationID == query.ConversationID
		})
		for _, record := range records {
			result = append(result, record.Turn)
		}
		return err
	})
	if err == nil {
		for i := range result {
			if err = s.hydrate(ctx, &result[i]); err != nil {
				break
			}
		}
	}
	return result, err
}

// ListDeliveries 分页列出投递。
func (s *store) ListDeliveries(ctx context.Context, query core.DeliveryQuery) ([]core.Delivery, error) {
	limit, err := core.PageLimit(query.Limit)
	if err != nil {
		return nil, err
	}
	var result []core.Delivery
	err = s.view(ctx, func(tx storeTx) error {
		index := bucketDeliveries
		switch {
		case query.OpenOnly:
			index = bucketDeliveryOpen
		case query.Status == core.DeliveryFailed:
			index = bucketDeliveryFailed
		}
		records, err := pageIDs(tx, index, bucketDeliveries, query.After, limit, func(r deliveryRecord) bool {
			return query.Status == "" || r.Status == query.Status
		})
		for _, record := range records {
			result = append(result, record.Delivery)
		}
		return err
	})
	if err == nil {
		for i := range result {
			if err = s.hydrate(ctx, &result[i]); err != nil {
				break
			}
		}
	}
	return result, err
}

// ListConversations 分页列出会话。
func (s *store) ListConversations(ctx context.Context, query core.ConversationQuery) ([]core.Conversation, error) {
	limit, err := core.PageLimit(query.Limit)
	if err != nil {
		return nil, err
	}
	var result []core.Conversation
	err = s.view(ctx, func(tx storeTx) error {
		var err error
		result, err = pageIDs(tx, bucketConversations, bucketConversations, query.After, limit, func(core.Conversation) bool { return true })
		return err
	})
	return result, err
}

// ListBindingVersions 返回全部配置版本。
func (s *store) ListBindingVersions(ctx context.Context) ([]core.BindingVersion, error) {
	var result []core.BindingVersion
	err := s.view(ctx, func(tx storeTx) error {
		return tx.Bucket(bucketVersions).ForEach(func(_, v []byte) error {
			var version core.BindingVersion
			if err := core.DecodeStored(v, &version); err != nil {
				return err
			}
			result = append(result, version)
			return nil
		})
	})
	return result, err
}

// sourceOpen 判断来源是否还有未终态投递。
func sourceOpen(tx storeTx, source core.DeliverySource) (bool, error) {
	if checked, ok := tx.(*pruneCheckedTx); ok {
		if pinned, err := checked.pinsSource(source); err != nil || pinned {
			return pinned, err
		}
	}
	if pinned, err := unsupportedSource(tx, source); err != nil || pinned {
		return pinned, err
	}
	prefix := sourcePrefix(source)
	c := tx.Bucket(bucketDeliveryBySrc).Cursor()
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if tx.Bucket(bucketDeliveryOpen).Get(v) != nil {
			return true, nil
		}
	}
	return false, nil
}

// deleteSource 删除来源的全部投递与入站消息（连同去重索引），返回删除的投递数。
func deleteSource(tx storeTx, source core.DeliverySource, messageID string) (int, error) {
	prefix := sourcePrefix(source)
	c := tx.Bucket(bucketDeliveryBySrc).Cursor()
	var keys, ids [][]byte
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		keys, ids = append(keys, append([]byte(nil), k...)), append(ids, append([]byte(nil), v...))
	}
	for i, id := range ids {
		record, err := getDelivery(tx, string(id))
		if err != nil {
			return 0, err
		}
		if record != nil {
			if err := tx.Bucket(bucketDeliveryUUID).Delete([]byte(record.UUID)); err != nil {
				return 0, err
			}
		}
		for _, bucket := range [][]byte{bucketDeliveries, bucketDeliveryFailed, bucketDeliveryOpen} {
			if err := tx.Bucket(bucket).Delete(id); err != nil {
				return 0, err
			}
		}
		if err := tx.Bucket(bucketDeliveryBySrc).Delete(keys[i]); err != nil {
			return 0, err
		}
	}
	message, err := getJSON[core.InboundMessage](tx, bucketMessages, []byte(messageID))
	if err != nil {
		return 0, err
	}
	if message != nil {
		if err := tx.Bucket(bucketMsgByProviderID).Delete(key([]byte(message.BindingID), []byte(message.ProviderMessageID))); err != nil {
			return 0, err
		}
		// 全部事件 ID 别名都指向该消息，按反向索引删除。
		prefix := append([]byte(messageID), 0)
		var aliases [][]byte
		ec := tx.Bucket(bucketEventsByMsg).Cursor()
		for k, _ := ec.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = ec.Next() {
			aliases = append(aliases, append([]byte(nil), k...))
		}
		for _, alias := range aliases {
			if err := tx.Bucket(bucketMsgByEventID).Delete(alias[len(prefix):]); err != nil {
				return 0, err
			}
			if err := tx.Bucket(bucketEventsByMsg).Delete(alias); err != nil {
				return 0, err
			}
		}
		if err := tx.Bucket(bucketMessages).Delete([]byte(messageID)); err != nil {
			return 0, err
		}
	}
	return len(ids), nil
}

// Prune 按结算时间索引删除过期的已结算记录及其关联数据。
func (s *store) Prune(ctx context.Context, before time.Time) (core.PruneResult, error) {
	var removed core.PruneResult
	proof, err := s.inspectPrune(ctx, before)
	if err != nil {
		return removed, err
	}
	err = s.update(ctx, func(tx storeTx) error {
		if proof != nil {
			tx = &pruneCheckedTx{storeTx: tx, proof: proof}
		}
		removed = core.PruneResult{}
		cutoff := nanos(before)
		var settledKeys [][]byte
		c := tx.Bucket(bucketSettledTurns).Cursor()
		for k, _ := c.First(); k != nil && binary.BigEndian.Uint64(k[:8]) < cutoff; k, _ = c.Next() {
			settledKeys = append(settledKeys, append([]byte(nil), k...))
		}
		for _, settledKey := range settledKeys {
			id := string(settledKey[8:])
			record, err := getTurn(tx, id)
			if err != nil {
				return err
			}
			source := core.DeliverySource{Kind: core.SourceTurn, ID: id}
			if record == nil {
				continue
			}
			supported, e := supportedTurn(tx, id)
			if e != nil {
				return e
			}
			if !supported {
				continue
			}
			if s.payloadCodec != nil {
				if _, err := payload.References(s.payloadNamespace, "turn", id, tx.Bucket(bucketTurns).Get([]byte(id))); err != nil {
					return err
				}
			}
			if supported, err := s.pruneChildrenSupported(tx, record.MessageID, record.ID); err != nil {
				return err
			} else if !supported {
				continue
			}
			if pinned, err := remoteTurnPinned(tx, id); err != nil {
				return err
			} else if pinned {
				continue
			}
			if open, err := sourceOpen(tx, source); err != nil || open {
				if err != nil {
					return err
				}
				continue
			}
			progressSource := core.DeliverySource{Kind: core.SourceProgress, ID: id}
			if open, err := sourceOpen(tx, progressSource); err != nil || open {
				if err != nil {
					return err
				}
				continue
			}
			reactionSource := core.DeliverySource{Kind: core.SourceReaction, ID: id}
			if open, err := sourceOpen(tx, reactionSource); err != nil || open {
				if err != nil {
					return err
				}
				continue
			}
			reactionDeleted, err := deleteSource(tx, reactionSource, "")
			if err != nil {
				return err
			}
			progressDeleted, err := deleteSource(tx, progressSource, "")
			if err != nil {
				return err
			}
			n, err := deleteSource(tx, source, record.MessageID)
			if err != nil {
				return err
			}
			removed.Deliveries += n + progressDeleted + reactionDeleted
			prefix := append([]byte(id), 0)
			var interactionKeys [][]byte
			ic := tx.Bucket(bucketInteractionTurn).Cursor()
			for k, _ := ic.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = ic.Next() {
				interactionKeys = append(interactionKeys, append([]byte(nil), k...))
			}
			for _, k := range interactionKeys {
				if err := tx.Bucket(bucketInteractions).Delete(k[len(prefix):]); err != nil {
					return err
				}
				if err := tx.Bucket(bucketInteractionTurn).Delete(k); err != nil {
					return err
				}
				removed.Interactions++
			}
			if err := tx.Bucket(bucketTurnsByVersion).Delete(key([]byte(record.BindingID), []byte(record.BindingVersion), []byte(id))); err != nil {
				return err
			}
			if err := tx.Bucket(bucketTurns).Delete([]byte(id)); err != nil {
				return err
			}
			if err := tx.Bucket(bucketSettledTurns).Delete(settledKey); err != nil {
				return err
			}
			removed.Turns++
			removed.Messages++
		}
		var controlKeys [][]byte
		cc := tx.Bucket(bucketControlsByTime).Cursor()
		for k, _ := cc.First(); k != nil && binary.BigEndian.Uint64(k[:8]) < cutoff; k, _ = cc.Next() {
			controlKeys = append(controlKeys, append([]byte(nil), k...))
		}
		for _, controlKey := range controlKeys {
			id := string(controlKey[8:])
			control, err := getJSON[core.ControlRequest](tx, bucketControls, []byte(id))
			if err != nil {
				return err
			}
			source := core.DeliverySource{Kind: core.SourceControl, ID: id}
			if control == nil {
				continue
			}
			if supported, err := legacyRecord(tx, bucketControls, []byte(id)); err != nil {
				return err
			} else if !supported {
				continue
			}
			if supported, err := s.pruneChildrenSupported(tx, control.MessageID, ""); err != nil {
				return err
			} else if !supported {
				continue
			}
			pinned, err := recallSourcePinned(tx, control.MessageID)
			if err != nil {
				return err
			}
			if pinned {
				continue
			}
			if open, err := sourceOpen(tx, source); err != nil || open {
				if err != nil {
					return err
				}
				continue
			}
			if control.Kind == core.RejectControlKind {
				ok, err := s.pruneRejectedSource(tx, control.ID, cutoff, &removed)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
			}
			n, err := deleteSource(tx, source, control.MessageID)
			if err != nil {
				return err
			}
			removed.Deliveries += n
			if err := tx.Bucket(bucketControls).Delete([]byte(id)); err != nil {
				return err
			}
			if err := tx.Bucket(bucketControlsByTime).Delete(controlKey); err != nil {
				return err
			}
			removed.Controls++
			removed.Messages++
		}
		if err := s.pruneVersions(tx, cutoff, &removed); err != nil {
			return err
		}
		if s.payloadCodec != nil {
			return s.prunePayloads(tx)
		}
		return nil
	})
	if errors.Is(err, core.ErrUnsupportedWork) {
		return core.PruneResult{}, nil // Opaque roots pin collection; the transaction rolled back.
	}
	return removed, err
}

// pruneVersions 删除 cutoff 之前创建、且不再被任何 Turn 或会话引用的配置版本。
func (s *store) pruneVersions(tx storeTx, cutoff uint64, removed *core.PruneResult) error {
	if s.payloadCodec != nil {
		return nil
	} // 加密记录的来源证明仍可能引用旧版本，首版保守保留。
	referenced := map[string]bool{}
	if err := tx.Bucket(bucketConversations).ForEach(func(_, v []byte) error {
		var conversation core.Conversation
		if err := core.DecodeStored(v, &conversation); err != nil {
			return err
		}
		referenced[conversation.Session.BindingVersion] = true
		return nil
	}); err != nil {
		return err
	}
	var stale [][]byte
	if err := tx.Bucket(bucketVersions).ForEach(func(k, v []byte) error {
		var version core.BindingVersion
		if err := core.DecodeStored(v, &version); err != nil {
			return err
		}
		if nanos(version.CreatedAt) >= cutoff || referenced[version.Version] {
			return nil
		}
		prefix := append(append([]byte(nil), k...), 0)
		if ref, _ := tx.Bucket(bucketTurnsByVersion).Cursor().Seek(prefix); ref != nil && bytes.HasPrefix(ref, prefix) {
			return nil
		}
		stale = append(stale, append([]byte(nil), k...))
		return nil
	}); err != nil {
		return err
	}
	for _, k := range stale {
		if err := tx.Bucket(bucketVersions).Delete(k); err != nil {
			return err
		}
		removed.BindingVersions++
	}
	return nil
}

// Export 读取全部内容副本，只用于测试。
func (s *store) Export(ctx context.Context) (core.Export, error) {
	var export core.Export
	err := s.view(ctx, func(tx storeTx) error {
		export = core.Export{}
		each := func(bucket []byte, fn func([]byte) error) error {
			return tx.Bucket(bucket).ForEach(func(_, v []byte) error { return fn(v) })
		}
		if err := each(bucketVersions, func(v []byte) error { return appendJSON(v, &export.BindingVersions) }); err != nil {
			return err
		}
		if err := each(bucketConversations, func(v []byte) error { return appendJSON(v, &export.Conversations) }); err != nil {
			return err
		}
		if err := each(bucketMessages, func(v []byte) error { return appendJSON(v, &export.Messages) }); err != nil {
			return err
		}
		if err := each(bucketTurns, func(v []byte) error {
			var record turnRecord
			if err := core.DecodeStored(v, &record); err != nil {
				return err
			}
			export.Turns = append(export.Turns, record.Turn)
			return nil
		}); err != nil {
			return err
		}
		if err := each(bucketControls, func(v []byte) error { return appendJSON(v, &export.Controls) }); err != nil {
			return err
		}
		if err := each(bucketInteractions, func(v []byte) error { return appendJSON(v, &export.Interactions) }); err != nil {
			return err
		}
		return each(bucketDeliveries, func(v []byte) error {
			var record deliveryRecord
			if err := core.DecodeStored(v, &record); err != nil {
				return err
			}
			export.Deliveries = append(export.Deliveries, record.Delivery)
			return nil
		})
	})
	if err == nil {
		for i := range export.Messages {
			if err = s.hydrate(ctx, &export.Messages[i]); err != nil {
				break
			}
		}
	}
	if err == nil {
		for i := range export.Turns {
			if err = s.hydrate(ctx, &export.Turns[i]); err != nil {
				break
			}
		}
	}
	if err == nil {
		for i := range export.Interactions {
			if err = s.hydrate(ctx, &export.Interactions[i]); err != nil {
				break
			}
		}
	}
	if err == nil {
		for i := range export.Deliveries {
			if err = s.hydrate(ctx, &export.Deliveries[i]); err != nil {
				break
			}
		}
	}
	if err != nil {
		return core.Export{}, err
	}
	return export, nil
}

func appendJSON[T any](data []byte, target *[]T) error {
	var value T
	if err := core.DecodeStored(data, &value); err != nil {
		return err
	}
	*target = append(*target, value)
	return nil
}

var bucketQueueDeadline = []byte("queue_deadline")

func queueExpiry(t turnRecord) uint64 {
	if !t.Status.Open() || t.Phase != core.PhaseNone || t.QueueDeadlineAt.IsZero() {
		return 0
	}
	at := t.QueueDeadlineAt
	if t.Lease.ExpiresAt.After(at) {
		at = t.Lease.ExpiresAt
	}
	return nanos(at)
}
