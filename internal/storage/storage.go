package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("message already exists")
)

type Meta struct {
	Queue          string `json:"queue"`
	MsgID          string `json:"msg_id"`
	IdempotencyKey string `json:"idempotency_key"`

	EnqueuedAtMs int64  `json:"enqueued_at_ms"`
	Status       Status `json:"status"`
	Attempt      int    `json:"attempt"`

	// 0 => forever
	ExpiresAtMs int64 `json:"expires_at_ms"`
}

type Result struct {
	ExitCode   int    `json:"exit_code"`
	DurationMs int64  `json:"duration_ms"`
	Err        string `json:"err,omitempty"`
	FinishedAt int64  `json:"finished_at_ms,omitempty"`
}

type Store struct {
	db *bolt.DB
}

// Buckets (NEW MODEL)
var (
	bMeta = []byte("meta") // msg_id -> Meta(json)
	bBody = []byte("body") // msg_id -> raw payload bytes
	bIdem = []byte("idem") // idem_key -> msg_id
	bExp  = []byte("exp")  // expKey(8B expiresAtMs + msg_id) -> msg_id
)

type OpenOptions struct {
	FilePath string
	Timeout  time.Duration
	NoSync   bool
}

func Open(opts OpenOptions) (*Store, error) {
	if opts.Timeout == 0 {
		opts.Timeout = 2 * time.Second
	}
	db, err := bolt.Open(opts.FilePath, 0o600, &bolt.Options{
		Timeout: opts.Timeout,
		NoSync:  opts.NoSync,
	})
	if err != nil {
		return nil, err
	}

	s := &Store{db: db}
	if err := s.initBuckets(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) initBuckets() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bMeta, bBody, bIdem, bExp} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
}

// PutNewMessage:
// - если idemKey уже есть -> возвращает существующий msg_id (created=false)
// - иначе пишет meta + body
// - пишет idemKey -> msg_id
// - пишет exp-index (если expiresAtMs != 0)
func (s *Store) PutNewMessage(queue, msgID string, body []byte, idempotencyKey string, enqueuedAtMs int64, expiresAtMs int64) (string, bool, error) {
	if msgID == "" {
		return "", false, fmt.Errorf("msgID is required")
	}
	if enqueuedAtMs == 0 {
		enqueuedAtMs = time.Now().UnixMilli()
	}

	meta := Meta{
		Queue:          queue,
		MsgID:          msgID,
		IdempotencyKey: idempotencyKey,
		EnqueuedAtMs:   enqueuedAtMs,
		Status:         StatusQueued,
		Attempt:        0,
		ExpiresAtMs:    expiresAtMs,
	}

	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return "", false, fmt.Errorf("marshal meta: %w", err)
	}

	retMsgID := msgID
	created := true

	err = s.db.Update(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		bb := tx.Bucket(bBody)
		bi := tx.Bucket(bIdem)
		be := tx.Bucket(bExp)

		// 1) idem: если ключ уже есть — возвращаем старый msg_id
		if idempotencyKey != "" {
			if v := bi.Get([]byte(idempotencyKey)); v != nil {
				retMsgID = string(v)
				created = false
				return nil
			}
		}

		// 2) защита от коллизий msg_id
		if v := bm.Get([]byte(msgID)); v != nil {
			return ErrAlreadyExists
		}

		// 3) meta + body
		if err := bm.Put([]byte(msgID), metaBytes); err != nil {
			return fmt.Errorf("put meta: %w", err)
		}
		if err := bb.Put([]byte(msgID), body); err != nil {
			return fmt.Errorf("put body: %w", err)
		}

		// 4) idemKey -> msg_id
		if idempotencyKey != "" {
			if err := bi.Put([]byte(idempotencyKey), []byte(msgID)); err != nil {
				return fmt.Errorf("put idem: %w", err)
			}
		}

		// 5) exp-index: expKey -> msg_id
		if expiresAtMs != 0 {
			k := makeExpKey(expiresAtMs, msgID)
			if err := be.Put(k, []byte(msgID)); err != nil {
				return fmt.Errorf("put exp: %w", err)
			}
		}

		return nil
	})

	if err != nil {
		return "", false, err
	}
	return retMsgID, created, nil
}

// ResolveIdemKey returns msg_id by idem_key (if exists)
func (s *Store) ResolveIdemKey(idemKey string) (string, bool, error) {
	if idemKey == "" {
		return "", false, nil
	}

	var msgID string
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bIdem)
		v := b.Get([]byte(idemKey))
		if v == nil {
			return nil
		}
		msgID = string(v)
		return nil
	})
	if err != nil {
		return "", false, err
	}
	if msgID == "" {
		return "", false, nil
	}
	return msgID, true, nil
}

// GetByMsgID returns meta + body
func (s *Store) GetByMsgID(msgID string) (Meta, []byte, error) {
	var meta Meta
	var body []byte

	err := s.db.View(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		bb := tx.Bucket(bBody)

		v := bm.Get([]byte(msgID))
		if v == nil {
			return ErrNotFound
		}
		if err := json.Unmarshal(v, &meta); err != nil {
			return fmt.Errorf("unmarshal meta: %w", err)
		}
		b := bb.Get([]byte(msgID))
		if b != nil {
			body = append([]byte(nil), b...)
		}
		return nil
	})

	return meta, body, err
}

// MarkProcessing sets status=processing and attempt
func (s *Store) MarkProcessing(msgID string, attempt int) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		v := bm.Get([]byte(msgID))
		if v == nil {
			return ErrNotFound
		}

		var meta Meta
		if err := json.Unmarshal(v, &meta); err != nil {
			return fmt.Errorf("unmarshal meta: %w", err)
		}

		meta.Status = StatusProcessing
		meta.Attempt = attempt

		buf, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("marshal meta: %w", err)
		}
		return bm.Put([]byte(msgID), buf)
	})
}

// MarkDone sets status (succeeded/failed) and optionally updates expiresAtMs (and exp-index)
func (s *Store) MarkDone(msgID string, status Status, res Result, newExpiresAtMs int64) error {
	if status != StatusSucceeded && status != StatusFailed {
		return fmt.Errorf("invalid done status: %s", status)
	}
	if res.FinishedAt == 0 {
		res.FinishedAt = time.Now().UnixMilli()
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		be := tx.Bucket(bExp)

		v := bm.Get([]byte(msgID))
		if v == nil {
			return ErrNotFound
		}

		var meta Meta
		if err := json.Unmarshal(v, &meta); err != nil {
			return fmt.Errorf("unmarshal meta: %w", err)
		}

		oldExp := meta.ExpiresAtMs
		if newExpiresAtMs != 0 {
			meta.ExpiresAtMs = newExpiresAtMs
		}
		meta.Status = status

		buf, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("marshal meta: %w", err)
		}
		if err := bm.Put([]byte(msgID), buf); err != nil {
			return fmt.Errorf("put meta: %w", err)
		}

		// update exp-index if needed
		if oldExp != 0 {
			_ = be.Delete(makeExpKey(oldExp, msgID))
		}
		if meta.ExpiresAtMs != 0 {
			if err := be.Put(makeExpKey(meta.ExpiresAtMs, msgID), []byte(msgID)); err != nil {
				return fmt.Errorf("put exp: %w", err)
			}
		}
		return nil
	})
}

// GC deletes messages with expires_at <= now, but does NOT delete processing.
// maxDeletes = 0 => unlimited
func (s *Store) GC(nowMs int64, maxDeletes int) (deleted int, err error) {
	if nowMs == 0 {
		nowMs = time.Now().UnixMilli()
	}

	err = s.db.Update(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		bb := tx.Bucket(bBody)
		bi := tx.Bucket(bIdem)
		be := tx.Bucket(bExp)

		c := be.Cursor()
k, _ := c.First()

	for k != nil {
		nextK, _ := c.Next()

		expAt, msgID, ok := parseExpKey(k)
		if !ok {
			_ = be.Delete(k)
			k = nextK
			continue
		}

		if expAt > nowMs {
			break
		}

			mv := bm.Get([]byte(msgID))
			if mv == nil {
				_ = bb.Delete([]byte(msgID))
				_ = be.Delete(k)
				deleted++
			} else {
				var meta Meta
				if json.Unmarshal(mv, &meta) != nil {
					_ = bm.Delete([]byte(msgID))
					_ = bb.Delete([]byte(msgID))
					_ = be.Delete(k)
					deleted++
				} else if meta.Status == StatusProcessing {
					// do not delete processing; leave exp index as-is (на твой выбор)
				} else {
					_ = bm.Delete([]byte(msgID))
					_ = bb.Delete([]byte(msgID))
					_ = be.Delete(k)
					deleted++

					// delete idemKey only if it points to this msg_id
					if meta.IdempotencyKey != "" {
						if iv := bi.Get([]byte(meta.IdempotencyKey)); iv != nil && string(iv) == msgID {
							_ = bi.Delete([]byte(meta.IdempotencyKey))
						}
					}
				}
			}

			if maxDeletes > 0 && deleted >= maxDeletes {
				break
			}

				k = nextK
		}

		return nil
	})

	return deleted, err
}

// expKey: 8 bytes big-endian expiresAtMs + msg_id bytes
func makeExpKey(expiresAtMs int64, msgID string) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, uint64(expiresAtMs))
	buf.WriteString(msgID)
	return buf.Bytes()
}

func parseExpKey(k []byte) (expiresAtMs int64, msgID string, ok bool) {
	if len(k) < 8 {
		return 0, "", false
	}
	exp := int64(binary.BigEndian.Uint64(k[:8]))
	return exp, string(k[8:]), true
}
