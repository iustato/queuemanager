package storage

import (
	"context"
	"fmt"
	"time"

	msgpack "github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"
)

type OpenOptions struct {
	FilePath string
	Timeout  time.Duration
	NoSync   bool
    
    ProcessingTimeoutMs int64
	GCProcessingGraceMs int64
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

	// дефолты если не заданы
	if opts.ProcessingTimeoutMs == 0 {
		opts.ProcessingTimeoutMs = defaultProcessingTimeoutMs
	}
	if opts.GCProcessingGraceMs == 0 {
		opts.GCProcessingGraceMs = defaultGCProcessingGraceMs
	}

	s := &Store{
		db:                  db,
		processingTimeoutMs: opts.ProcessingTimeoutMs,
		gcProcessingGraceMs: opts.GCProcessingGraceMs,
	}

	if err := s.initBuckets(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) initBuckets() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bMeta, bBody, bIdem, bExp, bProc, bEnqFail} {
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
func (s *Store) PutNewMessage(
    ctx context.Context, // 1. Добавляем контекст
    queue, msgID string, 
    body []byte, 
    idempotencyKey string, 
    enqueuedAtMs int64, 
    expiresAtMs int64,
) (string, bool, error) {
    // Базовые проверки
    if msgID == "" {
        return "", false, fmt.Errorf("msgID is required")
    }
    
    // Проверяем контекст еще до начала транзакции
    if err := ctx.Err(); err != nil {
        return "", false, err
    }

    if enqueuedAtMs == 0 {
        enqueuedAtMs = time.Now().UnixMilli()
    }

    meta := Meta{
        Queue:          queue,
        MsgID:          msgID,
        IdempotencyKey: idempotencyKey,
        EnqueuedAtMs:   enqueuedAtMs,
        Status:         StatusCreated,
        Attempt:        0,
        ExpiresAtMs:    expiresAtMs,
    }

    metaBytes, err := msgpack.Marshal(meta)
    if err != nil {
        return "", false, fmt.Errorf("marshal meta: %w", err)
    }

    retMsgID := msgID
    created := true

    // BoltDB не поддерживает нативно контекст в методе Update,
    // поэтому мы имитируем поддержку через проверку внутри функции.
    err = s.db.Update(func(tx *bolt.Tx) error {
        // 2. Периодически проверяем, не вышел ли таймаут
        if err := ctx.Err(); err != nil {
            return err // Транзакция откатится автоматически
        }

        bm := tx.Bucket(bMeta)
        bb := tx.Bucket(bBody)
        bi := tx.Bucket(bIdem)
        be := tx.Bucket(bExp)
        if bm == nil || bb == nil || bi == nil || be == nil {
            return ErrBucketMissing
    }

        // 1) idem
        if idempotencyKey != "" {
            if v := bi.Get([]byte(idempotencyKey)); v != nil {
                retMsgID = string(v)
                created = false
                return nil
            }
        }

        // 2) защита от коллизий
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

        // 5) exp-index
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
        if b == nil {
            return ErrBucketMissing
        }
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
        if bm == nil || bb == nil {
            return ErrBucketMissing
        }

		v := bm.Get([]byte(msgID))
		if v == nil {
			return ErrNotFound
		}
		if err := msgpack.Unmarshal(v, &meta); err != nil {
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



func (s *Store) GetStatusAndResult(msgID string) (Status, Result, error) {
    var st Status
    var res Result

    err := s.db.View(func(tx *bolt.Tx) error {
        bm := tx.Bucket(bMeta)
        if bm == nil {
            return ErrBucketMissing
        }
        v := bm.Get([]byte(msgID))
        if v == nil {
            return ErrNotFound
        }

        var meta Meta
        if err := msgpack.Unmarshal(v, &meta); err != nil {
            return fmt.Errorf("unmarshal meta: %w", err)
        }

        st = meta.Status
        if st == StatusSucceeded || st == StatusFailed {
            res = meta.Result
            return nil
        }
        return ErrNotReady
    })

    return st, res, err
}








