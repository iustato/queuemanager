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
		for _, b := range [][]byte{bMeta, bBody, bExp, bProc, bEnqFail} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
}

// PutNewMessage stores a new message or detects a duplicate by MessageGUID.
// If bMeta[guid] already exists → returns (guid, false, nil) — dedup.
func (s *Store) PutNewMessage(
	ctx context.Context,
	queue, guid string,
	body []byte,
	enqueuedAtMs int64,
	expiresAtMs int64,
) (string, bool, int64, error) {
	if guid == "" {
		return "", false, 0, fmt.Errorf("MessageGUID is required")
	}

	if err := ctx.Err(); err != nil {
		return "", false, 0, err
	}

	if enqueuedAtMs == 0 {
		enqueuedAtMs = time.Now().UnixMilli()
	}

	meta := Meta{
		Queue:        queue,
		MessageGUID:  guid,
		EnqueuedAtMs: enqueuedAtMs,
		Status:       StatusCreated,
		Attempt:      0,
		ExpiresAtMs:  expiresAtMs,
	}

	metaBytes, err := msgpack.Marshal(meta)
	if err != nil {
		return "", false, 0, fmt.Errorf("marshal meta: %w", err)
	}

	created := true
	resultEnqueuedAtMs := enqueuedAtMs

	err = s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		bm := tx.Bucket(bMeta)
		bb := tx.Bucket(bBody)
		be := tx.Bucket(bExp)
		if bm == nil || bb == nil || be == nil {
			return ErrBucketMissing
		}

		// dedup: if bMeta[guid] already exists → duplicate
		if v := bm.Get([]byte(guid)); v != nil {
			created = false
			var existing Meta
			if err := msgpack.Unmarshal(v, &existing); err == nil {
				resultEnqueuedAtMs = existing.EnqueuedAtMs
			}
			return nil
		}

		// meta + body
		if err := bm.Put([]byte(guid), metaBytes); err != nil {
			return fmt.Errorf("put meta: %w", err)
		}
		if err := bb.Put([]byte(guid), body); err != nil {
			return fmt.Errorf("put body: %w", err)
		}

		// exp-index
		if expiresAtMs != 0 {
			k := makeExpKey(expiresAtMs, guid)
			if err := be.Put(k, []byte(guid)); err != nil {
				return fmt.Errorf("put exp: %w", err)
			}
		}

		return nil
	})

	if err != nil {
		return "", false, 0, err
	}
	return guid, created, resultEnqueuedAtMs, nil
}

// GetByGUID returns meta + body
func (s *Store) GetByGUID(guid string) (Meta, []byte, error) {
	var meta Meta
	var body []byte

	err := s.db.View(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		bb := tx.Bucket(bBody)
		if bm == nil || bb == nil {
			return ErrBucketMissing
		}

		v := bm.Get([]byte(guid))
		if v == nil {
			return ErrNotFound
		}
		if err := msgpack.Unmarshal(v, &meta); err != nil {
			return fmt.Errorf("unmarshal meta: %w", err)
		}
		b := bb.Get([]byte(guid))
		if b != nil {
			body = append([]byte(nil), b...)
		}
		return nil
	})

	return meta, body, err
}

func (s *Store) GetStatusAndResult(guid string) (Status, Result, error) {
	var st Status
	var res Result

	err := s.db.View(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		if bm == nil {
			return ErrBucketMissing
		}
		v := bm.Get([]byte(guid))
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
