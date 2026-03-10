package storage

import (
    "fmt"
    "time"
	msgpack "github.com/vmihailenco/msgpack/v5"
    bolt "go.etcd.io/bbolt"
)

func (s *Store) GC(nowMs int64, maxDeletes int) ([]string, error) {
	if nowMs == 0 {
		nowMs = time.Now().UnixMilli()
	}

	var deletedIDs []string

	err := s.db.Update(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		bb := tx.Bucket(bBody)
		bi := tx.Bucket(bIdem)
		be := tx.Bucket(bExp)
		bp := tx.Bucket(bProc)
		bf := tx.Bucket(bEnqFail)

		if bm == nil || bb == nil || bi == nil || be == nil || bp == nil || bf == nil {
			return ErrBucketMissing
		}

		c := be.Cursor()
		for k, _ := c.First(); k != nil; {
			expAt, msgID, ok := parseExpKey(k)
			if !ok {
				_ = be.Delete(k)
				k, _ = c.Next()
				continue
			}

			if expAt > nowMs {
				break
			}

			nextK, _ := c.Next()

			mv := bm.Get([]byte(msgID))
			if mv == nil {
				_ = be.Delete(k)
				k = nextK
				continue
			}

			var meta Meta
			if err := msgpack.Unmarshal(mv, &meta); err != nil {
				_ = bm.Delete([]byte(msgID))
				_ = bb.Delete([]byte(msgID))
				_ = be.Delete(k)
				deletedIDs = append(deletedIDs, msgID)
				k = nextK
				continue
			}

			if meta.Status == StatusProcessing {
				_ = be.Delete(k)
				newExp := nowMs + s.gcProcessingGraceMs
				if err := be.Put(makeExpKey(newExp, msgID), []byte(msgID)); err != nil {
					return err
				}
			} else {
				// чистим proc-индекс при финальном удалении (если известен lease)
				if meta.LeaseUntilMs > 0 {
					_ = bp.Delete(makeProcKey(meta.LeaseUntilMs, msgID))
				}

				_ = bm.Delete([]byte(msgID))
				_ = bb.Delete([]byte(msgID))
				_ = be.Delete(k)

				if meta.IdempotencyKey != "" {
					if iv := bi.Get([]byte(meta.IdempotencyKey)); iv != nil && string(iv) == msgID {
						_ = bi.Delete([]byte(meta.IdempotencyKey))
					}
				}
				deletedIDs = append(deletedIDs, msgID)
			}

			if maxDeletes > 0 && len(deletedIDs) >= maxDeletes {
				break
			}
			k = nextK
		}
		return nil
	})

	return deletedIDs, err
}

func (s *Store) RequeueStuck(nowMs int64, max int) (requeued int, err error) {
	if nowMs == 0 {
		nowMs = time.Now().UnixMilli()
	}

	err = s.db.Update(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		bp := tx.Bucket(bProc)
		be := tx.Bucket(bEnqFail)
		if bm == nil || bp == nil || be == nil {
			return ErrBucketMissing
		}

		c := bp.Cursor()

		for k, _ := c.First(); k != nil; {
			leaseUntil, msgID, ok := parseProcKey(k)
			if !ok {
				nextK, _ := c.Next()
				_ = bp.Delete(k)
				k = nextK
				continue
			}

			if leaseUntil > nowMs {
				break
			}

			nextK, _ := c.Next()

			mv := bm.Get([]byte(msgID))
			if mv == nil {
				_ = bp.Delete(k)
				k = nextK
				continue
			}

			var meta Meta
			if err := msgpack.Unmarshal(mv, &meta); err != nil {
				_ = bp.Delete(k)
				k = nextK
				continue
			}

			if meta.Status != StatusProcessing || meta.LeaseUntilMs != leaseUntil {
				_ = bp.Delete(k)
				k = nextK
				continue
			}

			// вместо "queued" переводим в enqueue_failed + индекс
			meta.Status = StatusEnqueueFailed
			meta.EnqueueFailedAtMs = nowMs
			meta.EnqueueError = "requeue: lease expired"

			meta.LastRequeueReason = RequeueLeaseExpired
			meta.LastRequeueAtMs = nowMs

			meta.StartedAtMs = 0
			meta.LeaseUntilMs = 0
			meta.UpdatedAtMs = nowMs

			buf, err := msgpack.Marshal(meta)
			if err != nil {
				return err
			}
			if err := bm.Put([]byte(msgID), buf); err != nil {
				return err
			}

			// снимаем processing индекс
			_ = bp.Delete(k)

			// пишем enqueue_failed индекс (oldest-first)
			if err := be.Put(makeEnqueueFailedKey(meta.EnqueueFailedAtMs, msgID), []byte(msgID)); err != nil {
				return err
			}

			requeued++
			if max > 0 && requeued >= max {
				break
			}

			k = nextK
		}
		return nil
	})

	return requeued, err
}

func (s *Store) RequeueForRetry(msgID string, ts int64) error {
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		be := tx.Bucket(bEnqFail)
		if bm == nil || be == nil {
			return ErrBucketMissing
		}

		v := bm.Get([]byte(msgID))
		if v == nil {
			return ErrNotFound
		}

		var meta Meta
		if err := msgpack.Unmarshal(v, &meta); err != nil {
			return fmt.Errorf("requeueforretry: unmarshal meta msg_id=%s: %w", msgID, err)
		}

		// если уже есть запись в индексе — удалим старую
		if meta.EnqueueFailedAtMs > 0 {
			_ = be.Delete(makeEnqueueFailedKey(meta.EnqueueFailedAtMs, msgID))
		}

		meta.Status = StatusEnqueueFailed
		meta.EnqueueFailedAtMs = ts
		meta.EnqueueError = "retry scheduled"

		meta.StartedAtMs = 0
		meta.LeaseUntilMs = 0
		meta.UpdatedAtMs = ts

		buf, err := msgpack.Marshal(meta)
		if err != nil {
			return fmt.Errorf("requeueforretry: marshal meta: %w", err)
		}

		if err := bm.Put([]byte(msgID), buf); err != nil {
			return fmt.Errorf("requeueforretry: put meta: %w", err)
		}

		return be.Put(makeEnqueueFailedKey(meta.EnqueueFailedAtMs, msgID), []byte(msgID))
	})
}

func (s *Store) MarkQueued(msgID string) error {
	nowMs := time.Now().UnixMilli()

	return s.db.Update(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		be := tx.Bucket(bEnqFail)
		if bm == nil || be == nil {
			return ErrBucketMissing
		}

		v := bm.Get([]byte(msgID))
		if v == nil {
			return ErrNotFound
		}

		var meta Meta
		if err := msgpack.Unmarshal(v, &meta); err != nil {
			return fmt.Errorf("markqueued: unmarshal meta msg_id=%s: %w", msgID, err)
		}

		// если уже processing или finished — не трогаем
		switch meta.Status {
		case StatusProcessing, StatusSucceeded, StatusFailed:
			return nil
		}

		// если было enqueue_failed — убрать из индекса
		if meta.EnqueueFailedAtMs > 0 {
			_ = be.Delete(makeEnqueueFailedKey(meta.EnqueueFailedAtMs, msgID))
			meta.EnqueueFailedAtMs = 0
			meta.EnqueueError = ""
		}

		meta.Status = StatusQueued
		meta.UpdatedAtMs = nowMs

		buf, err := msgpack.Marshal(meta)
		if err != nil {
			return fmt.Errorf("markqueued: marshal meta: %w", err)
		}

		return bm.Put([]byte(msgID), buf)
	})
}

func (s *Store) MarkEnqueueFailed(msgID string, reason string) error {
	nowMs := time.Now().UnixMilli()

	return s.db.Update(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		be := tx.Bucket(bEnqFail)
		if bm == nil || be == nil {
			return ErrBucketMissing
		}

		v := bm.Get([]byte(msgID))
		if v == nil {
			return ErrNotFound
		}

		var meta Meta
		if err := msgpack.Unmarshal(v, &meta); err != nil {
			return fmt.Errorf("markenqueuefailed: unmarshal meta msg_id=%s: %w", msgID, err)
		}

		// если запись уже была в индексе — удалим старую (чтобы не копить мусор)
		oldFailAt := meta.EnqueueFailedAtMs
		if oldFailAt > 0 {
			_ = be.Delete(makeEnqueueFailedKey(oldFailAt, msgID))
		}

		meta.Status = StatusEnqueueFailed
		meta.EnqueueFailedAtMs = nowMs
		meta.EnqueueError = reason
		meta.UpdatedAtMs = nowMs

		buf, err := msgpack.Marshal(meta)
		if err != nil {
			return fmt.Errorf("markenqueuefailed: marshal meta: %w", err)
		}
		if err := bm.Put([]byte(msgID), buf); err != nil {
			return fmt.Errorf("markenqueuefailed: put meta: %w", err)
		}

		return be.Put(makeEnqueueFailedKey(meta.EnqueueFailedAtMs, msgID), []byte(msgID))
	})
}

func (s *Store) MarkProcessing(msgID string, attempt int) error {
	nowMs := time.Now().UnixMilli()
	return s.db.Update(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		bp := tx.Bucket(bProc)
		if bm == nil || bp == nil {
			return ErrBucketMissing
		}

		v := bm.Get([]byte(msgID))
		if v == nil {
			return ErrNotFound
		}

		var meta Meta
		if err := msgpack.Unmarshal(v, &meta); err != nil {
			return err
		}

		if meta.Status == StatusProcessing && meta.LeaseUntilMs > nowMs {
			return fmt.Errorf("already locked")
		}

		if meta.Status == StatusProcessing && meta.LeaseUntilMs > 0 {
			_ = bp.Delete(makeProcKey(meta.LeaseUntilMs, msgID))
		}

		meta.Status = StatusProcessing
		meta.Attempt = attempt
		meta.StartedAtMs = nowMs
		meta.LeaseUntilMs = nowMs + s.processingTimeoutMs
		meta.UpdatedAtMs = nowMs
		meta.Result = Result{}

		buf, err := msgpack.Marshal(meta)
		if err != nil {
			return err
		}
		if err := bm.Put([]byte(msgID), buf); err != nil {
			return err
		}

		return bp.Put(makeProcKey(meta.LeaseUntilMs, msgID), []byte(msgID))
	})
}
func (s *Store) TouchProcessing(msgID string) error {
	nowMs := time.Now().UnixMilli()
	return s.db.Update(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		bp := tx.Bucket(bProc)
		if bm == nil || bp == nil {
			return ErrBucketMissing
		}

		v := bm.Get([]byte(msgID))
		if v == nil {
			return ErrNotFound
		}

		var meta Meta
		if err := msgpack.Unmarshal(v, &meta); err != nil {
			return err
		}
		if meta.Status != StatusProcessing {
			return nil
		}

		oldLease := meta.LeaseUntilMs
		newLease := nowMs + s.processingTimeoutMs

		if oldLease > 0 {
			_ = bp.Delete(makeProcKey(oldLease, msgID))
		}

		meta.LeaseUntilMs = newLease
		meta.UpdatedAtMs = nowMs

		buf, err := msgpack.Marshal(meta)
		if err != nil {
			return err
		}
		if err := bm.Put([]byte(msgID), buf); err != nil {
			return err
		}

		return bp.Put(makeProcKey(newLease, msgID), []byte(msgID))
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
		bp := tx.Bucket(bProc)
		if bm == nil || be == nil {
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

		if bp != nil && meta.Status == StatusProcessing && meta.LeaseUntilMs > 0 {
			_ = bp.Delete(makeProcKey(meta.LeaseUntilMs, msgID))
		}

		oldExp := meta.ExpiresAtMs
		if newExpiresAtMs != 0 {
			meta.ExpiresAtMs = newExpiresAtMs
		}

		if res.DurationMs > 0 {
			meta.ExecutedTimeMs = res.DurationMs
		} else if meta.StartedAtMs > 0 && res.FinishedAt > 0 && res.FinishedAt >= meta.StartedAtMs {
			meta.ExecutedTimeMs = res.FinishedAt - meta.StartedAtMs
		} else {
			meta.ExecutedTimeMs = 0
		}

		meta.Status = status
		meta.Result = res

		meta.StartedAtMs = 0
		meta.LeaseUntilMs = 0
		meta.UpdatedAtMs = time.Now().UnixMilli()

		buf, err := msgpack.Marshal(meta)
		if err != nil {
			return fmt.Errorf("marshal meta: %w", err)
		}
		if err := bm.Put([]byte(msgID), buf); err != nil {
			return fmt.Errorf("put meta: %w", err)
		}

		if oldExp != 0 && oldExp != meta.ExpiresAtMs {
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