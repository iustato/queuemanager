package storage

import (
	"context"
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
	Result Result `json:"result,omitempty"`

		// для метрик и разборов
	LastRequeueReason RequeueReason `json:"last_requeue_reason,omitempty"`
	LastRequeueAtMs   int64         `json:"last_requeue_at_ms,omitempty"`

	// 0 => forever
	ExpiresAtMs int64 `json:"expires_at_ms"`
	StartedAtMs    int64  `json:"started_at_ms,omitempty"`
    LeaseUntilMs   int64  `json:"lease_until_ms,omitempty"`
    UpdatedAtMs    int64  `json:"updated_at_ms,omitempty"`
    ExecutedTimeMs int64 `json:"executed_time_ms,omitempty"`

}

type RequeueReason string

const (
	RequeueNone         RequeueReason = ""
	RequeueLeaseExpired RequeueReason = "lease_expired" // зависла в processing и lease истёк
	RequeueRetry        RequeueReason = "retry"         // retry после ошибки/таймаута
)

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
    bProc = []byte("proc") // procKey(8B leaseUntilMs + msg_id) -> msg_id
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
        for _, b := range [][]byte{bMeta, bBody, bIdem, bExp, bProc} {
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
const processingTimeoutMs int64 = 120_000 // пример: 2 минуты (лучше из конфига очереди)

func (s *Store) MarkProcessing(msgID string, attempt int) error {
    nowMs := time.Now().UnixMilli()
    return s.db.Update(func(tx *bolt.Tx) error {
        bm := tx.Bucket(bMeta)
        bp := tx.Bucket(bProc)

        v := bm.Get([]byte(msgID))
        if v == nil { return ErrNotFound }

        var meta Meta
        if err := json.Unmarshal(v, &meta); err != nil { return err }

        if meta.Status == StatusProcessing && meta.LeaseUntilMs > nowMs {
            return fmt.Errorf("already locked")
        }

        // удалить старый индекс, если был
        if meta.Status == StatusProcessing && meta.LeaseUntilMs > 0 {
            _ = bp.Delete(makeProcKey(meta.LeaseUntilMs, msgID))
        }

        meta.Status = StatusProcessing
        meta.Attempt = attempt
        meta.StartedAtMs = nowMs
        meta.LeaseUntilMs = nowMs + processingTimeoutMs
        meta.UpdatedAtMs = nowMs
        meta.Result = Result{}

        buf, _ := json.Marshal(meta)
        if err := bm.Put([]byte(msgID), buf); err != nil { return err }

        // добавить новый индекс
        return bp.Put(makeProcKey(meta.LeaseUntilMs, msgID), []byte(msgID))
    })
}


func (s *Store) TouchProcessing(msgID string) error {
    nowMs := time.Now().UnixMilli()
    return s.db.Update(func(tx *bolt.Tx) error {
        bm := tx.Bucket(bMeta)
        bp := tx.Bucket(bProc)

        v := bm.Get([]byte(msgID))
        if v == nil { return ErrNotFound }

        var meta Meta
        if err := json.Unmarshal(v, &meta); err != nil { return err }
        if meta.Status != StatusProcessing { return nil }

        oldLease := meta.LeaseUntilMs
        newLease := nowMs + processingTimeoutMs

        if oldLease > 0 {
            _ = bp.Delete(makeProcKey(oldLease, msgID))
        }

        meta.LeaseUntilMs = newLease
        meta.UpdatedAtMs = nowMs

        buf, _ := json.Marshal(meta)
        if err := bm.Put([]byte(msgID), buf); err != nil { return err }

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
		bp := tx.Bucket(bProc) // <-- FIX: индекс processing (lease_until + msg_id)

		v := bm.Get([]byte(msgID))
		if v == nil {
			return ErrNotFound
		}

		var meta Meta
		if err := json.Unmarshal(v, &meta); err != nil {
			return fmt.Errorf("unmarshal meta: %w", err)
		}

		// <-- FIX: если задача была в processing, удаляем её из proc-индекса
		// Важно сделать это ДО обнуления LeaseUntilMs.
		if bp != nil && meta.Status == StatusProcessing && meta.LeaseUntilMs > 0 {
			_ = bp.Delete(makeProcKey(meta.LeaseUntilMs, msgID))
		}

		oldExp := meta.ExpiresAtMs
		if newExpiresAtMs != 0 {
			meta.ExpiresAtMs = newExpiresAtMs
		}

		// считаем executed_time_ms, пока StartedAtMs ещё не обнулён
		if res.DurationMs > 0 {
			meta.ExecutedTimeMs = res.DurationMs
		} else if meta.StartedAtMs > 0 && res.FinishedAt > 0 && res.FinishedAt >= meta.StartedAtMs {
			meta.ExecutedTimeMs = res.FinishedAt - meta.StartedAtMs
		} else {
			meta.ExecutedTimeMs = 0
		}

		meta.Status = status
		meta.Result = res

		// состояние обработки больше не активно
		meta.StartedAtMs = 0
		meta.LeaseUntilMs = 0
		meta.UpdatedAtMs = time.Now().UnixMilli()

		buf, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("marshal meta: %w", err)
		}
		if err := bm.Put([]byte(msgID), buf); err != nil {
			return fmt.Errorf("put meta: %w", err)
		}

		// удалить старый TTL-индекс, если срок изменился
		if oldExp != 0 && oldExp != meta.ExpiresAtMs {
			_ = be.Delete(makeExpKey(oldExp, msgID))
		}

		// добавить (или обновить) новый TTL-индекс
		if meta.ExpiresAtMs != 0 {
			if err := be.Put(makeExpKey(meta.ExpiresAtMs, msgID), []byte(msgID)); err != nil {
				return fmt.Errorf("put exp: %w", err)
			}
		}

		return nil
	})
}

func (s *Store) RequeueForRetry(msgID string, nowMs int64) error {
	if nowMs == 0 {
		nowMs = time.Now().UnixMilli()
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		v := bm.Get([]byte(msgID))
		if v == nil {
			return ErrNotFound
		}

		var meta Meta
		if err := json.Unmarshal(v, &meta); err != nil {
			return err
		}

		meta.Status = StatusQueued
		meta.StartedAtMs = 0
		meta.LeaseUntilMs = 0
		meta.LastRequeueReason = RequeueRetry
		meta.LastRequeueAtMs = nowMs
		meta.UpdatedAtMs = nowMs

		buf, _ := json.Marshal(meta)
		return bm.Put([]byte(msgID), buf)
	})
}

var ErrNotReady = errors.New("result not ready")

func (s *Store) GetStatusAndResult(msgID string) (Status, Result, error) {
    var st Status
    var res Result

    err := s.db.View(func(tx *bolt.Tx) error {
        bm := tx.Bucket(bMeta)
        v := bm.Get([]byte(msgID))
        if v == nil {
            return ErrNotFound
        }

        var meta Meta
        if err := json.Unmarshal(v, &meta); err != nil {
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
type QueueInfo struct {
	Succeeded     int     `json:"succeeded"`
	Failed        int     `json:"failed"`
	Retries       int     `json:"retries"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
}

func (s *Store) GetInfo(queue string, fromTimeMs int64) (QueueInfo, error) {
	var info QueueInfo
	var totalDuration int64

	err := s.db.View(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)

		return bm.ForEach(func(k, v []byte) error {
			var meta Meta
			if err := json.Unmarshal(v, &meta); err != nil {
				return nil // пропускаем битые записи
			}

			// фильтр по очереди
			if queue != "" && meta.Queue != queue {
				return nil
			}

			// фильтр по периоду
			if fromTimeMs > 0 && meta.UpdatedAtMs < fromTimeMs {
				return nil
			}

			// retries = сумма (attempt-1)
			if meta.Attempt > 1 {
				info.Retries += meta.Attempt - 1
			}

			switch meta.Status {
			case StatusSucceeded:
				info.Succeeded++
				if meta.ExecutedTimeMs > 0 {
					totalDuration += meta.ExecutedTimeMs
				}
			case StatusFailed:
				info.Failed++
			}
			return nil
		})
	})

	if err != nil {
		return QueueInfo{}, err
	}
	if info.Succeeded > 0 {
		info.AvgDurationMs = float64(totalDuration) / float64(info.Succeeded)
	}
	return info, nil
}

func (s *Store) GetInfoAll(fromTimeMs int64) (map[string]QueueInfo, error) {
	infos := make(map[string]QueueInfo)
	totalDurations := make(map[string]int64) // queue -> суммарная длительность по succeeded

	err := s.db.View(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)

		return bm.ForEach(func(k, v []byte) error {
			var meta Meta
			if err := json.Unmarshal(v, &meta); err != nil {
				return nil
			}

			if meta.Queue == "" {
				return nil
			}

			if fromTimeMs > 0 && meta.UpdatedAtMs < fromTimeMs {
				return nil
			}

			info := infos[meta.Queue]

			if meta.Attempt > 1 {
				info.Retries += meta.Attempt - 1
			}

			switch meta.Status {
			case StatusSucceeded:
				info.Succeeded++
				if meta.ExecutedTimeMs > 0 {
					totalDurations[meta.Queue] += meta.ExecutedTimeMs
				}
			case StatusFailed:
				info.Failed++
			}

			infos[meta.Queue] = info
			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	// посчитать avg по каждой очереди
	for q, info := range infos {
		if info.Succeeded > 0 {
			info.AvgDurationMs = float64(totalDurations[q]) / float64(info.Succeeded)
			infos[q] = info
		}
	}

	return infos, nil
}


func (s *Store) RequeueStuck(nowMs int64, max int) (requeued int, err error) {
    if nowMs == 0 {
        nowMs = time.Now().UnixMilli()
    }

    err = s.db.Update(func(tx *bolt.Tx) error {
        bm := tx.Bucket(bMeta)
        bp := tx.Bucket(bProc)

        c := bp.Cursor()

        // идём с начала proc: все ключи отсортированы по leaseUntilMs
        for k, _ := c.First(); k != nil; {
            leaseUntil, msgID, ok := parseProcKey(k)
            if !ok {
                nextK, _ := c.Next()
                _ = bp.Delete(k)
                k = nextK
                continue
            }

            if leaseUntil > nowMs {
                break // дальше lease ещё “свежий”
            }

            // запоминаем следующий ключ до удаления текущего
            nextK, _ := c.Next()

            mv := bm.Get([]byte(msgID))
            if mv == nil {
                _ = bp.Delete(k) // мета нет — чистим индекс
                k = nextK
                continue
            }

            var meta Meta
            if err := json.Unmarshal(mv, &meta); err != nil {
                // битая мета — можно чистить индекс, а мету/тело уже твой выбор
                _ = bp.Delete(k)
                k = nextK
                continue
            }

            // защита от устаревшего индекса:
            // если мета уже не processing или lease отличается — просто чистим индекс
            if meta.Status != StatusProcessing || meta.LeaseUntilMs != leaseUntil {
                _ = bp.Delete(k)
                k = nextK
                continue
            }

            // requeue
            meta.Status = StatusQueued
            meta.LastRequeueReason = RequeueLeaseExpired
            meta.LastRequeueAtMs = nowMs
            meta.StartedAtMs = 0
            meta.LeaseUntilMs = 0
            meta.UpdatedAtMs = nowMs

            buf, _ := json.Marshal(meta)
            if err := bm.Put([]byte(msgID), buf); err != nil {
                return err
            }

            // убрать из индекса processing
            _ = bp.Delete(k)

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


func (s *Store) GC(nowMs int64, maxDeletes int) (deleted int, err error) {
    if nowMs == 0 {
        nowMs = time.Now().UnixMilli()
    }

    return deleted, s.db.Update(func(tx *bolt.Tx) error {
        bm := tx.Bucket(bMeta)
        bb := tx.Bucket(bBody)
        bi := tx.Bucket(bIdem)
        be := tx.Bucket(bExp)

        c := be.Cursor()
        for k, _ := c.First(); k != nil; {
            expAt, msgID, ok := parseExpKey(k)
            if !ok {
                _ = be.Delete(k) // Битая запись в индексе
                k, _ = c.Next()
                continue
            }

            if expAt > nowMs {
                break // Остальные ключи еще "свежие"
            }

            // Запоминаем следующий ключ до удаления текущего
            nextK, _ := c.Next()

            mv := bm.Get([]byte(msgID))
            if mv == nil {
                // Данных уже нет, просто чистим зависший индекс
                _ = be.Delete(k)
                k = nextK
                continue
            }

            var meta Meta
            if err := json.Unmarshal(mv, &meta); err != nil {
                // Битые данные — удаляем всё, чтобы не засорять базу
                _ = bm.Delete([]byte(msgID))
                _ = bb.Delete([]byte(msgID))
                _ = be.Delete(k)
                deleted++
                k = nextK
                continue
            }

            // --- КЛЮЧЕВАЯ ЛОГИКА ЗАЩИТЫ ---
            if meta.Status == StatusProcessing {
                // Сообщение живет, пока воркер работает. 
                // Сдвигаем TTL индекса на 2 минуты вперед.
                _ = be.Delete(k)
                newExp := nowMs + 120_000 
                _ = be.Put(makeExpKey(newExp, msgID), []byte(msgID))
            } else {
                // Для всех остальных статусов (Succeeded, Failed, Queued) — 
                // если TTL истек, удаляем окончательно.
                _ = bm.Delete([]byte(msgID))
                _ = bb.Delete([]byte(msgID))
                _ = be.Delete(k)
                
                if meta.IdempotencyKey != "" {
                    // Чистим идемпотентность только если она принадлежит этому сообщению
                    if iv := bi.Get([]byte(meta.IdempotencyKey)); iv != nil && string(iv) == msgID {
                        _ = bi.Delete([]byte(meta.IdempotencyKey))
                    }
                }
                deleted++
            }

            if maxDeletes > 0 && deleted >= maxDeletes {
                break
            }
            k = nextK
        }
        return nil
    })
}

// expKey: 8 bytes big-endian expiresAtMs + msg_id bytes
func makeExpKey(expiresAtMs int64, msgID string) []byte {
    b := make([]byte, 8+len(msgID))
    binary.BigEndian.PutUint64(b[:8], uint64(expiresAtMs))
    copy(b[8:], msgID)
    return b
}

func parseExpKey(k []byte) (expiresAtMs int64, msgID string, ok bool) {
	if len(k) < 8 {
		return 0, "", false
	}
	exp := int64(binary.BigEndian.Uint64(k[:8]))
	return exp, string(k[8:]), true
}

func makeProcKey(leaseUntilMs int64, msgID string) []byte {
    b := make([]byte, 8+len(msgID))
    binary.BigEndian.PutUint64(b[:8], uint64(leaseUntilMs))
    copy(b[8:], msgID)
    return b
}

func parseProcKey(k []byte) (leaseUntilMs int64, msgID string, ok bool) {
    if len(k) < 8 { return 0, "", false }
    lease := int64(binary.BigEndian.Uint64(k[:8]))
    return lease, string(k[8:]), true
}

