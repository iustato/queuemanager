package storage

import (
	"fmt"

	msgpack "github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"
)

func (s *Store) GetInfo(queue string, fromTimeMs int64) (QueueInfo, error) {
	var info QueueInfo
	var totalDuration int64

	err := s.db.View(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		if bm == nil {
			return ErrBucketMissing
		}

		return bm.ForEach(func(k, v []byte) error {
			var meta Meta
			if err := msgpack.Unmarshal(v, &meta); err != nil {
				return fmt.Errorf("getinfo: unmarshal meta guid=%s: %w", string(k), err)
			}

			if queue != "" && meta.Queue != queue {
				return nil
			}
			if fromTimeMs > 0 && meta.UpdatedAtMs < fromTimeMs {
				return nil
			}

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
		if bm == nil {
			return nil
		}

		return bm.ForEach(func(k, v []byte) error {
			var meta Meta
			if err := msgpack.Unmarshal(v, &meta); err != nil {
				return nil // пропускаем битые записи
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

	// avg по каждой очереди (только по succeeded)
	for q, info := range infos {
		if info.Succeeded > 0 {
			info.AvgDurationMs = float64(totalDurations[q]) / float64(info.Succeeded)
			infos[q] = info
		}
	}

	return infos, nil
}
