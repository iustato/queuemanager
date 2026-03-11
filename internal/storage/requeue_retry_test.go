package storage

import (
	"errors"
	"os"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	"github.com/vmihailenco/msgpack/v5"
)

func TestStore_RequeueForRetry_ShouldUpdateMetaAndIndex(t *testing.T) {
	dbPath := "test_requeue_retry.db"
	_ = os.Remove(dbPath)

	st, err := Open(OpenOptions{FilePath: dbPath})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	defer os.Remove(dbPath)

	// 1) create a meta record manually with an OLD enqueue-failed index key
	msgID := "m1"

	oldTs := time.Now().Add(-5 * time.Minute).UnixMilli()

	err = st.db.Update(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		be := tx.Bucket(bEnqFail)
		if bm == nil || be == nil {
			return ErrBucketMissing
		}

		meta := Meta{
			MessageGUID:        msgID,
			Status:             StatusCreated,
			EnqueueFailedAtMs:  oldTs,
			EnqueueError:       "old",
			StartedAtMs:        123,
			LeaseUntilMs:       456,
			UpdatedAtMs:        oldTs,
		}

		buf, err := msgpack.Marshal(meta)
		if err != nil {
			return err
		}
		if err := bm.Put([]byte(msgID), buf); err != nil {
			return err
		}

		// old index entry
		return be.Put(makeEnqueueFailedKey(oldTs, msgID), []byte(msgID))
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	// 2) call RequeueForRetry with explicit ts
	newTs := time.Now().UnixMilli()

	if err := st.RequeueForRetry(msgID, newTs); err != nil {
		t.Fatalf("RequeueForRetry: %v", err)
	}

	// 3) verify meta + index
	err = st.db.View(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		be := tx.Bucket(bEnqFail)
		if bm == nil || be == nil {
			return ErrBucketMissing
		}

		// meta updated
		v := bm.Get([]byte(msgID))
		if v == nil {
			t.Fatalf("meta missing")
		}
		var meta Meta
		if err := msgpack.Unmarshal(v, &meta); err != nil {
			t.Fatalf("unmarshal meta: %v", err)
		}

		if meta.Status != StatusEnqueueFailed {
			t.Fatalf("status: got %v want %v", meta.Status, StatusEnqueueFailed)
		}
		if meta.EnqueueFailedAtMs != newTs {
			t.Fatalf("EnqueueFailedAtMs: got %d want %d", meta.EnqueueFailedAtMs, newTs)
		}
		if meta.EnqueueError != "retry scheduled" {
			t.Fatalf("EnqueueError: got %q want %q", meta.EnqueueError, "retry scheduled")
		}
		if meta.StartedAtMs != 0 {
			t.Fatalf("StartedAtMs: got %d want 0", meta.StartedAtMs)
		}
		if meta.LeaseUntilMs != 0 {
			t.Fatalf("LeaseUntilMs: got %d want 0", meta.LeaseUntilMs)
		}
		if meta.UpdatedAtMs != newTs {
			t.Fatalf("UpdatedAtMs: got %d want %d", meta.UpdatedAtMs, newTs)
		}

		// old index deleted
		if got := be.Get(makeEnqueueFailedKey(oldTs, msgID)); got != nil {
			t.Fatalf("expected old index deleted, but found: %q", string(got))
		}

		// new index exists
		if got := be.Get(makeEnqueueFailedKey(newTs, msgID)); string(got) != msgID {
			t.Fatalf("expected new index=%q, got %q", msgID, string(got))
		}

		return nil
	})
	if err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestStore_RequeueForRetry_TsZero_ShouldSetNonZeroTimestamp(t *testing.T) {
	dbPath := "test_requeue_retry_ts0.db"
	_ = os.Remove(dbPath)

	st, err := Open(OpenOptions{FilePath: dbPath})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	defer os.Remove(dbPath)

	msgID := "m1"

	// seed meta without enqueue-failed
	err = st.db.Update(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		if bm == nil {
			return ErrBucketMissing
		}
		meta := Meta{MessageGUID: msgID, Status: StatusCreated}
		buf, err := msgpack.Marshal(meta)
		if err != nil {
			return err
		}
		return bm.Put([]byte(msgID), buf)
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	before := time.Now().UnixMilli()
	if err := st.RequeueForRetry(msgID, 0); err != nil {
		t.Fatalf("RequeueForRetry: %v", err)
	}
	after := time.Now().UnixMilli()

	// verify ts in range
	err = st.db.View(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		if bm == nil {
			return ErrBucketMissing
		}
		v := bm.Get([]byte(msgID))
		if v == nil {
			t.Fatalf("meta missing")
		}
		var meta Meta
		if err := msgpack.Unmarshal(v, &meta); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if meta.EnqueueFailedAtMs == 0 {
			t.Fatalf("expected EnqueueFailedAtMs != 0")
		}
		if meta.EnqueueFailedAtMs < before || meta.EnqueueFailedAtMs > after {
			t.Fatalf("ts out of range: got %d, want between [%d..%d]", meta.EnqueueFailedAtMs, before, after)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestStore_RequeueForRetry_NotFound(t *testing.T) {
	dbPath := "test_requeue_retry_nf.db"
	_ = os.Remove(dbPath)

	st, err := Open(OpenOptions{FilePath: dbPath})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	defer os.Remove(dbPath)

	err = st.RequeueForRetry("missing", time.Now().UnixMilli())
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %T: %v", err, err)
	}
}