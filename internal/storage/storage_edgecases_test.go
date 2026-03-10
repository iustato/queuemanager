package storage

import (
	"context"
	"testing"
	"time"

	msgpack "github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"
)

func TestParseExpKey_ShortKey(t *testing.T) {
	_, _, ok := parseExpKey([]byte{1, 2, 3, 4, 5, 6, 7})
	if ok {
		t.Fatalf("expected ok=false")
	}
}

func TestParseProcKey_ShortKey(t *testing.T) {
	_, _, ok := parseProcKey([]byte{1, 2, 3, 4, 5, 6, 7})
	if ok {
		t.Fatalf("expected ok=false")
	}
}

func TestGC_DeletesNonProcessingMessage_AndCleansIndexes(t *testing.T) {
	st := openTempStoreWithOpts(t, OpenOptions{
		FilePath:            "test_gc_deletes_non_processing.db",
		ProcessingTimeoutMs: 10_000,
		GCProcessingGraceMs: 5_000,
	})

	ctx := context.Background()
	queue := "q1"
	msgID := newUUIDv7ForTest(t)
	idem := newUUIDv7ForTest(t)
	body := []byte(`{"text":"gc-delete"}`)

	nowMs := time.Now().UnixMilli()

	// expires in the past -> eligible for GC
	expiredAt := nowMs - 1

	_, created, err := st.PutNewMessage(ctx, queue, msgID, body, idem, nowMs, expiredAt)
	if err != nil {
		t.Fatalf("PutNewMessage: %v", err)
	}
	if !created {
		t.Fatalf("expected created")
	}

	// sanity: exp index exists before GC
	if !bucketHasKey(t, st.db, bExp, makeExpKey(expiredAt, msgID)) {
		t.Fatalf("expected exp key to exist before GC")
	}
	// sanity: idem index exists
	got, ok, err := st.ResolveIdemKey(idem)
	if err != nil {
		t.Fatalf("ResolveIdemKey: %v", err)
	}
	if !ok || got != msgID {
		t.Fatalf("expected idem -> msgID mapping before GC")
	}

	deleted, err := st.GC(nowMs, 10)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if len(deleted) != 1 {
		t.Fatalf("expected deleted=1, got %d", len(deleted))
	}

	// meta/body removed
	_, _, err = st.GetByMsgID(msgID)
	if err == nil {
		t.Fatalf("expected ErrNotFound after GC")
	}

	// exp index removed
	if bucketHasKey(t, st.db, bExp, makeExpKey(expiredAt, msgID)) {
		t.Fatalf("expected exp key removed after GC")
	}

	// idem index removed
	_, ok, err = st.ResolveIdemKey(idem)
	if err != nil {
		t.Fatalf("ResolveIdemKey after GC: %v", err)
	}
	if ok {
		t.Fatalf("expected idem mapping removed after GC")
	}
}

func TestRequeueStuck_DoesNotRequeueIfLeaseNotExpired(t *testing.T) {
	st := openTempStoreWithOpts(t, OpenOptions{
		FilePath:            "test_requeue_stuck_not_expired.db",
		ProcessingTimeoutMs: 10_000, // lease will be in the future
	})

	ctx := context.Background()
	queue := "q1"
	msgID := newUUIDv7ForTest(t)
	idem := newUUIDv7ForTest(t)
	body := []byte(`{"text":"lease-ok"}`)

	_, created, err := st.PutNewMessage(ctx, queue, msgID, body, idem, time.Now().UnixMilli(), 0)
	if err != nil {
		t.Fatalf("PutNewMessage: %v", err)
	}
	if !created {
		t.Fatalf("expected created")
	}

	if err := st.MarkProcessing(msgID, 1); err != nil {
		t.Fatalf("MarkProcessing: %v", err)
	}

	meta := getMetaForTest(t, st, msgID)
	if meta.LeaseUntilMs == 0 {
		t.Fatalf("expected lease set")
	}
	// call with now well before leaseUntilMs
	nowMs := time.Now().UnixMilli()

	requeued, err := st.RequeueStuck(nowMs, 10)
	if err != nil {
		t.Fatalf("RequeueStuck: %v", err)
	}
	if requeued != 0 {
		t.Fatalf("expected requeued=0, got %d", requeued)
	}

	meta2 := getMetaForTest(t, st, msgID)
	if meta2.Status != StatusProcessing {
		t.Fatalf("expected still processing, got %v", meta2.Status)
	}

	// proc index should still contain lease key
	if !bucketHasKey(t, st.db, bProc, makeProcKey(meta.LeaseUntilMs, msgID)) {
		t.Fatalf("expected proc key to remain")
	}
}

func TestGC_RemovesInvalidExpKeys_AndExpKeysWithoutMeta(t *testing.T) {
	st := openTempStoreWithOpts(t, OpenOptions{
		FilePath: "test_gc_invalid_exp_and_missing_meta.db",
	})

	nowMs := time.Now().UnixMilli()

	// Seed:
	// 1) invalid exp key (len < 8) -> should be deleted
	// 2) valid exp key, but meta is missing -> should be deleted
	missingMsgID := "missing_meta_1"
	validExpiredKey := makeExpKey(nowMs-1, missingMsgID)

	err := st.db.Update(func(tx *bolt.Tx) error {
		be := tx.Bucket(bExp)
		if be == nil {
			return ErrBucketMissing
		}
		if err := be.Put([]byte{1, 2, 3, 4, 5, 6, 7}, []byte("x")); err != nil {
			return err
		}
		return be.Put(validExpiredKey, []byte(missingMsgID))
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	deleted, err := st.GC(nowMs, 10)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	_ = deleted // deleted may be 0 because "missing meta" does not increment deleted in your code

	// both keys should be gone
	if bucketHasKey(t, st.db, bExp, []byte{1, 2, 3, 4, 5, 6, 7}) {
		t.Fatalf("expected invalid exp key removed")
	}
	if bucketHasKey(t, st.db, bExp, validExpiredKey) {
		t.Fatalf("expected exp key without meta removed")
	}
}

func TestRequeueStuck_CleansInvalidProcKeys_AndLeaseMismatch(t *testing.T) {
	st := openTempStoreWithOpts(t, OpenOptions{
		FilePath: "test_requeue_stuck_invalid_keys_and_mismatch.db",
	})

	nowMs := time.Now().UnixMilli()

	// We'll seed:
	// 1) invalid proc key (len < 8) -> should be deleted
	// 2) a proc key where meta exists, but meta.LeaseUntilMs != leaseUntil in key -> should be deleted (mismatch)
	msgID := "mismatch_1"
	leaseKey := nowMs - 1000         // expired lease in index
	metaLease := nowMs + 1_000_000   // mismatching lease in meta (different!)

	procKeyMismatch := makeProcKey(leaseKey, msgID)

	err := st.db.Update(func(tx *bolt.Tx) error {
		bp := tx.Bucket(bProc)
		bm := tx.Bucket(bMeta)
		if bp == nil || bm == nil {
			return ErrBucketMissing
		}

		// (1) invalid proc key
		if err := bp.Put([]byte{9, 9, 9, 9, 9, 9, 9}, []byte("x")); err != nil {
			return err
		}

		// (2) mismatch key + meta
		if err := bp.Put(procKeyMismatch, []byte(msgID)); err != nil {
			return err
		}

		meta := Meta{
			MsgID:        msgID,
			Queue:        "q1",
			Status:       StatusProcessing,
			LeaseUntilMs: metaLease, // mismatch vs leaseKey
			UpdatedAtMs:  nowMs,
		}
		buf, err := msgpack.Marshal(meta)
		if err != nil {
			return err
		}
		return bm.Put([]byte(msgID), buf)
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	requeued, err := st.RequeueStuck(nowMs, 10)
	if err != nil {
		t.Fatalf("RequeueStuck: %v", err)
	}
	if requeued != 0 {
		t.Fatalf("expected requeued=0 for mismatch case, got %d", requeued)
	}

	// proc keys should be cleaned
	if bucketHasKey(t, st.db, bProc, []byte{9, 9, 9, 9, 9, 9, 9}) {
		t.Fatalf("expected invalid proc key removed")
	}
	if bucketHasKey(t, st.db, bProc, procKeyMismatch) {
		t.Fatalf("expected mismatch proc key removed")
	}

	// meta should remain processing and unchanged (no requeue happened)
	err = st.db.View(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		if bm == nil {
			return ErrBucketMissing
		}
		v := bm.Get([]byte(msgID))
		if v == nil {
			t.Fatalf("meta missing")
		}
		var got Meta
		if err := msgpack.Unmarshal(v, &got); err != nil {
			t.Fatalf("unmarshal meta: %v", err)
		}
		if got.Status != StatusProcessing {
			t.Fatalf("expected status processing, got %v", got.Status)
		}
		if got.LeaseUntilMs != metaLease {
			t.Fatalf("expected meta lease preserved: got %d want %d", got.LeaseUntilMs, metaLease)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view: %v", err)
	}
}