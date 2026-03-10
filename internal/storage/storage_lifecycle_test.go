package storage

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	msgpack "github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"
)

func openTempStoreWithOpts(t *testing.T, opts OpenOptions) *Store {
	t.Helper()
	if opts.FilePath == "" {
		opts.FilePath = "test_store_tmp.db"
	}
	_ = os.Remove(opts.FilePath)

	st, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		_ = st.Close()
		_ = os.Remove(opts.FilePath)
	})
	return st
}

func getMetaForTest(t *testing.T, st *Store, msgID string) Meta {
	t.Helper()
	var meta Meta
	err := st.db.View(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		if bm == nil {
			return ErrBucketMissing
		}
		v := bm.Get([]byte(msgID))
		if v == nil {
			return ErrNotFound
		}
		return msgpack.Unmarshal(v, &meta)
	})
	if err != nil {
		t.Fatalf("get meta: %v", err)
	}
	return meta
}

func bucketHasKey(t *testing.T, db *bolt.DB, bucket []byte, key []byte) bool {
	t.Helper()
	var ok bool
	_ = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		ok = b.Get(key) != nil
		return nil
	})
	return ok
}

func TestGC_ProcessingMessage_IsNotDeleted_AndExpIsPushed(t *testing.T) {
	// deterministic grace
	st := openTempStoreWithOpts(t, OpenOptions{
		FilePath:            "test_gc_processing_push.db",
		GCProcessingGraceMs: 5000,
		ProcessingTimeoutMs: 10_000,
	})

	ctx := context.Background()
	queue := "q1"
	msgID := newUUIDv7ForTest(t)
	idem := newUUIDv7ForTest(t)
	body := []byte(`{"text":"gc-proc"}`)

	nowMs := time.Now().UnixMilli()
	oldExp := nowMs - 1

	_, created, err := st.PutNewMessage(ctx, queue, msgID, body, idem, nowMs, oldExp)
	if err != nil {
		t.Fatalf("PutNewMessage: %v", err)
	}
	if !created {
		t.Fatalf("expected created")
	}

	if err := st.MarkProcessing(msgID, 1); err != nil {
		t.Fatalf("MarkProcessing: %v", err)
	}

	deleted, err := st.GC(nowMs, 10)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("expected deleted=0, got %d", len(deleted))
	}

	// meta/body still exist
	meta, gotBody, err := st.GetByMsgID(msgID)
	if err != nil {
		t.Fatalf("GetByMsgID: %v", err)
	}
	if meta.Status != StatusProcessing {
		t.Fatalf("status: got %v want %v", meta.Status, StatusProcessing)
	}
	if !bytes.Equal(gotBody, body) {
		t.Fatalf("body changed")
	}

	// exp index: old removed, new inserted
	oldKey := makeExpKey(oldExp, msgID)
	if bucketHasKey(t, st.db, bExp, oldKey) {
		t.Fatalf("expected old exp key removed")
	}

	newExp := nowMs + st.gcProcessingGraceMs
	newKey := makeExpKey(newExp, msgID)
	if !bucketHasKey(t, st.db, bExp, newKey) {
		t.Fatalf("expected new exp key present (exp=%d)", newExp)
	}
}

func TestTouchProcessing_UpdatesLeaseAndMovesProcIndex(t *testing.T) {
	st := openTempStoreWithOpts(t, OpenOptions{
		FilePath:            "test_touch_processing.db",
		ProcessingTimeoutMs: 10_000,
	})

	ctx := context.Background()
	queue := "q1"
	msgID := newUUIDv7ForTest(t)
	idem := newUUIDv7ForTest(t)
	body := []byte(`{"text":"touch"}`)

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

	meta1 := getMetaForTest(t, st, msgID)
	oldLease := meta1.LeaseUntilMs
	if oldLease == 0 {
		t.Fatalf("expected oldLease != 0")
	}

	oldProcKey := makeProcKey(oldLease, msgID)
	if !bucketHasKey(t, st.db, bProc, oldProcKey) {
		t.Fatalf("expected old proc key present")
	}

	time.Sleep(2 * time.Millisecond) // ensure now changes
	if err := st.TouchProcessing(msgID); err != nil {
		t.Fatalf("TouchProcessing: %v", err)
	}

	meta2 := getMetaForTest(t, st, msgID)
	newLease := meta2.LeaseUntilMs
	if newLease == 0 || newLease == oldLease {
		t.Fatalf("expected lease to change: old=%d new=%d", oldLease, newLease)
	}

	if bucketHasKey(t, st.db, bProc, oldProcKey) {
		t.Fatalf("expected old proc key removed")
	}
	newProcKey := makeProcKey(newLease, msgID)
	if !bucketHasKey(t, st.db, bProc, newProcKey) {
		t.Fatalf("expected new proc key present")
	}
}

func TestMarkProcessing_AlreadyLocked(t *testing.T) {
	st := openTempStoreWithOpts(t, OpenOptions{
		FilePath:            "test_mark_processing_locked.db",
		ProcessingTimeoutMs: 10_000,
	})

	ctx := context.Background()
	queue := "q1"
	msgID := newUUIDv7ForTest(t)
	idem := newUUIDv7ForTest(t)
	body := []byte(`{"text":"lock"}`)

	_, created, err := st.PutNewMessage(ctx, queue, msgID, body, idem, time.Now().UnixMilli(), 0)
	if err != nil {
		t.Fatalf("PutNewMessage: %v", err)
	}
	if !created {
		t.Fatalf("expected created")
	}

	if err := st.MarkProcessing(msgID, 1); err != nil {
		t.Fatalf("MarkProcessing #1: %v", err)
	}

	// second lock while lease is alive -> error
	if err := st.MarkProcessing(msgID, 2); err == nil {
		t.Fatalf("expected error, got nil")
	}

	meta := getMetaForTest(t, st, msgID)
	if meta.Status != StatusProcessing {
		t.Fatalf("status: got %v want %v", meta.Status, StatusProcessing)
	}
	if meta.Attempt != 1 {
		t.Fatalf("attempt: got %d want 1", meta.Attempt)
	}
}

func TestMarkDone_UpdatesExpIndex_AndRemovesProcIndex(t *testing.T) {
	st := openTempStoreWithOpts(t, OpenOptions{
		FilePath:            "test_mark_done_exp_proc.db",
		ProcessingTimeoutMs: 10_000,
	})

	ctx := context.Background()
	queue := "q1"
	msgID := newUUIDv7ForTest(t)
	idem := newUUIDv7ForTest(t)
	body := []byte(`{"text":"done"}`)

	now := time.Now().UnixMilli()
	oldExp := now + 1_000
	newExp := now + 9_999

	_, created, err := st.PutNewMessage(ctx, queue, msgID, body, idem, now, oldExp)
	if err != nil {
		t.Fatalf("PutNewMessage: %v", err)
	}
	if !created {
		t.Fatalf("expected created")
	}

	// ensure old exp key exists
	if !bucketHasKey(t, st.db, bExp, makeExpKey(oldExp, msgID)) {
		t.Fatalf("expected old exp key present")
	}

	if err := st.MarkProcessing(msgID, 1); err != nil {
		t.Fatalf("MarkProcessing: %v", err)
	}
	meta1 := getMetaForTest(t, st, msgID)
	lease := meta1.LeaseUntilMs
	if lease == 0 {
		t.Fatalf("expected lease != 0")
	}
	if !bucketHasKey(t, st.db, bProc, makeProcKey(lease, msgID)) {
		t.Fatalf("expected proc key present")
	}

	res := Result{
		ExitCode:   0,
		DurationMs: 123,
		Err:        "",
	}
	if err := st.MarkDone(msgID, StatusSucceeded, res, newExp); err != nil {
		t.Fatalf("MarkDone: %v", err)
	}

	// proc index removed
	if bucketHasKey(t, st.db, bProc, makeProcKey(lease, msgID)) {
		t.Fatalf("expected proc key removed")
	}

	// exp index moved
	if bucketHasKey(t, st.db, bExp, makeExpKey(oldExp, msgID)) {
		t.Fatalf("expected old exp key removed")
	}
	if !bucketHasKey(t, st.db, bExp, makeExpKey(newExp, msgID)) {
		t.Fatalf("expected new exp key present")
	}

	meta2 := getMetaForTest(t, st, msgID)
	if meta2.Status != StatusSucceeded {
		t.Fatalf("status: got %v want %v", meta2.Status, StatusSucceeded)
	}
	if meta2.ExpiresAtMs != newExp {
		t.Fatalf("ExpiresAtMs: got %d want %d", meta2.ExpiresAtMs, newExp)
	}
	if meta2.LeaseUntilMs != 0 || meta2.StartedAtMs != 0 {
		t.Fatalf("expected started/lease cleared, got started=%d lease=%d", meta2.StartedAtMs, meta2.LeaseUntilMs)
	}
}

func TestPutNewMessage_Dedup_DoesNotOverwriteBodyOrMeta(t *testing.T) {
	st := openTempStoreWithOpts(t, OpenOptions{
		FilePath: "test_put_dedup_no_overwrite.db",
	})

	ctx := context.Background()
	idem := newUUIDv7ForTest(t)

	msg1 := newUUIDv7ForTest(t)
	queue1 := "q1"
	body1 := []byte(`{"text":"first"}`)

	ret1, created1, err := st.PutNewMessage(ctx, queue1, msg1, body1, idem, time.Now().UnixMilli(), 0)
	if err != nil {
		t.Fatalf("PutNewMessage #1: %v", err)
	}
	if !created1 || ret1 != msg1 {
		t.Fatalf("expected created1=true ret1=msg1")
	}

	msg2 := newUUIDv7ForTest(t)
	queue2 := "q2"
	body2 := []byte(`{"text":"second"}`)

	ret2, created2, err := st.PutNewMessage(ctx, queue2, msg2, body2, idem, time.Now().UnixMilli(), 0)
	if err != nil {
		t.Fatalf("PutNewMessage #2: %v", err)
	}
	if created2 {
		t.Fatalf("expected created2=false (dedup)")
	}
	if ret2 != msg1 {
		t.Fatalf("expected dedup to return msg1, got %s", ret2)
	}

	meta, gotBody, err := st.GetByMsgID(msg1)
	if err != nil {
		t.Fatalf("GetByMsgID(msg1): %v", err)
	}
	if meta.MsgID != msg1 {
		t.Fatalf("meta.MsgID overwritten: got %s want %s", meta.MsgID, msg1)
	}
	if meta.Queue != queue1 {
		t.Fatalf("meta.Queue overwritten: got %q want %q", meta.Queue, queue1)
	}
	if meta.IdempotencyKey != idem {
		t.Fatalf("meta.IdempotencyKey overwritten: got %q want %q", meta.IdempotencyKey, idem)
	}
	if !bytes.Equal(gotBody, body1) {
		t.Fatalf("body overwritten: got %s want %s", string(gotBody), string(body1))
	}
}