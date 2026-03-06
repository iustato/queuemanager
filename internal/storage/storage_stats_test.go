package storage

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	msgpack "github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"
)

func TestResolveIdemKey_Empty(t *testing.T) {
	st := openTempStoreWithOpts(t, OpenOptions{FilePath: "test_resolve_idem_empty.db"})

	msgID, ok, err := st.ResolveIdemKey("")
	if err != nil {
		t.Fatalf("ResolveIdemKey: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false")
	}
	if msgID != "" {
		t.Fatalf("expected msgID empty")
	}
}

func TestResolveIdemKey_Found(t *testing.T) {
	st := openTempStoreWithOpts(t, OpenOptions{FilePath: "test_resolve_idem_found.db"})

	ctx := context.Background()
	queue := "q1"
	msgID := newUUIDv7ForTest(t)
	idem := newUUIDv7ForTest(t)
	body := []byte(`{"text":"hello"}`)

	_, created, err := st.PutNewMessage(ctx, queue, msgID, body, idem, time.Now().UnixMilli(), 0)
	if err != nil {
		t.Fatalf("PutNewMessage: %v", err)
	}
	if !created {
		t.Fatalf("expected created")
	}

	got, ok, err := st.ResolveIdemKey(idem)
	if err != nil {
		t.Fatalf("ResolveIdemKey: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if got != msgID {
		t.Fatalf("msgID: got %q want %q", got, msgID)
	}
}

func TestGetStatusAndResult_NotReady(t *testing.T) {
	st := openTempStoreWithOpts(t, OpenOptions{FilePath: "test_get_status_not_ready.db"})

	ctx := context.Background()
	queue := "q1"
	msgID := newUUIDv7ForTest(t)
	idem := newUUIDv7ForTest(t)
	body := []byte(`{"text":"nr"}`)

	_, created, err := st.PutNewMessage(ctx, queue, msgID, body, idem, time.Now().UnixMilli(), 0)
	if err != nil {
		t.Fatalf("PutNewMessage: %v", err)
	}
	if !created {
		t.Fatalf("expected created")
	}

	_, _, err = st.GetStatusAndResult(msgID)
	if err == nil {
		t.Fatalf("expected ErrNotReady, got nil")
	}
	if !errors.Is(err, ErrNotReady) {
		t.Fatalf("expected ErrNotReady, got %T: %v", err, err)
	}
}

func TestGetStatusAndResult_Succeeded(t *testing.T) {
	st := openTempStoreWithOpts(t, OpenOptions{
		FilePath:            "test_get_status_succeeded.db",
		ProcessingTimeoutMs: 10_000,
	})

	ctx := context.Background()
	queue := "q1"
	msgID := newUUIDv7ForTest(t)
	idem := newUUIDv7ForTest(t)
	body := []byte(`{"text":"ok"}`)

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

	want := Result{
		ExitCode:   0,
		DurationMs: 111,
		Err:        "",
	}
	if err := st.MarkDone(msgID, StatusSucceeded, want, 0); err != nil {
		t.Fatalf("MarkDone: %v", err)
	}

	stt, res, err := st.GetStatusAndResult(msgID)
	if err != nil {
		t.Fatalf("GetStatusAndResult: %v", err)
	}
	if stt != StatusSucceeded {
		t.Fatalf("status: got %v want %v", stt, StatusSucceeded)
	}
	if res.ExitCode != want.ExitCode || res.DurationMs != want.DurationMs || res.Err != want.Err {
		t.Fatalf("result: got %+v want %+v", res, want)
	}
	if res.FinishedAt == 0 {
		t.Fatalf("expected FinishedAt != 0")
	}
}

func TestGetStatusAndResult_Failed(t *testing.T) {
	st := openTempStoreWithOpts(t, OpenOptions{
		FilePath:            "test_get_status_failed.db",
		ProcessingTimeoutMs: 10_000,
	})

	ctx := context.Background()
	queue := "q1"
	msgID := newUUIDv7ForTest(t)
	idem := newUUIDv7ForTest(t)
	body := []byte(`{"text":"fail"}`)

	_, created, err := st.PutNewMessage(ctx, queue, msgID, body, idem, time.Now().UnixMilli(), 0)
	if err != nil {
		t.Fatalf("PutNewMessage: %v", err)
	}
	if !created {
		t.Fatalf("expected created")
	}

	if err := st.MarkProcessing(msgID, 2); err != nil {
		t.Fatalf("MarkProcessing: %v", err)
	}

	want := Result{
		ExitCode:   7,
		DurationMs: 0,
		Err:        "boom",
	}
	if err := st.MarkDone(msgID, StatusFailed, want, 0); err != nil {
		t.Fatalf("MarkDone: %v", err)
	}

	stt, res, err := st.GetStatusAndResult(msgID)
	if err != nil {
		t.Fatalf("GetStatusAndResult: %v", err)
	}
	if stt != StatusFailed {
		t.Fatalf("status: got %v want %v", stt, StatusFailed)
	}
	if res.ExitCode != want.ExitCode || res.Err != want.Err {
		t.Fatalf("result: got %+v want %+v", res, want)
	}
}

func TestGetInfo_ComputesCountsRetriesAndAvg(t *testing.T) {
	st := openTempStoreWithOpts(t, OpenOptions{
		FilePath:            "test_get_info.db",
		ProcessingTimeoutMs: 10_000,
	})

	ctx := context.Background()
	queue := "q1"
	now := time.Now().UnixMilli()

	// succeeded #1 (attempt=3 => retries +2, duration 100)
	msg1 := newUUIDv7ForTest(t)
	idem1 := newUUIDv7ForTest(t)
	_, _, err := st.PutNewMessage(ctx, queue, msg1, []byte(`{"n":1}`), idem1, now, 0)
	if err != nil {
		t.Fatalf("PutNewMessage #1: %v", err)
	}
	if err := st.MarkProcessing(msg1, 3); err != nil {
		t.Fatalf("MarkProcessing #1: %v", err)
	}
	if err := st.MarkDone(msg1, StatusSucceeded, Result{ExitCode: 0, DurationMs: 100}, 0); err != nil {
		t.Fatalf("MarkDone #1: %v", err)
	}

	// succeeded #2 (attempt=1 => retries +0, duration 200)
	msg2 := newUUIDv7ForTest(t)
	idem2 := newUUIDv7ForTest(t)
	_, _, err = st.PutNewMessage(ctx, queue, msg2, []byte(`{"n":2}`), idem2, now, 0)
	if err != nil {
		t.Fatalf("PutNewMessage #2: %v", err)
	}
	if err := st.MarkProcessing(msg2, 1); err != nil {
		t.Fatalf("MarkProcessing #2: %v", err)
	}
	if err := st.MarkDone(msg2, StatusSucceeded, Result{ExitCode: 0, DurationMs: 200}, 0); err != nil {
		t.Fatalf("MarkDone #2: %v", err)
	}

	// failed (attempt=2 => retries +1)
	msg3 := newUUIDv7ForTest(t)
	idem3 := newUUIDv7ForTest(t)
	_, _, err = st.PutNewMessage(ctx, queue, msg3, []byte(`{"n":3}`), idem3, now, 0)
	if err != nil {
		t.Fatalf("PutNewMessage #3: %v", err)
	}
	if err := st.MarkProcessing(msg3, 2); err != nil {
		t.Fatalf("MarkProcessing #3: %v", err)
	}
	if err := st.MarkDone(msg3, StatusFailed, Result{ExitCode: 9, Err: "x"}, 0); err != nil {
		t.Fatalf("MarkDone #3: %v", err)
	}

	info, err := st.GetInfo(queue, 0)
	if err != nil {
		t.Fatalf("GetInfo: %v", err)
	}

	if info.Succeeded != 2 {
		t.Fatalf("Succeeded: got %d want 2", info.Succeeded)
	}
	if info.Failed != 1 {
		t.Fatalf("Failed: got %d want 1", info.Failed)
	}

	// retries = (3-1) + (2-1) = 3
	if info.Retries != 3 {
		t.Fatalf("Retries: got %d want 3", info.Retries)
	}

	// avg duration = (100 + 200) / 2 = 150
	if info.AvgDurationMs != 150 {
		t.Fatalf("AvgDurationMs: got %v want %v", info.AvgDurationMs, 150.0)
	}
}

func TestGetInfoAll_PerQueueAggregation(t *testing.T) {
	st := openTempStoreWithOpts(t, OpenOptions{
		FilePath:            "test_get_info_all.db",
		ProcessingTimeoutMs: 10_000,
	})

	ctx := context.Background()
	now := time.Now().UnixMilli()

	// q1 succeeded duration 50
	msg1 := newUUIDv7ForTest(t)
	idem1 := newUUIDv7ForTest(t)
	_, _, err := st.PutNewMessage(ctx, "q1", msg1, []byte(`{"q":"q1"}`), idem1, now, 0)
	if err != nil {
		t.Fatalf("PutNewMessage q1: %v", err)
	}
	if err := st.MarkProcessing(msg1, 1); err != nil {
		t.Fatalf("MarkProcessing q1: %v", err)
	}
	if err := st.MarkDone(msg1, StatusSucceeded, Result{ExitCode: 0, DurationMs: 50}, 0); err != nil {
		t.Fatalf("MarkDone q1: %v", err)
	}

	// q2 succeeded duration 300
	msg2 := newUUIDv7ForTest(t)
	idem2 := newUUIDv7ForTest(t)
	_, _, err = st.PutNewMessage(ctx, "q2", msg2, []byte(`{"q":"q2"}`), idem2, now, 0)
	if err != nil {
		t.Fatalf("PutNewMessage q2: %v", err)
	}
	if err := st.MarkProcessing(msg2, 1); err != nil {
		t.Fatalf("MarkProcessing q2: %v", err)
	}
	if err := st.MarkDone(msg2, StatusSucceeded, Result{ExitCode: 0, DurationMs: 300}, 0); err != nil {
		t.Fatalf("MarkDone q2: %v", err)
	}

	infos, err := st.GetInfoAll(0)
	if err != nil {
		t.Fatalf("GetInfoAll: %v", err)
	}

	i1, ok := infos["q1"]
	if !ok {
		t.Fatalf("expected q1 in infos")
	}
	if i1.Succeeded != 1 || i1.Failed != 0 || i1.Retries != 0 || i1.AvgDurationMs != 50 {
		t.Fatalf("q1 info unexpected: %+v", i1)
	}

	i2, ok := infos["q2"]
	if !ok {
		t.Fatalf("expected q2 in infos")
	}
	if i2.Succeeded != 1 || i2.Failed != 0 || i2.Retries != 0 || i2.AvgDurationMs != 300 {
		t.Fatalf("q2 info unexpected: %+v", i2)
	}
}

func TestGetInfo_CorruptMeta_ReturnsError(t *testing.T) {
	// отдельная БД, чтобы не мешать другим тестам
	dbPath := "test_get_info_corrupt.db"
	_ = os.Remove(dbPath)
	st, err := Open(OpenOptions{FilePath: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		_ = st.Close()
		_ = os.Remove(dbPath)
	})

	// seed corrupt meta
	msgID := "bad_meta_1"
	err = st.db.Update(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		if bm == nil {
			return ErrBucketMissing
		}
		return bm.Put([]byte(msgID), []byte{0xff, 0x00, 0x01}) // invalid msgpack
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, err = st.GetInfo("", 0)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestGetInfoAll_CorruptMeta_IsSkipped(t *testing.T) {
	dbPath := "test_get_info_all_corrupt.db"
	_ = os.Remove(dbPath)
	st, err := Open(OpenOptions{FilePath: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		_ = st.Close()
		_ = os.Remove(dbPath)
	})

	// seed one corrupt meta + one valid succeeded meta
	err = st.db.Update(func(tx *bolt.Tx) error {
		bm := tx.Bucket(bMeta)
		if bm == nil {
			return ErrBucketMissing
		}

		if err := bm.Put([]byte("bad"), []byte{0xff, 0x01}); err != nil {
			return err
		}

		m := Meta{
			Queue:          "q1",
			MsgID:          "ok1",
			Status:         StatusSucceeded,
			Attempt:        1,
			ExecutedTimeMs: 10,
			UpdatedAtMs:    time.Now().UnixMilli(),
		}
		buf, err := msgpack.Marshal(m)
		if err != nil {
			return err
		}
		return bm.Put([]byte("ok1"), buf)
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	infos, err := st.GetInfoAll(0)
	if err != nil {
		t.Fatalf("GetInfoAll: %v", err)
	}

	// corrupt should be skipped, ok1 should be counted
	i, ok := infos["q1"]
	if !ok {
		t.Fatalf("expected q1 in infos")
	}
	if i.Succeeded != 1 || i.AvgDurationMs != 10 {
		t.Fatalf("unexpected q1 info: %+v", i)
	}
}