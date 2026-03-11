package storage

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"
)

func newUUIDv7ForTest(t *testing.T) string {
	t.Helper()
	u, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid v7: %v", err)
	}
	return u.String()
}

func openTempStore(t *testing.T) *Store {
	t.Helper()
	dbPath := "test_store_enqfail.db"
	_ = os.Remove(dbPath)

	st, err := Open(OpenOptions{FilePath: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		_ = st.Close()
		_ = os.Remove(dbPath)
	})
	return st
}

func bucketHasMsgID(t *testing.T, db *bolt.DB, bucket []byte, msgID string) bool {
	t.Helper()
	var found bool
	_ = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			_ = v
			// ключи у тебя: 8 bytes ts + msgID, а value обычно msgID
			// поэтому проще проверять value, или окончание key
			if string(v) == msgID {
				found = true
				return nil
			}
		}
		return nil
	})
	return found
}

func TestMarkEnqueueFailed_Indexed_ThenRemovedByMarkQueued(t *testing.T) {
	st := openTempStore(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	queue := "q1"
	msgID := newUUIDv7ForTest(t)
	body := []byte(`{"text":"hello"}`)

	_, created, _, err := st.PutNewMessage(ctx, queue, msgID, body, time.Now().UnixMilli(), time.Now().Add(time.Hour).UnixMilli())
	if err != nil {
		t.Fatalf("PutNewMessage: %v", err)
	}
	if !created {
		t.Fatalf("expected created=true")
	}

	if err := st.MarkEnqueueFailed(msgID, "queue busy"); err != nil {
		t.Fatalf("MarkEnqueueFailed: %v", err)
	}

	if !bucketHasMsgID(t, st.db, bEnqFail, msgID) {
		t.Fatalf("expected msgID in enq_fail index")
	}

	if err := st.MarkQueued(msgID); err != nil {
		t.Fatalf("MarkQueued: %v", err)
	}

	if bucketHasMsgID(t, st.db, bEnqFail, msgID) {
		t.Fatalf("expected msgID removed from enq_fail index after MarkQueued")
	}
}

func TestPutNewMessage_Dedup(t *testing.T) {
	st := openTempStore(t)

	ctx := context.Background()
	queue := "q1"
	body := []byte(`{"text":"hello"}`)
	guid := newUUIDv7ForTest(t)

	ret1, created1, _, err := st.PutNewMessage(ctx, queue, guid, body, time.Now().UnixMilli(), 0)
	if err != nil {
		t.Fatalf("PutNewMessage #1: %v", err)
	}
	if !created1 || ret1 != guid {
		t.Fatalf("expected created1=true ret1=guid")
	}

	ret2, created2, _, err := st.PutNewMessage(ctx, queue, guid, body, time.Now().UnixMilli(), 0)
	if err != nil {
		t.Fatalf("PutNewMessage #2: %v", err)
	}
	if created2 {
		t.Fatalf("expected created2=false (dedup)")
	}
	if ret2 != guid {
		t.Fatalf("expected dedup to return same guid, got %s", ret2)
	}
}