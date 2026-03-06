package storage

import (
	"testing"
)

func TestParseEnqueueFailedKey_Ok(t *testing.T) {
	msgID := "m1"
	ts := int64(123456789)

	k := makeEnqueueFailedKey(ts, msgID)

	gotTs, gotMsgID, ok := parseEnqueueFailedKey(k)
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if gotTs != ts {
		t.Fatalf("ts: got %d want %d", gotTs, ts)
	}
	if gotMsgID != msgID {
		t.Fatalf("msgID: got %q want %q", gotMsgID, msgID)
	}
}

func TestParseEnqueueFailedKey_ShortKey(t *testing.T) {
	_, _, ok := parseEnqueueFailedKey([]byte{1, 2, 3, 4, 5, 6, 7}) // len < 8
	if ok {
		t.Fatalf("expected ok=false")
	}
}