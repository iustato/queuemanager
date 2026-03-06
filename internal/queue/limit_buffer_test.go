package queue

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLimitBuffer_Truncated_WhenOverLimit(t *testing.T) {
	var b limitBuffer
	b.limit = 5

	n, err := b.Write([]byte("hello world"))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len("hello world") {
		t.Fatalf("n: got %d want %d", n, len("hello world"))
	}
	if string(b.Bytes()) != "hello" {
		t.Fatalf("Bytes: got %q want %q", string(b.Bytes()), "hello")
	}
	if !b.Truncated() {
		t.Fatalf("expected Truncated=true")
	}
}

func TestReadLimitedBody_WithinLimit(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "http://x", strings.NewReader("abc"))

	body, err := readLimitedBody(w, r, 10)
	if err != nil {
		t.Fatalf("readLimitedBody: %v", err)
	}
	if !bytes.Equal(body, []byte("abc")) {
		t.Fatalf("body: got %q want %q", string(body), "abc")
	}
}

func TestReadLimitedBody_OverLimit_ReturnsError(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "http://x", strings.NewReader("0123456789"))

	_, err := readLimitedBody(w, r, 5)
	if err == nil {
		t.Fatalf("expected error on over-limit body, got nil")
	}

	// Часто эта функция ещё пишет статус 413/400 в ResponseWriter.
	// Если у тебя так — можно раскомментировать строгую проверку:
	//
	// if w.Code == 200 {
	//     t.Fatalf("expected non-200 status when over limit, got %d", w.Code)
	// }
}