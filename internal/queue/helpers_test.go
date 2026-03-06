package queue

import (
	"bytes"
	"testing"
)

func TestStripPort(t *testing.T) {
	cases := []struct {
		in  string
		out string
	}{
		{"127.0.0.1:9000", "127.0.0.1"},
		{"localhost:8080", "localhost"},
		{"9000", "9000"},
		{"", ""},
	}
	for _, tc := range cases {
		if got := stripPort(tc.in); got != tc.out {
			t.Fatalf("stripPort(%q)=%q want %q", tc.in, got, tc.out)
		}
	}
}

func TestSplitCGI_ParsesHeadersAndBody(t *testing.T) {
	raw := []byte("Content-Type: text/plain\r\nX-Test: 1\r\n\r\nBODY")
	hdr, body := splitCGI(raw)

	if hdr == nil {
		t.Fatalf("expected hdr != nil")
	}
	if hdr["content-type"] != "text/plain" {
		t.Fatalf("content-type: got %q want %q", hdr["content-type"], "text/plain")
	}
	if hdr["x-test"] != "1" {
		t.Fatalf("x-test: got %q want %q", hdr["x-test"], "1")
	}
	if !bytes.Equal(body, []byte("BODY")) {
		t.Fatalf("body: got %q want %q", string(body), "BODY")
	}
}

func TestSafeCmd(t *testing.T) {
	// safeCmd обычно возвращает безопасный cmd (не nil/не пустой, или обрезает мусор)
	_ = safeCmd(nil)
	_ = safeCmd([]string{})
	_ = safeCmd([]string{"php", "-v"})
}

func TestSplitCGIBodyIndex(t *testing.T) {
	idx := splitCGIBodyIndex([]byte("A: b\r\n\r\nBODY"))
	if idx <= 0 {
		t.Fatalf("expected idx>0, got %d", idx)
	}
}

func TestSplitCGI_NoSeparator_ReturnsAllBody(t *testing.T) {
	raw := []byte("NO-HEADERS-JUST-BODY")
	hdr, body := splitCGI(raw)

	_ = hdr // hdr может быть nil или пустым map — зависит от реализации
	if !bytes.Equal(body, raw) {
		t.Fatalf("body: got %q want %q", string(body), string(raw))
	}
}

func TestSplitCGI_SeparatorLFOnly(t *testing.T) {
	raw := []byte("Content-Type: text/plain\n\nBODY")
	hdr, body := splitCGI(raw)

	if hdr["content-type"] != "text/plain" {
		t.Fatalf("content-type: got %q", hdr["content-type"])
	}
	if !bytes.Equal(body, []byte("BODY")) {
		t.Fatalf("body: got %q", string(body))
	}
}