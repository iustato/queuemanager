package queue

import "testing"

func TestSplitCGIBodyIndex_CRLF(t *testing.T) {
	data := []byte("Header: x\r\n\r\nBODY")

	i := splitCGIBodyIndex(data)

	if i <= 0 {
		t.Fatalf("expected index >0")
	}
}

func TestSplitCGIBodyIndex_LFOnly(t *testing.T) {
	data := []byte("Header: x\n\nBODY")

	i := splitCGIBodyIndex(data)

	if i <= 0 {
		t.Fatalf("expected index >0")
	}
}

func TestSplitCGIBodyIndex_NoSeparator(t *testing.T) {
	data := []byte("Header: x\nBody")

	i := splitCGIBodyIndex(data)

	if i != 0 {
		t.Fatalf("expected -1 got %d", i)
	}
}