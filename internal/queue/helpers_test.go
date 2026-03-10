package queue

import (
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

func TestSplitCGIBodyIndex(t *testing.T) {
	idx := splitCGIBodyIndex([]byte("A: b\r\n\r\nBODY"))
	if idx <= 0 {
		t.Fatalf("expected idx>0, got %d", idx)
	}
}
