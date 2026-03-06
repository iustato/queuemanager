package queue

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseFromTimeMs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rawURL  string
		want    int64
		wantErr bool
	}{
		{
			name:    "missing param => default 0",
			rawURL:  "http://example.test/info",
			want:    0,
			wantErr: false,
		},
		{
			name:    "empty param => default 0",
			rawURL:  "http://example.test/info?from_time_ms=",
			want:    0,
			wantErr: false,
		},
		{
			name:    "valid integer",
			rawURL:  "http://example.test/info?from_time_ms=123",
			want:    123,
			wantErr: false,
		},
		{
			name:    "valid integer with spaces",
			rawURL:  "http://example.test/info?from_time_ms=%20%20123%20",
			want:    123,
			wantErr: false,
		},
		{
			name:    "zero is allowed",
			rawURL:  "http://example.test/info?from_time_ms=0",
			want:    0,
			wantErr: false,
		},
		{
			name:    "negative => error",
			rawURL:  "http://example.test/info?from_time_ms=-1",
			want:    0,
			wantErr: true,
		},
		{
			name:    "non-integer => error",
			rawURL:  "http://example.test/info?from_time_ms=abc",
			want:    0,
			wantErr: true,
		},
		{
			name:    "float => error",
			rawURL:  "http://example.test/info?from_time_ms=1.2",
			want:    0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodGet, tt.rawURL, nil)

			got, err := parseFromTimeMs(r)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseFromTimeMs() err=%v wantErr=%v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("parseFromTimeMs()=%d want=%d", got, tt.want)
			}
		})
	}
}