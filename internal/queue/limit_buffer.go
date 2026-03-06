package queue

import (
	"errors"
	"io"
	"net/http"
	"bytes"
)

type limitBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func (b *limitBuffer) Write(p []byte) (int, error) {
	if b.limit <= 0 {
		b.truncated = true
		return len(p), nil
	}
	remain := b.limit - b.buf.Len()
	if remain <= 0 {
		b.truncated = true
		return len(p), nil
	}
	if len(p) <= remain {
		return b.buf.Write(p)
	}
	_, _ = b.buf.Write(p[:remain])
	b.truncated = true
	return len(p), nil
}

func (b *limitBuffer) Bytes() []byte { return b.buf.Bytes() }
func (b *limitBuffer) Truncated() bool {
	return b.truncated
}

func readLimitedBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	defer func() { _ = r.Body.Close() }()

	b, err := io.ReadAll(r.Body)
	if err != nil {
		if errors.As(err, new(*http.MaxBytesError)) {
			return nil, ErrPayloadTooLarge{LimitBytes: limit}
		}
		return nil, err
	}
	return b, nil
}