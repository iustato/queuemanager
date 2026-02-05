package httpserver

import (
    "context"
    "crypto/rand"
    "encoding/hex"
    "net/http"
    "strconv"
    "strings"
    "time"
)

type ctxKey struct{}
var requestIDKey = ctxKey{}//пустая структура для хранения

func GetRequestID(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey).(string)
	return v
}

// newRequestID generates a 32-hex-char request id (16 random bytes).
// Falls back to time-based value if crypto/rand fails.
func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}

	// Fallback: not cryptographically strong, but avoids "0000..." and keeps uniqueness in practice
	return strconv.FormatInt(time.Now().UnixNano(), 16)
}
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 1) Take from incoming header if present
		rid := strings.TrimSpace(r.Header.Get("X-Request-Id"))

		// 2) If missing (or only spaces), generate new
		if rid == "" {
			rid = newRequestID()
		}

		// (Optional) clamp very long IDs to prevent log/header abuse
		// Typical IDs are 32 (hex) or 36 (uuid). Adjust as you wish.
		if len(rid) > 128 {
			rid = rid[:128]
		}

		// 3) Put into response header so client can reference it
		w.Header().Set("X-Request-Id", rid)

		// 4) Put into context for internal code (AccessLog, handlers, etc.)
		ctx := context.WithValue(r.Context(), requestIDKey, rid)

		// 5) Continue chain with updated context
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}