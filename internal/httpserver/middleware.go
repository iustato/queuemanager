package httpserver

import (
	"net"
	"net/http"
	"time"

	"go.uber.org/zap"
)

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += n
	return n, err
}

// AccessLog — лог на каждый запрос: метод, путь, статус, длительность.
func AccessLog(log *zap.Logger, next http.Handler, disabled ...bool) http.Handler {
	if len(disabled) > 0 && disabled[0] {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}

		next.ServeHTTP(sw, r)

		remote := r.RemoteAddr
		if host, _, err := net.SplitHostPort(remote); err == nil {
			remote = host
		}

		rid := sw.Header().Get("X-Request-Id")
		if rid == "" {
			rid = GetRequestID(r.Context())
		}

		log.Info("http_request",
			zap.String("request_id", rid),
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Int("status", sw.status),
			zap.Int64("duration_ms", time.Since(start).Milliseconds()),
			zap.Int("bytes", sw.bytes),
			zap.String("remote", remote),
			zap.String("ua", r.UserAgent()),
		)
	})
}