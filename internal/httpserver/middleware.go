package httpserver

import (
	"net/http"
	"time"

	"go.uber.org/zap"
)

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// AccessLog — лог на каждый запрос: метод, путь, статус, длительность.
func AccessLog(log *zap.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: 200}

		next.ServeHTTP(sw, r)

		log.Info("http_request",
			zap.String("request_id", GetRequestID(r.Context())),
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Int("status", sw.status),
			zap.Int64("duration_ms", time.Since(start).Milliseconds()),
			zap.String("remote", r.RemoteAddr),
			zap.String("ua", r.UserAgent()),
		)
	})
}

