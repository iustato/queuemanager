package queue

import (
	"context"
	"net/http"
	"strings"
	"time"

	"go-web-server/internal/httpserver"

	"go.uber.org/zap"
)

func (m *Manager) HandleNewMessage(queueName string, w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := httpserver.GetRequestID(r.Context())

	// 1) Быстрая проверка существования очереди ДО чтения body (экономия ресурсов)
	rt, ok := m.Get(queueName)
	if !ok || rt == nil {
		m.log.Warn("queue_new_message_unknown_queue",
			zap.String("request_id", requestID),
			zap.String("queue", queueName),
			zap.String("cmd", cmdNewMessage),
			zap.Int("status", http.StatusNotFound),
			zap.Int64("duration_ms", time.Since(start).Milliseconds()),
		)
		writeAPIError(w, r, http.StatusNotFound, "unknown_queue", "unknown queue", nil)
		return
	}

	// per-queue limit
	limit := int64(maxBodyBytes)
	if rt.Cfg.MaxSize > 0 && int64(rt.Cfg.MaxSize) < limit {
		limit = int64(rt.Cfg.MaxSize)
	}

	body, err := readLimitedBody(w, r, limit)
	if err != nil {
		st, msg, reason := mapPushErrorToHTTP(err)
		m.log.Warn("queue_new_message_rejected",
			zap.String("request_id", requestID),
			zap.String("queue", queueName),
			zap.String("cmd", cmdNewMessage),
			zap.Int("status", st),
			zap.Int64("duration_ms", time.Since(start).Milliseconds()),
			zap.String("reason", reason),
			zap.Error(err),
		)
		writeAPIError(w, r, st, reason, msg, nil)
		return
	}

	pushTimeout := time.Duration(rt.Cfg.PushTimeoutSec) * time.Second
	if pushTimeout <= 0 {
		pushTimeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), pushTimeout)
	defer cancel()

	resp, pushErr := m.service.Push(ctx, PushRequest{
		Queue:       queueName,
		Body:        body,
		MessageGUID: strings.TrimSpace(r.Header.Get(headerMessageGUID)),

		Method:      r.Method,
		QueryString: r.URL.RawQuery,
		ContentType: r.Header.Get("Content-Type"),
		RemoteAddr:  r.RemoteAddr,
	})

	// 2) Если сервис уже успел вычислить MessageGUID — возвращаем даже при ошибке
	if resp.MessageGUID != "" {
		w.Header().Set(headerMessageGUID, resp.MessageGUID)
	}

	if pushErr != nil {
		st, msg, reason := mapPushErrorToHTTP(pushErr)
		m.log.Warn("queue_new_message_rejected",
			zap.String("request_id", requestID),
			zap.String("queue", queueName),
			zap.String("cmd", cmdNewMessage),
			zap.Int("status", st),
			zap.Int64("duration_ms", time.Since(start).Milliseconds()),
			zap.String("reason", reason),
			zap.String("message_guid", resp.MessageGUID),
			zap.Error(pushErr),
		)
		writeAPIError(w, r, st, reason, msg, nil)
		return
	}

	st := http.StatusAccepted
	ev := "queue_new_message_accepted"
	if !resp.Created {
		ev = "queue_new_message_dedup"
	}

	m.log.Info(ev,
		zap.String("request_id", requestID),
		zap.String("queue", queueName),
		zap.String("cmd", cmdNewMessage),
		zap.Int("status", st),
		zap.Int64("duration_ms", time.Since(start).Milliseconds()),
		zap.String("message_guid", resp.MessageGUID),
		zap.Bool("created", resp.Created),
	)

	w.Header().Set(headerMessageGUID, resp.MessageGUID)
	writeAPIOK(w, r, st, map[string]any{
		"status":       "accepted",
		"message_guid": resp.MessageGUID,
		"created_at":   time.UnixMilli(resp.CreatedAtMs).UTC().Format(time.RFC3339),
	})
}