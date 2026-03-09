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
		http.Error(w, "unknown queue", http.StatusNotFound)
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
		http.Error(w, msg, st)
		return
	}

	pushTimeout := time.Duration(rt.Cfg.PushTimeoutSec) * time.Second
	if pushTimeout <= 0 {
		pushTimeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), pushTimeout)
	defer cancel()

	resp, pushErr := m.service.Push(ctx, PushRequest{
		Queue:    queueName,
		Body:     body,
		IdemKey:  strings.TrimSpace(r.Header.Get(headerIdempotencyKey)),
		AutoIdem: m.allowAutoIdem,

		Method:      r.Method,
		QueryString: r.URL.RawQuery,
		ContentType: r.Header.Get("Content-Type"),
		RemoteAddr:  r.RemoteAddr,
	})

	// 2) Если сервис уже успел вычислить msgID/idemKey — возвращаем их даже при ошибке
	if resp.MsgID != "" {
		w.Header().Set("X-Message-Id", resp.MsgID)
	}
	if resp.IdemKey != "" {
		w.Header().Set(headerIdempotencyKey, resp.IdemKey)
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
			zap.String("msg_id", resp.MsgID),
			zap.String("idempotency_key", resp.IdemKey),
			zap.Error(pushErr),
		)
		http.Error(w, msg, st)
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
		zap.String("msg_id", resp.MsgID),
		zap.String("idempotency_key", resp.IdemKey),
		zap.Bool("created", resp.Created),
	)

	// 3) Явный Content-Type
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Message-Id", resp.MsgID)
	w.Header().Set(headerIdempotencyKey, resp.IdemKey)
	w.WriteHeader(st)
	_, _ = w.Write([]byte("accepted"))
}