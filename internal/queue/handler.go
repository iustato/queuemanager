package queue

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go-web-server/internal/httpserver"

	"go.uber.org/zap"
)

const (
	headerIdempotencyKey = "Idempotency-Key"
	cmdNewMessage        = "newmessage"
	maxBodyBytes         = 1 << 20 // 1 MiB (глобальный safety-лимит)
)

func newMessageID() string {
	// временно: UUIDv4-подобный безопасный рандом (32 hex chars)
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// fallback на время (хуже, но не падаем)
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (m *Manager) HandleNewMessage(queueName string, w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := httpserver.GetRequestID(r.Context())

	// 1) queue exists?
	rt, ok := m.Get(queueName)
	if !ok {
		status := http.StatusNotFound
		m.log.Warn("queue_new_message_rejected",
			zap.String("request_id", requestID),
			zap.String("queue", queueName),
			zap.String("cmd", cmdNewMessage),
			zap.String("reason", "unknown_queue"),
			zap.Int("status", status),
			zap.Int64("duration_ms", time.Since(start).Milliseconds()),
		)
		http.Error(w, "unknown queue", status)
		return
	}

	// 2) idempotency key (optional/required — у тебя сейчас required)
	idemKey := strings.TrimSpace(r.Header.Get(headerIdempotencyKey))
	if idemKey == "" {
		status := http.StatusBadRequest
		m.log.Warn("queue_new_message_rejected",
			zap.String("request_id", requestID),
			zap.String("queue", queueName),
			zap.String("cmd", cmdNewMessage),
			zap.String("reason", "missing_idempotency_key"),
			zap.Int("status", status),
			zap.Int64("duration_ms", time.Since(start).Milliseconds()),
		)
		http.Error(w, "missing Idempotency-Key header", status)
		return
	}
	if !looksLikeUUID(idemKey) {
		status := http.StatusBadRequest
		m.log.Warn("queue_new_message_rejected",
			zap.String("request_id", requestID),
			zap.String("queue", queueName),
			zap.String("cmd", cmdNewMessage),
			zap.String("reason", "invalid_idempotency_key_format"),
			zap.String("idempotency_key", idemKey),
			zap.Int("status", status),
			zap.Int64("duration_ms", time.Since(start).Milliseconds()),
		)
		http.Error(w, "invalid Idempotency-Key format", status)
		return
	}

	// 3) generate msg id (всегда серверный)
	msgID := newMessageID()
	if msgID == "" {
		m.log.Error("msg_id_generation_failed",
			zap.String("request_id", requestID),
			zap.String("queue", queueName),
			zap.String("cmd", cmdNewMessage),
		)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// helper: единые поля в каждом логе (теперь msgID уже готов)
	baseFields := func(status int) []zap.Field {
		return []zap.Field{
			zap.String("request_id", requestID),
			zap.String("queue", queueName),
			zap.String("cmd", cmdNewMessage),
			zap.String("msg_id", msgID),
			zap.String("idempotency_key", idemKey),
			zap.Int("status", status),
			zap.Int64("duration_ms", time.Since(start).Milliseconds()),
		}
	}

	// 4) read body (with per-queue MaxSize + global cap)
	limit := int64(maxBodyBytes)
	if rt.Cfg.MaxSize > 0 && int64(rt.Cfg.MaxSize) < limit {
		limit = int64(rt.Cfg.MaxSize)
	}

	r.Body = http.MaxBytesReader(w, r.Body, limit)
	defer func() { _ = r.Body.Close() }()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		if errors.As(err, new(*http.MaxBytesError)) {
			status := http.StatusRequestEntityTooLarge // 413
			m.log.Warn("queue_new_message_rejected",
				append(baseFields(status),
					zap.String("reason", "payload_too_large"),
					zap.Int64("limit_bytes", limit),
					zap.Int("max_size", rt.Cfg.MaxSize),
				)...,
			)
			http.Error(w, "payload too large", status)
			return
		}

		status := http.StatusBadRequest
		m.log.Warn("queue_new_message_rejected",
			append(baseFields(status),
				zap.String("reason", "read_body_failed"),
				zap.Error(err),
			)...,
		)
		http.Error(w, "cannot read request body", status)
		return
	}

	payloadBytes := len(body)

	// 5) parse json
	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		status := http.StatusBadRequest
		m.log.Warn("queue_new_message_rejected",
			append(baseFields(status),
				zap.String("reason", "invalid_json"),
				zap.Int("payload_bytes", payloadBytes),
				zap.Error(err),
			)...,
		)
		http.Error(w, "invalid json", status)
		return
	}

	// 6) schema validate
	if err := rt.Schema.ValidateJSON(payload); err != nil {
		status := http.StatusUnprocessableEntity
		m.log.Warn("queue_new_message_rejected",
			append(baseFields(status),
				zap.String("reason", "schema_validation_failed"),
				zap.Int("payload_bytes", payloadBytes),
				zap.Error(err),
			)...,
		)
		http.Error(w, "schema validation failed: "+err.Error(), status)
		return
	}

	// 7) enqueue
	job := Job{
		Queue:          queueName,
		MsgID:          msgID,
		Body:           body,
		EnqueuedAt:     time.Now(),
		Attempt:        1,
		Method:         r.Method,
		QueryString:    r.URL.RawQuery,
		ContentType:    r.Header.Get("Content-Type"),
		RemoteAddr:     r.RemoteAddr,
		IdempotencyKey: idemKey,
	}

	if err := rt.Enqueue(job); err != nil {
		status := http.StatusTooManyRequests
		reason := "enqueue_failed"
		if errors.Is(err, ErrUnknownQueue) {
			status = http.StatusNotFound
			reason = "unknown_queue"
		}

		m.log.Warn("queue_new_message_rejected",
			append(baseFields(status),
				zap.String("reason", reason),
				zap.Error(err),
				zap.Int("payload_bytes", payloadBytes),
			)...,
		)

		http.Error(w, err.Error(), status)
		return
	}

	status := http.StatusAccepted
	m.log.Info("queue_new_message_accepted",
		append(baseFields(status),
			zap.Int("payload_bytes", payloadBytes),
		)...,
	)

	w.WriteHeader(status)
	_, _ = w.Write([]byte("accepted"))
}

func looksLikeUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for _, i := range []int{8, 13, 18, 23} {
		if s[i] != '-' {
			return false
		}
	}
	for i := 0; i < len(s); i++ {
		if s[i] == '-' {
			continue
		}
		c := s[i]
		isHex := ('0' <= c && c <= '9') || ('a' <= c && c <= 'f') || ('A' <= c && c <= 'F')
		if !isHex {
			return false
		}
	}
	return true
}
