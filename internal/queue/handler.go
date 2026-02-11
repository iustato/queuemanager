package queue

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"go-web-server/internal/httpserver"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

const (
	headerIdempotencyKey = "Idempotency-Key"
	cmdNewMessage        = "newmessage"
	maxBodyBytes         = 1 << 20 // 1 MiB
)

func newUUIDv7String() (string, error) {
	u, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func (m *Manager) HandleNewMessage(queueName string, w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := httpserver.GetRequestID(r.Context())

	baseRequestFields := func(status int) []zap.Field {
		return []zap.Field{
			zap.String("request_id", requestID),
			zap.String("queue", queueName),
			zap.String("cmd", cmdNewMessage),
			zap.Int("status", status),
			zap.Int64("duration_ms", time.Since(start).Milliseconds()),
		}
	}

	// 1) queue exists?
	rt, ok := m.Get(queueName)
	if !ok {
		status := http.StatusNotFound
		m.log.Warn("queue_new_message_rejected",
			append(baseRequestFields(status),
				zap.String("reason", "unknown_queue"),
			)...,
		)
		http.Error(w, "unknown queue", status)
		return
	}

	// 2) idempotency key
	idemKey := strings.TrimSpace(r.Header.Get(headerIdempotencyKey))
	generatedIdem := false

	if idemKey == "" {
		if os.Getenv("ALLOW_AUTO_IDEMPOTENCY") == "true" {
			gen, err := newUUIDv7String()
			if err != nil {
				status := http.StatusInternalServerError
				m.log.Error("idempotency_generation_failed",
					append(baseRequestFields(status), zap.Error(err))...,
				)
				http.Error(w, "internal error", status)
				return
			}
			idemKey = gen
			generatedIdem = true
			w.Header().Set(headerIdempotencyKey, idemKey)
		} else {
			status := http.StatusBadRequest
			m.log.Warn("queue_new_message_rejected",
				append(baseRequestFields(status),
					zap.String("reason", "missing_idempotency_key"),
				)...,
			)
			http.Error(w, "missing Idempotency-Key header", status)
			return
		}
	}

	parsed, err := uuid.Parse(idemKey)
	if err != nil {
		status := http.StatusBadRequest
		m.log.Warn("queue_new_message_rejected",
			append(baseRequestFields(status),
				zap.String("reason", "invalid_idempotency_key_format"),
				zap.String("idempotency_key", idemKey),
				zap.Bool("idempotency_generated", generatedIdem),
				zap.Error(err),
			)...,
		)
		http.Error(w, "invalid Idempotency-Key format", status)
		return
	}

	if parsed.Version() != uuid.Version(7) {
		status := http.StatusBadRequest
		m.log.Warn("queue_new_message_rejected",
			append(baseRequestFields(status),
				zap.String("reason", "idempotency_key_not_uuid7"),
				zap.String("idempotency_key", idemKey),
				zap.Bool("idempotency_generated", generatedIdem),
				zap.Int("uuid_version", int(parsed.Version())),
			)...,
		)
		http.Error(w, "Idempotency-Key must be UUIDv7", status)
		return
	}

	// 3) read body (per-queue MaxSize + global cap)
	limit := int64(maxBodyBytes)
	if rt.Cfg.MaxSize > 0 && int64(rt.Cfg.MaxSize) < limit {
		limit = int64(rt.Cfg.MaxSize)
	}

	r.Body = http.MaxBytesReader(w, r.Body, limit)
	defer func() { _ = r.Body.Close() }()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		if errors.As(err, new(*http.MaxBytesError)) {
			status := http.StatusRequestEntityTooLarge
			m.log.Warn("queue_new_message_rejected",
				append(baseRequestFields(status),
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
			append(baseRequestFields(status),
				zap.String("reason", "read_body_failed"),
				zap.Error(err),
			)...,
		)
		http.Error(w, "cannot read request body", status)
		return
	}

	payloadBytes := len(body)

	// 4) parse json
	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		status := http.StatusBadRequest
		m.log.Warn("queue_new_message_rejected",
			append(baseRequestFields(status),
				zap.String("reason", "invalid_json"),
				zap.Int("payload_bytes", payloadBytes),
				zap.Error(err),
			)...,
		)
		http.Error(w, "invalid json", status)
		return
	}

	// 5) schema validate
	if err := rt.Schema.ValidateJSON(payload); err != nil {
		status := http.StatusUnprocessableEntity
		m.log.Warn("queue_new_message_rejected",
			append(baseRequestFields(status),
				zap.String("reason", "schema_validation_failed"),
				zap.Int("payload_bytes", payloadBytes),
				zap.Error(err),
			)...,
		)
		http.Error(w, "schema validation failed: "+err.Error(), status)
		return
	}
	// 6) generate msg id (candidate)
	msgID, err := newUUIDv7String()
	if err != nil {
		status := http.StatusInternalServerError
		m.log.Error("msg_id_generation_failed",
			append(baseRequestFields(status), zap.Error(err))...,
		)
		http.Error(w, "internal error", status)
		return
	}

	expiresAtMs := time.Now().Add(30 * 24 * time.Hour).UnixMilli() // временно

	baseJobFields := func(status int, effectiveMsgID string) []zap.Field {
		return append(
			baseRequestFields(status),
			zap.String("msg_id", effectiveMsgID),
			zap.String("idempotency_key", idemKey),
			zap.Bool("idempotency_generated", generatedIdem),
		)
	}

	// 7) store (dedup barrier)
	effectiveMsgID := msgID
	created := true
	if rt.Store != nil {
		storedMsgID, wasCreated, err := rt.Store.PutNewMessage(
			queueName,
			msgID,
			body, // raw payload bytes
			idemKey,
			time.Now().UnixMilli(),
			expiresAtMs,
		)
		if err != nil {
			status := http.StatusInternalServerError
			m.log.Error("storage_put_failed",
				append(baseJobFields(status, msgID), zap.Error(err))...,
			)
			http.Error(w, "internal error", status)
			return
		}

		effectiveMsgID = storedMsgID
		created = wasCreated

		if !created {
			// дубль: не enqueue, просто отдаем существующий msg_id
			status := http.StatusAccepted
			m.log.Info("queue_new_message_dedup",
				append(baseJobFields(status, effectiveMsgID),
					zap.Int("payload_bytes", payloadBytes),
				)...,
			)

			w.Header().Set("X-Message-Id", effectiveMsgID)
			w.Header().Set(headerIdempotencyKey, idemKey)
			w.WriteHeader(status)
			_, _ = w.Write([]byte("accepted"))
			return
		}
	}

	// 8) enqueue ONLY for created=true
	job := Job{
		Queue:          queueName,
		MsgID:          effectiveMsgID,
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
		st := http.StatusTooManyRequests
		reason := "enqueue_failed"
		if errors.Is(err, ErrUnknownQueue) {
			st = http.StatusNotFound
			reason = "unknown_queue"
		}

		m.log.Warn("queue_new_message_rejected",
			append(baseJobFields(st, effectiveMsgID),
				zap.String("reason", reason),
				zap.Error(err),
				zap.Int("payload_bytes", payloadBytes),
			)...,
		)
		http.Error(w, err.Error(), st)
		return
	}

	status := http.StatusAccepted
	m.log.Info("queue_new_message_accepted",
		append(baseJobFields(status, effectiveMsgID),
			zap.Int("payload_bytes", payloadBytes),
		)...,
	)

	w.Header().Set("X-Message-Id", effectiveMsgID)
	w.Header().Set(headerIdempotencyKey, idemKey)
	w.WriteHeader(status)
	_, _ = w.Write([]byte("accepted"))
	return

status = http.StatusAccepted
m.log.Info("queue_new_message_accepted",
	append(baseJobFields(status, effectiveMsgID),
		zap.Int("payload_bytes", payloadBytes),
		// (опционально) zap.Bool("idempotency_generated", idemGenerated),
	)...,
)

w.Header().Set("X-Message-Id", msgID)
w.Header().Set("Idempotency-Key", idemKey)
w.WriteHeader(status)
_, _ = w.Write([]byte("accepted"))
}
