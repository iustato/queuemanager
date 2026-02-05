package queue

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"go-web-server/internal/httpserver"

	"go.uber.org/zap"
)

const (
	headerMsgUUID = "X-Message-UUID"
	cmdNewMessage  = "newmessage"
	maxBodyBytes   = 1 << 20 // 1 MiB
)

func (m *Manager) HandleNewMessage(queueName string, w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	requestID := httpserver.GetRequestID(r.Context())
	msgID := strings.TrimSpace(r.Header.Get(headerMsgUUID))

	// helper: единые поля в каждом логе
	baseFields := func(status int) []zap.Field {
		return []zap.Field{
			zap.String("request_id", requestID),
			zap.String("queue", queueName),
			zap.String("cmd", cmdNewMessage),
			zap.String("msg_id", msgID),
			zap.Int("status", status),
			zap.Int64("duration_ms", time.Since(start).Milliseconds()),
		}
	}

	// 1) queue exists?
	rt, ok := m.Get(queueName)
	if !ok {
		status := http.StatusNotFound
		m.log.Warn("queue_new_message_rejected",
			append(baseFields(status),
				zap.String("reason", "unknown_queue"),
			)...,
		)
		http.Error(w, "unknown queue", status)
		return
	}

	// 2) msg id required
	if msgID == "" {
		status := http.StatusBadRequest
		m.log.Warn("queue_new_message_rejected",
			append(baseFields(status),
				zap.String("reason", "missing_msg_id"),
			)...,
		)
		http.Error(w, "missing X-Message-UUID header", status)
		return
	}

	if !looksLikeUUID(msgID) {
		status := http.StatusBadRequest
		m.log.Warn("queue_new_message_rejected",
			append(baseFields(status),
				zap.String("reason", "invalid_msg_id_format"),
			)...,
		)
		http.Error(w, "invalid X-Message-UUID format", status)
		return
	}

	// 3) read body (with limit)
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer func() { _ = r.Body.Close() }()

	body, err := io.ReadAll(r.Body)
	if err != nil {
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

	// 4) parse json
	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		status := http.StatusBadRequest
		m.log.Warn("queue_new_message_rejected",
			append(baseFields(status),
				zap.String("reason", "invalid_json"),
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
			append(baseFields(status),
				zap.String("reason", "schema_validation_failed"),
				zap.Error(err),
			)...,
		)
		http.Error(w, "schema validation failed: "+err.Error(), status)
		return
	}

	// 6) enqueue (пока заглушка) -> успех
	status := http.StatusAccepted
	m.log.Info("queue_new_message_accepted",
		append(baseFields(status),
			zap.Int("payload_bytes", len(body)),
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
