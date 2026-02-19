package queue

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
	"strconv"

	"go-web-server/internal/httpserver"

	"github.com/google/uuid"
	"go.uber.org/zap"
	storage "go-web-server/internal/storage"
)

const (
	headerIdempotencyKey = "Idempotency-Key"
	cmdNewMessage        = "newmessage"
	maxBodyBytes         = 1 << 20 // 1 MiB
	cmdGetInfo    = "info"
	cmdGetInfoAll = "info_all"
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
		status := http.StatusUnprocessableEntity//422
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
		status := http.StatusInternalServerError//500
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

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
    defer cancel()

    // 7) store (dedup barrier)
    effectiveMsgID := msgID
    created := true
    if rt.Store != nil {
        // Передаем ctx в PutNewMessage (нужно будет обновить сигнатуру метода в Store)
        storedMsgID, wasCreated, err := rt.Store.PutNewMessage(
            ctx, // <-- Добавлен контекст
            queueName,
            msgID,
            body,
            idemKey,
            time.Now().UnixMilli(),
            expiresAtMs,
        )
        if err != nil {
            status := http.StatusInternalServerError
            // Если ошибка вызвана таймаутом контекста
            if errors.Is(err, context.DeadlineExceeded) {
                status = http.StatusGatewayTimeout
            }
            
            m.log.Error("storage_put_failed",
                append(baseJobFields(status, msgID), zap.Error(err))...,
            )
            http.Error(w, "storage operation timed out or failed", status)
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

	if err := rt.Enqueue(ctx, job); err != nil { // <-- Добавлен контекст
        st := http.StatusTooManyRequests
        if errors.Is(err, context.DeadlineExceeded) {
            st = http.StatusGatewayTimeout
        } else if errors.Is(err, ErrUnknownQueue) {
            st = http.StatusNotFound
        }

        m.log.Warn("queue_new_message_rejected",
            append(baseJobFields(st, effectiveMsgID),
                zap.String("reason", "enqueue_failed"),
                zap.Error(err),
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
}

func (m *Manager) HandleReportDone(queueName, msgID string, w http.ResponseWriter, r *http.Request) {
    rt, ok := m.Get(queueName)
    if !ok || rt.Store == nil {
        http.Error(w, "unknown queue", http.StatusNotFound)
        return
    }

    // защита: токен воркера
    token := r.Header.Get("X-Worker-Token")
    if token == "" || token != os.Getenv("WORKER_TOKEN") {
        http.Error(w, "forbidden", http.StatusForbidden)
        return
    }

    defer func() { _ = r.Body.Close() }()

    var res storage.Result
    dec := json.NewDecoder(r.Body)
    if err := dec.Decode(&res); err != nil && !errors.Is(err, io.EOF) {
        http.Error(w, "invalid json", http.StatusBadRequest)
        return
    }

    // статус по результату (пример)
    st := storage.StatusSucceeded
    if res.ExitCode != 0 {
        st = storage.StatusFailed
    }

    if err := rt.Store.MarkDone(msgID, st, res, res.DurationMs); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    w.WriteHeader(http.StatusNoContent)
}

const (
    cmdGetStatus = "status"
    cmdGetResult = "result"
)

func (m *Manager) HandleGetStatus(queueName, msgID string, w http.ResponseWriter, r *http.Request) {
    start := time.Now()
    requestID := httpserver.GetRequestID(r.Context())

    base := func(status int) []zap.Field {
        return []zap.Field{
            zap.String("request_id", requestID),
            zap.String("queue", queueName),
            zap.String("cmd", cmdGetStatus),
            zap.Int("status", status),
            zap.Int64("duration_ms", time.Since(start).Milliseconds()),
            zap.String("msg_id", msgID),
        }
    }

    rt, ok := m.Get(queueName)
    if !ok || rt.Store == nil {
        st := http.StatusNotFound
        m.log.Warn("queue_get_status_rejected", append(base(st),
            zap.String("reason", "unknown_queue"),
        )...)
        http.Error(w, "unknown queue", st)
        return
    }

    // если нет отдельного GetStatus — можно вызвать GetStatusAndResult и игнорить результат
    status, _, err := rt.Store.GetStatusAndResult(msgID)
    if err != nil && !errors.Is(err, storage.ErrNotReady) {
        if errors.Is(err, storage.ErrNotFound) {
            http.Error(w, "not found", http.StatusNotFound)
            return
        }
        http.Error(w, "internal error", http.StatusInternalServerError)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    _ = json.NewEncoder(w).Encode(map[string]any{
        "msg_id": msgID,
        "status": status,
    })
}

func (m *Manager) HandleGetResult(queueName, msgID string, w http.ResponseWriter, r *http.Request) {
    start := time.Now()
    requestID := httpserver.GetRequestID(r.Context())

    base := func(status int) []zap.Field {
        return []zap.Field{
            zap.String("request_id", requestID),
            zap.String("queue", queueName),
            zap.String("cmd", cmdGetResult),
            zap.Int("status", status),
            zap.Int64("duration_ms", time.Since(start).Milliseconds()),
            zap.String("msg_id", msgID),
        }
    }

    rt, ok := m.Get(queueName)
    if !ok || rt.Store == nil {
        st := http.StatusNotFound
        m.log.Warn("queue_get_result_rejected", append(base(st),
            zap.String("reason", "unknown_queue"),
        )...)
        http.Error(w, "unknown queue", st)
        return
    }

    status, res, err := rt.Store.GetStatusAndResult(msgID)
    if err != nil {
        if errors.Is(err, storage.ErrNotFound) {
            http.Error(w, "not found", http.StatusNotFound)
            return
        }
        if errors.Is(err, storage.ErrNotReady) {
            // 202: ещё не готово
            st := http.StatusAccepted
            m.log.Info("queue_get_result_not_ready", base(st)...)

            w.Header().Set("Content-Type", "application/json")
            w.WriteHeader(st)
            _ = json.NewEncoder(w).Encode(map[string]any{
                "msg_id": msgID,
                "status": status,
            })
            return
        }
        http.Error(w, "internal error", http.StatusInternalServerError)
        return
    }

    // 200: готово
    st := http.StatusOK
    m.log.Info("queue_get_result_ok", base(st)...)

    w.Header().Set("Content-Type", "application/json")
    _ = json.NewEncoder(w).Encode(map[string]any{
        "msg_id": msgID,
        "status": status,
        "result": res,
    })
}

func (m *Manager) HandleGetInfo(queueName string, w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := httpserver.GetRequestID(r.Context())

	// hours query param (?hours=6), default = 24
	hoursStr := r.URL.Query().Get("hours")
	hours := 24
	if hoursStr != "" {
		if h, err := strconv.Atoi(hoursStr); err == nil && h > 0 {
			hours = h
		}
	}
	fromTime := time.Now().Add(-time.Duration(hours) * time.Hour).UnixMilli()

	base := func(status int) []zap.Field {
		return []zap.Field{
			zap.String("request_id", requestID),
			zap.String("queue", queueName),
			zap.String("cmd", cmdGetInfo),
			zap.Int("status", status),
			zap.Int64("duration_ms", time.Since(start).Milliseconds()),
		}
	}

	rt, ok := m.Get(queueName)
	if !ok || rt.Store == nil {
		st := http.StatusNotFound
		m.log.Warn("queue_get_info_rejected",
			append(base(st), zap.String("reason", "unknown_queue"))...,
		)
		http.Error(w, "unknown queue", st)
		return
	}

	info, err := rt.Store.GetInfo(queueName, fromTime)
	if err != nil {
		st := http.StatusInternalServerError
		m.log.Error("queue_get_info_failed",
			append(base(st), zap.Error(err))...,
		)
		http.Error(w, "internal error", st)
		return
	}

	st := http.StatusOK
	m.log.Info("queue_get_info_ok",
		append(base(st), zap.Int("hours", hours))...,
	)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(st)
	_ = json.NewEncoder(w).Encode(info)
}

func (m *Manager) HandleGetInfoAll(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := httpserver.GetRequestID(r.Context())

	base := func(status int) []zap.Field {
		return []zap.Field{
			zap.String("request_id", requestID),
			zap.String("cmd", cmdGetInfoAll),
			zap.Int("status", status),
			zap.Int64("duration_ms", time.Since(start).Milliseconds()),
		}
	}

	if m.store == nil {
		st := http.StatusServiceUnavailable
		m.log.Warn("queue_get_info_all_rejected",
			append(base(st), zap.String("reason", "storage_disabled"))...,
		)
		http.Error(w, "storage disabled", st)
		return
	}

	fromTime := time.Now().Add(-24 * time.Hour).UnixMilli()

	infos, err := m.store.GetInfoAll(fromTime)
	if err != nil {
		st := http.StatusInternalServerError
		m.log.Error("queue_get_info_all_failed",
			append(base(st), zap.Error(err))...,
		)
		http.Error(w, "internal error", st)
		return
	}

	st := http.StatusOK
	m.log.Info("queue_get_info_all_ok", base(st)...)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(st)
	_ = json.NewEncoder(w).Encode(infos)
}

func (m *Manager) StartBackgroundTasks(ctx context.Context) {
    ticker := time.NewTicker(1 * time.Minute)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            // Проходим по всем очередям и чистим их
            for _, name := range m.ListNames() {
                if rt, ok := m.Get(name); ok && rt.Store != nil {
                    now := time.Now().UnixMilli()
                    
                    // 1. Возвращаем "протухшие" задачи в очередь
                    requeued, _ := rt.Store.RequeueStuck(now, 100)
                    
                    // 2. Удаляем старый мусор
                    deleted, _ := rt.Store.GC(now, 100)
                    
                    if requeued > 0 || deleted > 0 {
                        m.log.Info("background_cleanup_done",
                            zap.String("queue", name),
                            zap.Int("requeued", requeued),
                            zap.Int("deleted", deleted),
                        )
                    }
                }
            }
        }
    }
}
