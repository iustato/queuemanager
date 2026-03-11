package queue

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"go-web-server/internal/config"
	"go-web-server/internal/storage"
)

func (m *Manager) HandleGetInfoAll(w http.ResponseWriter, r *http.Request) {
	fromMs, err := parseFromTimeMs(r)
	if err != nil {
		writeAPIError(w, r, http.StatusBadRequest,
			"invalid_argument",
			err.Error(),
			nil,
		)
		return
	}

	// aggregate info from each per-queue store
	m.mu.RLock()
	names := make([]string, 0, len(m.queues))
	rts := make([]*Runtime, 0, len(m.queues))
	for name, rt := range m.queues {
		if rt != nil && rt.Store != nil {
			names = append(names, name)
			rts = append(rts, rt)
		}
	}
	m.mu.RUnlock()

	type queueRow struct {
		Queue         string  `json:"queue"`
		Succeeded     int     `json:"succeeded"`
		Failed        int     `json:"failed"`
		Retries       int     `json:"retries"`
		AvgDurationMs float64 `json:"avg_duration_ms"`
	}

	queues := make([]queueRow, 0, len(names))
	for i, rt := range rts {
		info, err := rt.Store.GetInfo(names[i], fromMs)
		if err != nil {
			continue
		}
		queues = append(queues, queueRow{
			Queue:         names[i],
			Succeeded:     info.Succeeded,
			Failed:        info.Failed,
			Retries:       info.Retries,
			AvgDurationMs: info.AvgDurationMs,
		})
	}

	sort.Slice(queues, func(i, j int) bool { return queues[i].Queue < queues[j].Queue })

	writeAPIOK(w, r, http.StatusOK, map[string]any{
		"from_time_ms": fromMs,
		"queues":       queues,
	})
}

func (m *Manager) HandleGetInfo(queueName string, w http.ResponseWriter, r *http.Request) {
	queueName = strings.TrimSpace(queueName)
	if queueName == "" {
		writeAPIError(w, r, http.StatusBadRequest,
			"bad_request",
			"queue is required",
			nil,
		)
		return
	}
	rt, ok := m.Get(queueName)
	if !ok || rt == nil {
		writeAPIError(w, r, http.StatusNotFound,
			"queue_not_found",
			"unknown queue",
			map[string]any{"queue": queueName},
		)
		return
	}
	if rt.Store == nil {
		writeAPIError(w, r, http.StatusInternalServerError,
			"storage_not_initialized",
			"storage is not initialized",
			map[string]any{"queue": queueName},
		)
		return
	}

	fromMs, err := parseFromTimeMs(r)
	if err != nil {
		writeAPIError(w, r, http.StatusBadRequest,
			"invalid_argument",
			err.Error(),
			map[string]any{"queue": queueName},
		)
		return
	}

	info, err := rt.Store.GetInfo(queueName, fromMs)
	if err != nil {
		writeAPIError(w, r, http.StatusInternalServerError,
			"internal",
			err.Error(),
			map[string]any{"queue": queueName},
		)
		return
	}

	writeAPIOK(w, r, http.StatusOK, map[string]any{
		"queue":           queueName,
		"succeeded":       info.Succeeded,
		"failed":          info.Failed,
		"retries":         info.Retries,
		"avg_duration_ms": info.AvgDurationMs,
		"from_time_ms":    fromMs,
	})
}

// GET /{queue}/status/{msg_id}
func (m *Manager) HandleGetStatus(queueName, msgID string, w http.ResponseWriter, r *http.Request) {
	queueName = strings.TrimSpace(queueName)
	msgID = strings.TrimSpace(msgID)
	if queueName == "" || msgID == "" {
		writeAPIError(w, r, http.StatusBadRequest,
			"bad_request",
			"queue and msg_id are required",
			nil,
		)
		return
	}
	rt, ok := m.Get(queueName)
	if !ok || rt == nil {
		writeAPIError(w, r, http.StatusNotFound,
			"queue_not_found",
			"unknown queue",
			map[string]any{"queue": queueName},
		)
		return
	}
	if rt.Store == nil {
		writeAPIError(w, r, http.StatusInternalServerError,
			"storage_not_initialized",
			"storage is not initialized",
			nil,
		)
		return
	}

	st, _, err := rt.Store.GetStatusAndResult(msgID)
	if err != nil {
		if errors.Is(err, storage.ErrNotReady) {
			writeAPIError(w, r, http.StatusConflict,
				"not_ready",
				"status is not ready yet",
				map[string]any{"queue": queueName, "msg_id": msgID},
			)
			return
		}
		writeAPIError(w, r, http.StatusNotFound,
			"not_found",
			err.Error(),
			map[string]any{"queue": queueName, "msg_id": msgID},
		)
		return
	}

	writeAPIOK(w, r, http.StatusOK, map[string]any{
		"queue":  queueName,
		"msg_id": msgID,
		"status": st,
	})
}

// GET /{queue}/result/{msg_id}
func (m *Manager) HandleGetResult(queueName, msgID string, w http.ResponseWriter, r *http.Request) {
	queueName = strings.TrimSpace(queueName)
	msgID = strings.TrimSpace(msgID)
	if queueName == "" || msgID == "" {
		writeAPIError(w, r, http.StatusBadRequest,
			"bad_request",
			"queue and msg_id are required",
			nil,
		)
		return
	}
	rt, ok := m.Get(queueName)
	if !ok || rt == nil {
		writeAPIError(w, r, http.StatusNotFound,
			"queue_not_found",
			"unknown queue",
			map[string]any{"queue": queueName},
		)
		return
	}
	if rt.Store == nil {
		writeAPIError(w, r, http.StatusInternalServerError,
			"storage_not_initialized",
			"storage is not initialized",
			nil,
		)
		return
	}

	st, res, err := rt.Store.GetStatusAndResult(msgID)
	if err != nil {
		if errors.Is(err, storage.ErrNotReady) {
			writeAPIError(w, r, http.StatusConflict,
				"not_ready",
				"result is not ready yet",
				map[string]any{"queue": queueName, "msg_id": msgID},
			)
			return
		}
		writeAPIError(w, r, http.StatusNotFound,
			"not_found",
			err.Error(),
			map[string]any{"queue": queueName, "msg_id": msgID},
		)
		return
	}

	writeAPIOK(w, r, http.StatusOK, map[string]any{
		"queue":  queueName,
		"msg_id": msgID,
		"status": st,
		"result": res,
	})
}

func (m *Manager) HandleReportDone(queueName, msgID string, w http.ResponseWriter, r *http.Request) {
	queueName = strings.TrimSpace(queueName)
	msgID = strings.TrimSpace(msgID)
	if queueName == "" || msgID == "" {
		writeAPIError(w, r, http.StatusBadRequest,
			"bad_request",
			"queue and msg_id are required",
			nil,
		)
		return
	}
	rt, ok := m.Get(queueName)
	if !ok || rt == nil {
		writeAPIError(w, r, http.StatusNotFound,
			"queue_not_found",
			"unknown queue",
			map[string]any{"queue": queueName},
		)
		return
	}
	if rt.Store == nil {
		writeAPIError(w, r, http.StatusInternalServerError,
			"storage_not_initialized",
			"storage is not initialized",
			nil,
		)
		return
	}

	// optional auth
	if tok := strings.TrimSpace(m.workerToken); tok != "" {
		if !checkWorkerToken(r, tok) {
			writeAPIError(w, r, http.StatusUnauthorized,
				"unauthorized",
				"unauthorized",
				nil,
			)
			return
		}
	}

	type payload struct {
		ExitCode   int    `json:"exit_code"`
		DurationMs int64  `json:"duration_ms"`
		Err        string `json:"err"`
		TTLms      int64  `json:"ttl_ms"`
		FinishedAt int64  `json:"finished_at"`
	}

	// read body with size limit (worker callback payload is small JSON)
	const maxReportBody int64 = 64 * 1024 // 64 KB
	body, err := readLimitedBody(w, r, maxReportBody)
	if err != nil {
		writeAPIError(w, r, http.StatusRequestEntityTooLarge,
			"payload_too_large",
			"request body too large",
			nil,
		)
		return
	}
	if len(bytes.TrimSpace(body)) == 0 {
		writeAPIError(w, r, http.StatusBadRequest,
			"empty_body",
			"request body is required (must contain exit_code, duration_ms, etc.)",
			nil,
		)
		return
	}

	var p payload
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		writeAPIError(w, r, http.StatusBadRequest,
			"invalid_json",
			"invalid json body",
			map[string]any{"error": err.Error()},
		)
		return
	}

	finishedAt := p.FinishedAt
	if finishedAt == 0 {
		finishedAt = time.Now().UnixMilli()
	}

	status := storage.StatusSucceeded
	if p.ExitCode != 0 || strings.TrimSpace(p.Err) != "" {
		status = storage.StatusFailed
	}

	ttl := p.TTLms
	if ttl == 0 {
		if parsed, err := config.ParseDurationExt(rt.Cfg.ResultTTL); err == nil && parsed > 0 {
			ttl = time.Now().Add(parsed).UnixMilli()
		} else {
			ttl = -1 // хранить вечно
		}
	}

	if err := rt.Store.MarkDone(msgID, status, storage.Result{
		FinishedAt: finishedAt,
		ExitCode:   p.ExitCode,
		DurationMs: p.DurationMs,
		Err:        strings.TrimSpace(p.Err),
	}, ttl); err != nil {
		writeAPIError(w, r, http.StatusInternalServerError,
			"internal",
			err.Error(),
			map[string]any{"queue": queueName, "msg_id": msgID},
		)
		return
	}

	writeAPIOK(w, r, http.StatusOK, map[string]any{
		"queue":  queueName,
		"msg_id": msgID,
		"status": status,
	})
}

func parseFromTimeMs(r *http.Request) (int64, error) {
	v := strings.TrimSpace(r.URL.Query().Get("from_time_ms"))
	if v == "" {
		return 0, nil
	}

	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid from_time_ms")
	}

	return n, nil
}

func checkWorkerToken(r *http.Request, token string) bool {
	// X-Worker-Token (constant-time comparison to prevent timing attacks)
	got := strings.TrimSpace(r.Header.Get("X-Worker-Token"))
	if len(got) > 0 && subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1 {
		return true
	}
	// Authorization: Bearer <token>
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	const pref = "Bearer "
	if strings.HasPrefix(auth, pref) {
		bearer := strings.TrimSpace(strings.TrimPrefix(auth, pref))
		return subtle.ConstantTimeCompare([]byte(bearer), []byte(token)) == 1
	}
	return false
}
