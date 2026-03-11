package queue

import (
	"context"
	"errors"
	"net/http"
)

func mapPushErrorToHTTP(err error) (status int, publicMsg string, reason string) {
	if err == nil {
		return http.StatusOK, "", ""
	}

	// атомарность: запись создана, enqueue не удался, будет ретрай фоном
	if errors.Is(err, ErrPendingEnqueue) {
		return http.StatusAccepted, "accepted", "pending_enqueue"
	}

	// контекст
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return http.StatusGatewayTimeout, "request timed out", "timeout"
	}

	// очередь
	if errors.Is(err, ErrQueueNotFound) {
		return http.StatusNotFound, "unknown queue", "unknown_queue"
	}

	// runtime enqueue
	if errors.Is(err, ErrQueueBusy) {
		return http.StatusTooManyRequests, "queue is busy", "queue_busy"
	}
	if errors.Is(err, ErrQueueStopped) {
		return http.StatusServiceUnavailable, "queue is stopped", "queue_stopped"
	}
	if errors.Is(err, ErrRuntimeNotInit) {
		return http.StatusInternalServerError, "internal error", "runtime_not_initialized"
	}

	// guid/json/schema/size
	var guidErr ErrInvalidMessageGUID
	if errors.As(err, &guidErr) {
		return http.StatusBadRequest, guidErr.Error(), "invalid_message_guid"
	}
	var ptl ErrPayloadTooLarge
	if errors.As(err, &ptl) {
		return http.StatusRequestEntityTooLarge, "payload too large", "payload_too_large"
	}
	var ij ErrInvalidJSON
	if errors.As(err, &ij) {
		return http.StatusBadRequest, "invalid json", "invalid_json"
	}
	var sv ErrSchemaValidation
	if errors.As(err, &sv) {
		return http.StatusUnprocessableEntity, "schema validation failed: "+sv.Unwrap().Error(), "schema_validation_failed"
	}

	return http.StatusInternalServerError, "internal error", "internal_error"
}