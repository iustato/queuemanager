package queue

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func TestMapPushErrorToHTTP_PendingEnqueue(t *testing.T) {
	st, msg, reason := mapPushErrorToHTTP(ErrPendingEnqueue)
	if st != http.StatusAccepted {
		t.Fatalf("status: want %d got %d", http.StatusAccepted, st)
	}
	if reason != "pending_enqueue" {
		t.Fatalf("reason: want %q got %q", "pending_enqueue", reason)
	}
	if msg != "accepted" {
		t.Fatalf("msg: want %q got %q", "accepted", msg)
	}
}

func TestMapPushErrorToHTTP_ContextTimeout(t *testing.T) {
	st, _, reason := mapPushErrorToHTTP(context.DeadlineExceeded)
	if st != http.StatusGatewayTimeout {
		t.Fatalf("status: want %d got %d", http.StatusGatewayTimeout, st)
	}
	if reason != "timeout" {
		t.Fatalf("reason: want timeout got %q", reason)
	}
}

func TestMapPushErrorToHTTP_InvalidIdem(t *testing.T) {
	err := ErrInvalidIdemKey{Msg: "missing Idempotency-Key header"}
	st, msg, reason := mapPushErrorToHTTP(err)

	if st != http.StatusBadRequest {
		t.Fatalf("status: want %d got %d", http.StatusBadRequest, st)
	}
	if reason != "invalid_idempotency_key" {
		t.Fatalf("reason: want invalid_idempotency_key got %q", reason)
	}
	if msg == "" {
		t.Fatalf("msg should not be empty")
	}
}

func TestMapPushErrorToHTTP_SchemaValidation(t *testing.T) {
	err := ErrSchemaValidation{Cause: errors.New("nope")}
	st, msg, reason := mapPushErrorToHTTP(err)

	if st != http.StatusUnprocessableEntity {
		t.Fatalf("status: want %d got %d", http.StatusUnprocessableEntity, st)
	}
	if reason != "schema_validation_failed" {
		t.Fatalf("reason: want schema_validation_failed got %q", reason)
	}
	if msg == "" {
		t.Fatalf("msg should not be empty")
	}
}