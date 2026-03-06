package queue

import (
	"context"
	"errors"
	"testing"
)

func TestMapPushErrorToHTTP_UnknownError(t *testing.T) {
	st, msg, reason := mapPushErrorToHTTP(errors.New("random"))

	if st == 0 || msg == "" || reason == "" {
		t.Fatalf("unexpected mapping")
	}
}

func TestMapPushErrorToHTTP_ContextCanceled(t *testing.T) {
	st, _, _ := mapPushErrorToHTTP(context.Canceled)

	if st == 0 {
		t.Fatalf("expected http status")
	}
}