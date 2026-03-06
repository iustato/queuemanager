package queue

import (
	"errors"
	"testing"

	"go-web-server/internal/validate"
)

func TestServiceErrors_ErrorAndUnwrap(t *testing.T) {
	// ErrInvalidIdemKey.Error()
	{
		e := ErrInvalidIdemKey{Msg: "bad idem"}
		if got := e.Error(); got != "bad idem" {
			t.Fatalf("ErrInvalidIdemKey.Error(): got %q want %q", got, "bad idem")
		}
	}

	// ErrInvalidJSON.Error() + Unwrap()
	{
		cause := validate.ErrInvalidJSON
		e := ErrInvalidJSON{Cause: cause}

		if got := e.Error(); got != "invalid json" {
			t.Fatalf("ErrInvalidJSON.Error(): got %q want %q", got, "invalid json")
		}
		if !errors.Is(e, validate.ErrInvalidJSON) {
			t.Fatalf("ErrInvalidJSON should unwrap to validate.ErrInvalidJSON")
		}
		if e.Unwrap() != cause {
			t.Fatalf("ErrInvalidJSON.Unwrap(): got %v want %v", e.Unwrap(), cause)
		}
	}

	// ErrSchemaValidation.Error() + Unwrap()
	{
		cause := errors.New("schema violation")
		e := ErrSchemaValidation{Cause: cause}

		if got := e.Error(); got != "schema validation failed" {
			t.Fatalf("ErrSchemaValidation.Error(): got %q want %q", got, "schema validation failed")
		}
		if !errors.Is(e, cause) {
			t.Fatalf("ErrSchemaValidation should unwrap to cause")
		}
		if e.Unwrap() != cause {
			t.Fatalf("ErrSchemaValidation.Unwrap(): got %v want %v", e.Unwrap(), cause)
		}
	}

	// ErrPayloadTooLarge.Error()
	{
		e := ErrPayloadTooLarge{LimitBytes: 123}
		if got := e.Error(); got != "payload too large" {
			t.Fatalf("ErrPayloadTooLarge.Error(): got %q want %q", got, "payload too large")
		}
	}
}