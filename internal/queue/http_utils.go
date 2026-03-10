package queue

import (
	"encoding/json"
	"net/http"

	httpserver "go-web-server/internal/httpserver"
)

type apiResponse struct {
	OK        bool      `json:"ok"`
	Data      any       `json:"data,omitempty"`
	Error     *apiError `json:"error,omitempty"`
	RequestID string    `json:"request_id,omitempty"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeAPIOK(w http.ResponseWriter, r *http.Request, status int, data any) {
	writeJSON(w, status, apiResponse{
		OK:        true,
		Data:      data,
		RequestID: httpserver.GetRequestID(r.Context()),
	})
}

func writeAPIError(w http.ResponseWriter, r *http.Request, status int, code, message string, details any) {
	writeJSON(w, status, apiResponse{
		OK: false,
		Error: &apiError{
			Code:    code,
			Message: message,
			Details: details,
		},
		RequestID: httpserver.GetRequestID(r.Context()),
	})
}
