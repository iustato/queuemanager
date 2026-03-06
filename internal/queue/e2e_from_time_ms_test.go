package queue

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestE2E_HTTP_Info_InvalidFromTimeMs_Returns400(t *testing.T) {
	ts, _, cleanup := newE2EServer(t, e2eServerOpts{})
	defer cleanup()

	code, hdr, bodyB, ar := httpGetAPIResp(t, ts.URL+"/info?from_time_ms=abc")
	body := string(bodyB)

	if code != http.StatusBadRequest {
		t.Fatalf("/info invalid from_time_ms: expected 400, got %d; body=%s", code, body)
	}
	if ar.OK {
		t.Fatalf("/info invalid from_time_ms: expected ok=false; body=%s", body)
	}
	if ar.Error == nil {
		t.Fatalf("/info invalid from_time_ms: expected error object; body=%s", body)
	}
	if ar.Error.Code != "invalid_argument" {
		t.Fatalf("/info invalid from_time_ms: expected error.code=invalid_argument, got %q; body=%s", ar.Error.Code, body)
	}

	mustAPIHeaders(t, hdr)
}

func TestE2E_HTTP_QueueInfo_InvalidFromTimeMs_Returns400(t *testing.T) {
	ts, _, cleanup := newE2EServer(t, e2eServerOpts{})
	defer cleanup()

	queue := "queue1"

	code, hdr, bodyB, ar := httpGetAPIResp(t, ts.URL+"/"+queue+"/info?from_time_ms=abc")
	body := string(bodyB)

	if code != http.StatusBadRequest {
		t.Fatalf("/%s/info invalid from_time_ms: expected 400, got %d; body=%s", queue, code, body)
	}
	if ar.OK {
		t.Fatalf("/%s/info invalid from_time_ms: expected ok=false; body=%s", queue, body)
	}
	if ar.Error == nil {
		t.Fatalf("/%s/info invalid from_time_ms: expected error object; body=%s", queue, body)
	}
	if ar.Error.Code != "invalid_argument" {
		t.Fatalf("/%s/info invalid from_time_ms: expected error.code=invalid_argument, got %q; body=%s", queue, ar.Error.Code, body)
	}

	mustAPIHeaders(t, hdr)
}

// httpGetAPIResp: like httpGetAPI but returns headers too.
// Note: We attempt to unmarshal apiResp for both success and error responses.
func httpGetAPIResp(t *testing.T, url string) (status int, hdr http.Header, body []byte, ar apiResp) {
	t.Helper()

	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	body, _ = io.ReadAll(resp.Body)
	hdr = resp.Header.Clone()

	// Try decode envelope for any response that should be API JSON.
	// If it isn't JSON, ar stays zero-value and tests can print body.
	_ = json.Unmarshal(body, &ar)

	return resp.StatusCode, hdr, body, ar
}

func mustAPIHeaders(t *testing.T, hdr http.Header) {
	t.Helper()

	ct := hdr.Get("Content-Type")
	if !strings.HasPrefix(strings.ToLower(ct), "application/json") {
		t.Fatalf("expected Content-Type application/json..., got %q", ct)
	}

	if rid := strings.TrimSpace(hdr.Get("X-Request-Id")); rid == "" {
		t.Fatalf("expected X-Request-Id header to be present")
	}
}