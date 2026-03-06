package httpserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap/zapcore"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestAccessLog_LogsMethodPathStatusAndRequestID(t *testing.T) {
	t.Parallel()

	// observer logger: перехватываем логи вместо stdout
	core, obs := observer.New(zap.InfoLevel)
	log := zap.New(core)

	// финальный handler — отдаём явный статус и тело, чтобы AccessLog смог их залогировать
	final := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("accepted"))
	})

	// Важно: в проде у тебя порядок такой:
	// publicHandler = RequestID(publicHandler)
	// publicHandler = AccessLog(logger, publicHandler)
	//
	// То есть AccessLog оборачивает RequestID.
	h := AccessLog(log, RequestID(final))

	req := httptest.NewRequest(http.MethodPost, "http://example.com/queue1/newmessage", strings.NewReader(`{"text":"hi"}`))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "127.0.0.1:54321" // чтобы SplitHostPort отработал

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	// 1) RequestID middleware должен поставить заголовок
	rid := strings.TrimSpace(rr.Header().Get("X-Request-Id"))
	if rid == "" {
		t.Fatalf("expected X-Request-Id header to be present")
	}

	// 2) AccessLog должен записать 1 лог-запись
	entries := obs.All()
	if len(entries) != 1 {
		t.Fatalf("expected 1 log entry, got %d", len(entries))
	}

	e := entries[0]
	fields := fieldsToMap(e.Context)

	// method
	if got := fields["method"]; got != http.MethodPost {
		t.Fatalf("expected method=%q, got %q (fields=%v)", http.MethodPost, got, fields)
	}

	// path
	if got := fields["path"]; got != "/queue1/newmessage" {
		t.Fatalf("expected path=%q, got %q (fields=%v)", "/queue1/newmessage", got, fields)
	}

	// status
	// (в zap observer numbers приходят как float64)
	if got := fields["status"]; got != "202" {
		t.Fatalf("expected status=202, got %q (fields=%v)", got, fields)
	}

	// request id: ключ у тебя может быть request_id или rid — проверяем оба
	gotRID := fields["request_id"]
	if gotRID == "" {
		gotRID = fields["rid"]
	}
	if gotRID == "" {
		t.Fatalf("expected request id field (request_id or rid) to be present (fields=%v)", fields)
	}
	if gotRID != rid {
		t.Fatalf("expected request id in log to match header: header=%q log=%q (fields=%v)", rid, gotRID, fields)
	}
}

// helper: превращаем zap fields в map[string]string для удобных ассертов
func fieldsToMap(fs []zap.Field) map[string]string {
	out := make(map[string]string, len(fs))
	for _, f := range fs {
		switch f.Type {
		case zapcore.StringType:
			out[f.Key] = f.String
		case zapcore.Int64Type:
			out[f.Key] = itoa64(f.Integer)
		case zapcore.Int32Type:
			out[f.Key] = itoa64(f.Integer)
		case zapcore.Uint64Type:
			out[f.Key] = itoa64(f.Integer)
		case zapcore.Float64Type:
			out[f.Key] = formatFloat(f.Integer) // редко, но на всякий
		default:
			// у observer числа часто идут как Reflect/Interface — ниже мы поймаем через Encode.
			// Для простоты оставим пустым — тест проверяет ключи, которые обычно string/int.
		}

		// В zaptest/observer числовые значения чаще лежат в f.Integer,
		// а статус/bytes могут быть закодированы иначе. Подстрахуемся:
		if _, ok := out[f.Key]; !ok && f.Interface != nil {
			if v, ok2 := f.Interface.(string); ok2 {
				out[f.Key] = v
			}
		}
	}
	// Если у тебя status логируется как int (обычно так), то он попадёт через Integer.
	// Но если он логируется как zap.Int("status", ...), observer его увидит как Integer — ок.

	// Нормализуем status если он случайно попал как "" (бывает при нестандартном Field Type).
	// В таком случае тест выше упадёт и ты увидишь fields=... и подправим ключ/тип точечно.
	return out
}

// --- минимальные helpers без fmt чтобы тест был лёгкий ---

// itoa64 делает простую конвертацию int64→string без fmt.
func itoa64(x int64) string {
	// достаточно для маленьких чисел (status, bytes)
	if x == 0 {
		return "0"
	}
	neg := x < 0
	if neg {
		x = -x
	}
	var buf [32]byte
	i := len(buf)
	for x > 0 {
		i--
		buf[i] = byte('0' + x%10)
		x /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// formatFloat — заглушка; в этом тесте не критично.
func formatFloat(_ int64) string { return "" }