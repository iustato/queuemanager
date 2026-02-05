package queue

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

// HandleNewMessage обрабатывает запрос вида:
// POST /{queue}/newmessage
//
// Требования:
//  - {queue} должен существовать (загружен из конфигов при старте)
//  - Header: X-Message-UUID обязателен (UUIDv7)
//  - Body: валидный JSON
//  - JSON должен пройти валидацию по JSON Schema очереди
func (m *Manager) HandleNewMessage(queueName string, w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// 1) Достаём runtime очереди из памяти.	
	rt, ok := m.Get(queueName)
	if !ok {
		http.Error(w, "unknown queue", http.StatusNotFound)
		return
	}

	// 2) Проверяем message id из заголовка.

	msgID := strings.TrimSpace(r.Header.Get("X-Message-UUID"))
	if msgID == "" {
		http.Error(w, "missing X-Message-UUID header", http.StatusBadRequest)
		return
	}

	// TODO: здесь стоит провалидировать формат UUIDv7.
	// Например, через библиотеку UUID, и при ошибке вернуть 400.
	// Сейчас оставляем как заглушку:
	if !looksLikeUUID(msgID) {
		http.Error(w, "invalid X-Message-UUID format", http.StatusBadRequest)
		return
	}

	// 3) Читаем тело запроса.
	// Значение лимита можно вынести в общий конфиг или в cfg очереди.
	const maxBodyBytes = 1 << 20 // 1 MiB
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		// Если превышен maxBodyBytes — MaxBytesReader вернёт ошибку.
		http.Error(w, "cannot read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// 4) JSON в any для гибкости
	//содержимое сообщения клиента

	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	// 5) Валидация по JSON Schema.
	// Схема уже скомпилирована при старте приложения и лежит в rt.Schema.
	if err := rt.Schema.ValidateJSON(payload); err != nil {
		// 422 — корректный код для "синтаксис JSON ок, но структура не подходит"
		http.Error(w, "schema validation failed: "+err.Error(), http.StatusUnprocessableEntity)
		return
	}

	// 6) Постановка сообщения в очередь (заглушка).
	// Здесь позже появится:
	//  - дедупликация по msgID
	//  - запись в storage (bbolt) / память
	//  - возврат статуса/позиции/ack
	//
	// Примерно:
	//   err = rt.Queue.Enqueue(msgID, payload)
	//   if err == ErrQueueFull -> 429 или 409
	//   if err == ErrDuplicate -> 409
	_ = start // можно использовать в логах duration_ms

	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("accepted"))
}

// looksLikeUUID — очень грубая проверка формата.
// Это НЕ UUIDv7-парсер, а только быстрая фильтрация мусора.
// Лучше заменить на нормальный парсинг библиотекой.
func looksLikeUUID(s string) bool {
	// минимально ожидаем: 36 символов с дефисами (UUID text format)
	// xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
	if len(s) != 36 {
		return false
	}
	// проверяем позиции дефисов
	for _, i := range []int{8, 13, 18, 23} {
		if s[i] != '-' {
			return false
		}
	}
	// остальное — hex
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

// (Опционально) пример того, как можно отличать ошибки тела запроса,
// если захочешь более точные ответы. Сейчас не используется.
func isTooLargeBody(err error) bool {
	// http.MaxBytesReader может возвращать разные ошибки — тут оставлено как идея.
	return errors.Is(err, http.ErrBodyReadAfterClose)
}
