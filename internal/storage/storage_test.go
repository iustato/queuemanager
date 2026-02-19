package storage

import (
	"context"
	"fmt"      // Добавь эту строку
	"os"
	"testing"
	"time"
)

func TestGC_ShouldNotDeleteProcessingMessage(t *testing.T) {
	// 1. Инициализация временной БД в текущей папке
	dbPath := "test_gc.db"
	// Удаляем старый файл, если он остался от прошлого запуска
	_ = os.Remove(dbPath)

	store, err := Open(OpenOptions{FilePath: dbPath})
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()
	defer os.Remove(dbPath) // Удаляем за собой после теста

	ctx := context.Background()
	msgID := "test_msg_1"
	
	// 2. Кладем сообщение, которое "протухнет" через 50 миллисекунд (очень скоро)
	expiresAt := time.Now().UnixMilli() + 50
	_, _, err = store.PutNewMessage(ctx, "default", msgID, []byte("payload"), "idem_1", 0, expiresAt)
	if err != nil {
		t.Fatalf("Failed to put message: %v", err)
	}

	// 3. Переводим в статус Processing
	// Это вешает Lease (аренду) и защищает сообщение
	err = store.MarkProcessing(msgID, 1)
	if err != nil {
		t.Fatalf("Failed to mark processing: %v", err)
	}

	// Ждем 100 мс, чтобы время TTL (expiresAt) гарантированно прошло
	time.Sleep(100 * time.Millisecond)

	// 4. Запускаем GC
	// Мы передаем текущее время. TTL сообщения уже в прошлом.
	now := time.Now().UnixMilli()
	deleted, err := store.GC(now, 0)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	// ПРОВЕРКИ
	if deleted != 0 {
		t.Errorf("GC deleted a processing message! Expected 0 deletions, got %d", deleted)
	}

	// Проверяем, что сообщение всё еще в базе и его статус не изменился
	meta, _, err := store.GetByMsgID(msgID)
	if err != nil {
		t.Fatalf("Message was deleted from meta bucket, but it should stay: %v", err)
	}

	if meta.Status != StatusProcessing {
		t.Errorf("Message status changed. Expected %s, got %s", StatusProcessing, meta.Status)
	}
	
	t.Log("Success: GC ignored the processing message despite expired TTL")
}

func TestRequeue_ShouldRecoverStuckMessage(t *testing.T) {
	dbPath := "test_requeue.db"
	_ = os.Remove(dbPath)
	store, _ := Open(OpenOptions{FilePath: dbPath})
	defer os.Remove(dbPath)
	defer store.Close()

	ctx := context.Background()
	msgID := "stuck_msg_1"

	// 1. Создаем новое сообщение
	store.PutNewMessage(ctx, "default", msgID, []byte("payload"), "idem_retry", 0, 0)

	// 2. Помечаем как Processing (ставим очень короткий таймаут для теста, 
    // но в реальности используем системный)
	store.MarkProcessing(msgID, 1)

	// Имитируем, что время аренды (LeaseUntilMs) вышло.
	// В реальном MarkProcessing мы ставим +120сек, 
    // поэтому для теста мы "промотаем" время или подождем.
	// Чтобы тест был быстрым, мы вызовем RequeueStuck с "будущим" временем.
	
	now := time.Now().UnixMilli()
	futureTime := now + 130_000 // Сдвигаем время на 130 секунд вперед

	// 3. Запускаем восстановление
	requeued, err := store.RequeueStuck(futureTime, 100)
	if err != nil {
		t.Fatalf("RequeueStuck failed: %v", err)
	}

	if requeued != 1 {
		t.Errorf("Expected 1 message to be requeued, got %d", requeued)
	}

	// 4. Проверяем, что статус снова Queued
	meta, _, _ := store.GetByMsgID(msgID)
	if meta.Status != StatusQueued {
		t.Errorf("Message should be Queued, but got %s", meta.Status)
	}
    
    t.Log("Success: Stuck message was successfully recovered to Queued status")
}

func TestRequeue_BatchLimit(t *testing.T) {
	dbPath := "test_batch.db"
	_ = os.Remove(dbPath)
	store, _ := Open(OpenOptions{FilePath: dbPath})
	defer os.Remove(dbPath)
	defer store.Close()

	// Создаем 20 сообщений в статусе Processing
	for i := 0; i < 20; i++ {
		msgID := fmt.Sprintf("batch-msg-%d", i)
		store.PutNewMessage(context.Background(), "default", msgID, []byte("data"), "", 0, 0)
		store.MarkProcessing(msgID, 1)
	}

	// Имитируем, что всё зависло (время в будущем)
	futureTime := time.Now().UnixMilli() + 500_000

	// Просим восстановить только 5 штук
	limit := 5
	requeued, err := store.RequeueStuck(futureTime, limit)
	if err != nil {
		t.Fatalf("RequeueStuck failed: %v", err)
	}

	if requeued != limit {
		t.Errorf("Expected exactly %d messages to be requeued, got %d", limit, requeued)
	}
	
	t.Logf("Success: RequeueStuck respected batch limit %d", limit)
}

func TestGC_ShouldCleanupIdempotency(t *testing.T) {
	dbPath := "test_gc_idem.db"
	_ = os.Remove(dbPath)
	store, _ := Open(OpenOptions{FilePath: dbPath})
	defer os.Remove(dbPath)
	defer store.Close()

	ctx := context.Background()
	msgID := "msg-to-delete"
	idemKey := "action-42"

	// 1. Создаем сообщение с TTL, который истекает сейчас
	expiresAt := time.Now().UnixMilli() - 1000 
	store.PutNewMessage(ctx, "default", msgID, []byte("data"), idemKey, 0, expiresAt)
    // Убеждаемся, что статус не Processing (иначе GC не удалит)
	store.MarkDone(msgID, StatusSucceeded, Result{}, expiresAt)

	// 2. Запускаем GC
	deleted, _ := store.GC(time.Now().UnixMilli(), 0)
	if deleted != 1 {
		t.Fatalf("GC should have deleted the message")
	}

	// 3. ПРОВЕРКА: Пробуем снова создать сообщение с ТЕМ ЖЕ ключом идемпотентности
	// Если GC не удалил ключ из bIdem, этот Put вернет created=false
	_, created, err := store.PutNewMessage(ctx, "default", "new-msg", []byte("data"), idemKey, 0, 0)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if !created {
		t.Error("Idempotency key was not cleaned up by GC! Cannot reuse key.")
	} else {
		t.Log("Success: GC cleaned up both message and its idempotency key")
	}
}

func TestLease_ShouldStayLockedUntilTimeout(t *testing.T) {
	dbPath := "test_lease.db"
	_ = os.Remove(dbPath)
	store, _ := Open(OpenOptions{FilePath: dbPath})
	defer os.Remove(dbPath)
	defer store.Close()

	msgID := "lease-msg"
	store.PutNewMessage(context.Background(), "default", msgID, []byte("data"), "", 0, 0)
	store.MarkProcessing(msgID, 1)

	// Проверяем через 1 секунду (таймаут в 2 минуты еще не вышел)
	now := time.Now().UnixMilli() + 1000
	requeued, _ := store.RequeueStuck(now, 0)

	if requeued != 0 {
		t.Error("RequeueStuck recovered message too early! Lease is still active.")
	} else {
		t.Log("Success: Message stayed locked during active lease")
	}
}

func TestTouchProcessing_ShouldExtendLease(t *testing.T) {
    dbPath := "test_touch.db"
    _ = os.Remove(dbPath)
    store, _ := Open(OpenOptions{FilePath: dbPath})
    defer os.Remove(dbPath)
    defer store.Close()

    msgID := "touch-msg"
    store.PutNewMessage(context.Background(), "default", msgID, []byte("data"), "", 0, 0)
    
    // 1. Помечаем как Processing
    store.MarkProcessing(msgID, 1)
    
    // Запоминаем время, когда сообщение должно было "протухнуть" (через 2 минуты)
    expireTimeInitial := time.Now().UnixMilli() + 120_000

    // 2. Делаем Touch через какое-то время
    time.Sleep(50 * time.Millisecond)
    if err := store.TouchProcessing(msgID); err != nil {
        t.Fatalf("Touch failed: %v", err)
    }

    // 3. Проверяем: RequeueStuck в момент первоначального истечения (expireTimeInitial) 
    // НЕ должен забрать сообщение, так как Touch его продлил.
    requeued, _ := store.RequeueStuck(expireTimeInitial, 10)
    if requeued != 0 {
        t.Errorf("Message was requeued but lease should have been extended by Touch")
    }
}

func TestMarkDone_ShouldRemoveFromProcIndex(t *testing.T) {
    dbPath := "test_done_index.db"
    _ = os.Remove(dbPath)
    store, _ := Open(OpenOptions{FilePath: dbPath})
    defer os.Remove(dbPath)
    defer store.Close()

    msgID := "done-msg"
    store.PutNewMessage(context.Background(), "default", msgID, []byte("data"), "", 0, 0)
    store.MarkProcessing(msgID, 1)
    
    // 1. Завершаем задачу
    store.MarkDone(msgID, StatusSucceeded, Result{ExitCode: 0}, 0)

    // 2. Проверяем RequeueStuck спустя время аренды
    future := time.Now().UnixMilli() + 300_000
    requeued, _ := store.RequeueStuck(future, 10)

    if requeued != 0 {
        t.Errorf("RequeueStuck recovered a message that was already MarkDone!")
    }
}

func TestPutNewMessage_IdempotencyCollision(t *testing.T) {
    dbPath := "test_collision.db"
    _ = os.Remove(dbPath)
    store, _ := Open(OpenOptions{FilePath: dbPath})
    defer os.Remove(dbPath)
    defer store.Close()

    idemKey := "same-action"
    id1 := "msg-1"
    id2 := "msg-2"

    // Первый раз
    retID1, created1, _ := store.PutNewMessage(context.Background(), "q", id1, []byte("d"), idemKey, 0, 0)
    // Второй раз с другим msgID, но тем же idemKey
    retID2, created2, _ := store.PutNewMessage(context.Background(), "q", id2, []byte("d"), idemKey, 0, 0)

    if !created1 || retID1 != id1 {
        t.Errorf("First put failed")
    }
    if created2 || retID2 != id1 {
        t.Errorf("Second put should not create new message and should return first msgID. Got ID: %s", retID2)
    }
}