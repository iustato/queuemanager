package queue

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"go-web-server/internal/config"
	"go-web-server/internal/storage"
	"go-web-server/internal/validate"

	"go.uber.org/zap"
)

func testRuntimeCmd() string {
	// Важно: AddQueue/ReplaceQueue передают []string{cfg.Runtime} в NewRuntime.
	// Нам нужно, чтобы команда существовала, но не обязательно запускалась (workers=0).
	if runtime.GOOS == "windows" {
		return "cmd"
	}
	// В Linux/WSL почти всегда есть /usr/bin/true
	return "true"
}

func TestManager_NewManager_NonNilFields(t *testing.T) {
	m := NewManager(nil, t.TempDir(), storage.OpenOptions{})
	if m == nil {
		t.Fatalf("expected non-nil manager")
	}
	if m.log == nil {
		t.Fatalf("expected logger initialized")
	}
	if m.service == nil {
		t.Fatalf("expected service initialized")
	}
}

func TestManager_AddQueue_Duplicate(t *testing.T) {
	m := NewManager(zap.NewNop(), t.TempDir(), storage.OpenOptions{})

	cfg := config.QueueConfig{
		Name:    "q1",
		Workers: 0, // чтобы не поднимать воркеры в тесте
		Runtime: testRuntimeCmd(),
		Script:  "dummy.php",
	}
	var schema *validate.CompiledSchema = nil

	if err := m.AddQueue(cfg, schema); err != nil {
		t.Fatalf("AddQueue #1: %v", err)
	}
	if err := m.AddQueue(cfg, schema); err == nil {
		t.Fatalf("expected error on duplicate queue name, got nil")
	}
}

func TestManager_ListNames_Get_DeleteQueue(t *testing.T) {
	m := NewManager(zap.NewNop(), t.TempDir(), storage.OpenOptions{})

	cfg := config.QueueConfig{
		Name:    "q1",
		Workers: 0,
		Runtime: testRuntimeCmd(),
		Script:  "dummy.php",
	}
	if err := m.AddQueue(cfg, nil); err != nil {
		t.Fatalf("AddQueue: %v", err)
	}

	// Get
	if rt, ok := m.Get("q1"); !ok || rt == nil {
		t.Fatalf("expected q1 to exist")
	}

	// ListNames
	names := m.ListNames()
	if len(names) != 1 || names[0] != "q1" {
		t.Fatalf("ListNames: got %#v want [q1]", names)
	}

	// DeleteQueue должен удалить и не паниковать
	m.DeleteQueue("q1")
	if _, ok := m.Get("q1"); ok {
		t.Fatalf("expected q1 deleted")
	}
}

func TestManager_DeleteQueue_RemovesDBFile(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(zap.NewNop(), dir, storage.OpenOptions{})

	cfg := config.QueueConfig{
		Name:    "q1",
		Workers: 0,
		Runtime: testRuntimeCmd(),
		Script:  "dummy.php",
	}
	if err := m.AddQueue(cfg, nil); err != nil {
		t.Fatalf("AddQueue: %v", err)
	}

	dbPath := filepath.Join(dir, "q1.db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatalf("expected %s to exist after AddQueue", dbPath)
	}

	m.DeleteQueue("q1")

	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatalf("expected %s to be removed after DeleteQueue, err=%v", dbPath, err)
	}
}

func TestManager_ReplaceQueue_Replaces(t *testing.T) {
	m := NewManager(zap.NewNop(), t.TempDir(), storage.OpenOptions{})

	cfg1 := config.QueueConfig{
		Name:    "q1",
		Workers: 0,
		Runtime: testRuntimeCmd(),
		Script:  "dummy.php",
	}
	if err := m.AddQueue(cfg1, nil); err != nil {
		t.Fatalf("AddQueue: %v", err)
	}
	rt1, _ := m.Get("q1")
	if rt1 == nil {
		t.Fatalf("expected rt1 != nil")
	}

	cfg2 := cfg1
	cfg2.Workers = 0 // остаёмся без воркеров
	// можно поменять Script/Runtime, но это не обязательно
	if err := m.ReplaceQueue(cfg2, nil); err != nil {
		t.Fatalf("ReplaceQueue: %v", err)
	}

	rt2, _ := m.Get("q1")
	if rt2 == nil {
		t.Fatalf("expected rt2 != nil")
	}
	if rt2 == rt1 {
		t.Fatalf("expected runtime pointer replaced")
	}
}

func TestManager_SetCommand_Deprecated(t *testing.T) {
	m := NewManager(zap.NewNop(), t.TempDir(), storage.OpenOptions{})

	err := m.SetCommand("q1", []string{"x"})
	if err == nil {
		t.Fatalf("expected error for deprecated SetCommand")
	}
}

func TestManager_Enqueue_UnknownQueue(t *testing.T) {
	m := NewManager(zap.NewNop(), t.TempDir(), storage.OpenOptions{})

	err := m.Enqueue(context.Background(), "missing", "m1", []byte("x"))
	if err == nil {
		t.Fatalf("expected error")
	}
	if err != ErrUnknownQueue {
		t.Fatalf("expected ErrUnknownQueue, got %v", err)
	}
}

func TestManager_StopAll_NoPanic(t *testing.T) {
	m := NewManager(zap.NewNop(), t.TempDir(), storage.OpenOptions{})

	// добавим пару очередей
	for _, q := range []string{"q1", "q2"} {
		cfg := config.QueueConfig{
			Name:    q,
			Workers: 0,
			Runtime: testRuntimeCmd(),
			Script:  "dummy.php",
		}
		if err := m.AddQueue(cfg, nil); err != nil {
			t.Fatalf("AddQueue %s: %v", q, err)
		}
	}

	// Главное — не паникнуть/не зависнуть
	m.StopAll()
}