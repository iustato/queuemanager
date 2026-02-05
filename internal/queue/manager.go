package queue

import (
	"fmt"
	"sync"

	"go-web-server/internal/config"
	"go-web-server/internal/validate"
)

// Runtime  состояние очереди в памяти.

type Runtime struct {
	Cfg config.QueueConfig

	// Schema компилируется один раз при старте сервиса
		
	Schema *validate.CompiledSchema

	// TODO: здесь позже появится реальная очередь/хранилище/воркеры:
	// Queue   *InMemoryQueue или интерфейс Queue
	// Store   storage.Store
	// Pool    *WorkerPool
}

// Manager владеет всеми очередями и их runtime-состоянием.
// Он создаётся в main() и живёт столько же, сколько процесс.
//
// Потокобезопасность нужна, потому что handler-ы читают map параллельно.
// Если ты не делаешь горячую перезагрузку — записи будут только при старте,
// но read-lock всё равно полезен и дешевый.
type Manager struct {
	mu     sync.RWMutex
	queues map[string]*Runtime
}

// NewManager создаёт пустой менеджер очередей.
// Отдельный конструктор полезен, чтобы гарантировать инициализацию map
// и скрыть детали реализации от main/httpserver.
func NewManager() *Manager {
	return &Manager{
		queues: make(map[string]*Runtime),
	}
}

// AddQueue регистрирует очередь в менеджере.
//
// Здесь НЕ должно быть IO (чтения файлов) — все тяжёлое делается раньше:
//  - config.LoadQueueConfigs
//  - validate.LoadSchemaFromFile (compile)
//
// AddQueue отвечает только за консистентность структуры в памяти.
func (m *Manager) AddQueue(cfg config.QueueConfig, schema *validate.CompiledSchema) error {
	if cfg.Name == "" {
		return fmt.Errorf("queue name is empty")
	}
	if schema == nil {
		return fmt.Errorf("schema is nil for queue %q", cfg.Name)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Защита от конфигов с одинаковым именем очереди
	if _, exists := m.queues[cfg.Name]; exists {
		return fmt.Errorf("queue %q already exists", cfg.Name)
	}

	m.queues[cfg.Name] = &Runtime{
		Cfg:    cfg,
		Schema: schema,
	}

	return nil
}

// Get возвращает runtime очереди по имени.
// Это самый частый путь в рантайме — вызывается из HTTP handler-а.
//
// Возвращаем (*Runtime, bool), чтобы не полагаться на nil и
// корректно отличать "очереди нет" от "очередь есть, но runtime nil" (что мы не допускаем).
func (m *Manager) Get(name string) (*Runtime, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rt, ok := m.queues[name]
	return rt, ok
}

// ListNames полезен для дебага и для будущего эндпоинта типа GET /queues.
func (m *Manager) ListNames() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]string, 0, len(m.queues))
	for name := range m.queues {
		out = append(out, name)
	}
	return out
}

// ReplaceQueue понадобится, если ты решишь делать горячую перезагрузку конфигов.
// Тогда runtime можно "атомарно" заменить (под write-lock),
// а читающие handler-ы продолжат получать консистентные ссылки.
//
// Сейчас не используется, но оставлено как задел.
func (m *Manager) ReplaceQueue(cfg config.QueueConfig, schema *validate.CompiledSchema) error {
	if cfg.Name == "" {
		return fmt.Errorf("queue name is empty")
	}
	if schema == nil {
		return fmt.Errorf("schema is nil for queue %q", cfg.Name)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.queues[cfg.Name] = &Runtime{
		Cfg:    cfg,
		Schema: schema,
	}
	return nil
}

// DeleteQueue — опасная операция, обычно запрещена в проде во время работы.
// Если когда-нибудь понадобится — надо решать, что делать с уже принятыми сообщениями,
// воркерами и storage.
func (m *Manager) DeleteQueue(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.queues, name)
}
