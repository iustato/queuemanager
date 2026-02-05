package queue

import (
	"fmt"
	"sync"

	"go-web-server/internal/config"
	"go-web-server/internal/validate"

	"go.uber.org/zap"
)

type Runtime struct {
	Cfg    config.QueueConfig
	Schema *validate.CompiledSchema
}

type Manager struct {
	mu     sync.RWMutex
	queues map[string]*Runtime
	log    *zap.Logger
}

func NewManager(logger *zap.Logger) *Manager {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Manager{
		queues: make(map[string]*Runtime),
		log:    logger.Named("queue"),
	}
}

func (m *Manager) AddQueue(cfg config.QueueConfig, schema *validate.CompiledSchema) error {
	if cfg.Name == "" {
		return fmt.Errorf("queue name is empty")
	}
	if schema == nil {
		return fmt.Errorf("schema is nil for queue %q", cfg.Name)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.queues[cfg.Name]; exists {
		return fmt.Errorf("queue %q already exists", cfg.Name)
	}

	m.queues[cfg.Name] = &Runtime{Cfg: cfg, Schema: schema}

	// (опционально) лог о регистрации очереди
	m.log.Info("queue_registered",
		zap.String("queue", cfg.Name),
		zap.String("schema_file", cfg.SchemaFile),
	)

	return nil
}

func (m *Manager) Get(name string) (*Runtime, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rt, ok := m.queues[name]
	return rt, ok
}

func (m *Manager) ListNames() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]string, 0, len(m.queues))
	for name := range m.queues {
		out = append(out, name)
	}
	return out
}

func (m *Manager) ReplaceQueue(cfg config.QueueConfig, schema *validate.CompiledSchema) error {
	if cfg.Name == "" {
		return fmt.Errorf("queue name is empty")
	}
	if schema == nil {
		return fmt.Errorf("schema is nil for queue %q", cfg.Name)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.queues[cfg.Name] = &Runtime{Cfg: cfg, Schema: schema}

	m.log.Info("queue_replaced",
		zap.String("queue", cfg.Name),
		zap.String("schema_file", cfg.SchemaFile),
	)

	return nil
}

func (m *Manager) DeleteQueue(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.queues, name)

	m.log.Warn("queue_deleted", zap.String("queue", name))
}
