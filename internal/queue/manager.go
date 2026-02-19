package queue

import (
	"errors"
	"fmt"
	"sync"
	"time"
	"context"


	"go-web-server/internal/config"
	"go-web-server/internal/validate"
	"go-web-server/internal/storage"

	"go.uber.org/zap"
)

type Manager struct {
	mu     sync.RWMutex
	queues map[string]*Runtime
	log    *zap.Logger
	store *storage.Store
}

func NewManager(logger *zap.Logger) *Manager {
	return NewManagerWithStore(logger, nil)
}


func NewManagerWithStore(logger *zap.Logger, st *storage.Store) *Manager {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Manager{
		queues: make(map[string]*Runtime),
		log:    logger.Named("queue"),
		store:  st,
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
	if _, exists := m.queues[cfg.Name]; exists {
		m.mu.Unlock()
		return fmt.Errorf("queue %q already exists", cfg.Name)
	}

	rt := &Runtime{
		Cfg:    cfg,
		Schema: schema,
		Store:  m.store,
	}

	// Значения из конфигурации YAML
	rt.Command = []string{cfg.Runtime}
	rt.ScriptPath = cfg.Script

	// защита от зависания
	if rt.Cfg.TimeoutSec <= 0 {
		rt.Cfg.TimeoutSec = 10
	}
	if rt.Cfg.MaxQueue <= 0 {
		rt.Cfg.MaxQueue = 100
	}

	m.queues[cfg.Name] = rt
	m.mu.Unlock()

	// СНАЧАЛА лог регистрации (теперь он будет перед queue_started)
	m.log.Info("queue_registered",
		zap.String("queue", cfg.Name),
		zap.String("schema_file", cfg.SchemaFile),
		zap.Int("workers", cfg.Workers),
		zap.Int("max_size", cfg.MaxSize),
		zap.String("runtime", cfg.Runtime),
		zap.String("script", cfg.Script),
		zap.Int("timeout_sec", cfg.TimeoutSec),
		zap.Int("max_queue", cfg.MaxQueue),
	)

	// ПОТОМ старт
	if err := rt.initIfNeeded(m.log); err != nil {
		m.mu.Lock()
		delete(m.queues, cfg.Name)
		m.mu.Unlock()

		m.log.Error("queue_start_failed",
			zap.String("queue", cfg.Name),
			zap.Error(err),
		)
		return err
	}

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
	return out//возвращает список имен очередей по ключу
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

	if old, ok := m.queues[cfg.Name]; ok && old != nil {
		old.Stop()
	}

	rt := &Runtime{
		Cfg:    cfg,
		Schema: schema,
		Store:  m.store, 
	}

	if err := rt.initIfNeeded(m.log); err != nil {
		//защита от подвешенного состояния
		delete(m.queues, cfg.Name) 

        return fmt.Errorf("failed to replace queue %q: %w", cfg.Name, err)
	}

	m.queues[cfg.Name] = rt

	m.log.Info("queue_replaced",
		zap.String("queue", cfg.Name),
		zap.String("schema_file", cfg.SchemaFile),
	)

	return nil
}

func (m *Manager) DeleteQueue(name string) {
	m.mu.Lock()
	rt := m.queues[name]
	delete(m.queues, name)
	m.mu.Unlock()
	
	//если очередь была удалена ранее
	if rt != nil {
		rt.Stop()
	}

	m.log.Warn("queue_deleted", zap.String("queue", name))
}

var ErrUnknownQueue = errors.New("unknown queue")

func (m *Manager) SetCommand(queueName string, cmd []string) error {
    m.mu.Lock()
    rt, ok := m.queues[queueName]
    m.mu.Unlock() 

    if !ok || rt == nil {
        return ErrUnknownQueue
    }

    //Блокируем конкретный Runtime для записи команды
    rt.mu.Lock() 
    rt.Command = cmd
    rt.mu.Unlock()

    m.log.Info("queue_command_set",
        zap.String("queue", queueName),
        zap.String("cmd", safeCmd(cmd)),
    )
    return nil
}

// Добавили ctx в аргументы, чтобы передать его дальше в rt.Enqueue
func (m *Manager) Enqueue(ctx context.Context, queueName, msgID string, body []byte) error {
    m.mu.RLock()
    rt, ok := m.queues[queueName]
    m.mu.RUnlock()

    if !ok || rt == nil {
        return ErrUnknownQueue
    }

    job := Job{
        Queue:      queueName,
        MsgID:      msgID,
        Body:       body,
        EnqueuedAt: time.Now(),
        Attempt:    1,
    }

    // ИСПРАВЛЕНО: передаем значения ctx и job, а не типы
    return rt.Enqueue(ctx, job)
}

func (m *Manager) SetScript(queueName, scriptPath string) error {
    m.mu.Lock()
    rt, ok := m.queues[queueName]
    m.mu.Unlock()

    if !ok || rt == nil {
        return ErrUnknownQueue
    }

    rt.mu.Lock() 
    rt.ScriptPath = scriptPath
    rt.mu.Unlock()

    m.log.Info("queue_script_set",
        zap.String("queue", queueName),
        zap.String("script", scriptPath),
    )
    return nil
}

func (m *Manager) StopAll() {
	m.mu.RLock()
	rts := make([]*Runtime, 0, len(m.queues))
	for _, rt := range m.queues {
		if rt != nil {
			rts = append(rts, rt)
		}
	}
	m.mu.RUnlock()

	for _, rt := range rts {
		rt.Stop()
	}
}
