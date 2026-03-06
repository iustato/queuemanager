package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"os"
	"strings"

	"go-web-server/internal/config"
	"go-web-server/internal/storage"
	"go-web-server/internal/validate"

	"go.uber.org/zap"
)

type Manager struct {
	mu     sync.RWMutex
	queues map[string]*Runtime
	log    *zap.Logger
	store  *storage.Store

	workerToken   string
	allowAutoIdem bool
	service       *QueueService
}

func NewManager(logger *zap.Logger) *Manager {
	return NewManagerWithStore(logger, nil)
}

func NewManagerWithStore(logger *zap.Logger, st *storage.Store) *Manager {
	if logger == nil {
		logger = zap.NewNop()
	}

	m := &Manager{
		queues: make(map[string]*Runtime),
		log:    logger.Named("queue"),
		store:  st,
	}

	// читаем env один раз
	m.workerToken = os.Getenv("WORKER_TOKEN")
	m.allowAutoIdem = os.Getenv("ALLOW_AUTO_IDEMPOTENCY") == "true"

	// сервис тоже лучше инициализировать тут
	m.service = NewQueueService(m)

	return m
}

func (m *Manager) AddQueue(cfg config.QueueConfig, schema *validate.CompiledSchema) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.queues[cfg.Name]; exists {
		return fmt.Errorf("queue %q already exists", cfg.Name)
	}

	rtStr := strings.TrimSpace(cfg.Runtime)

	// "новые" допустимые значения runtime-kind
	isKind := rtStr == string(RuntimePHPFPM) ||
		rtStr == string(RuntimePHPCGI) ||
		rtStr == string(RuntimeExec)

	// Команда и scriptPath, которые реально пойдут в NewRuntime
	cmd := cfg.Command
	scriptPath := cfg.Script

	// BACKWARD COMPAT:
	// если runtime не kind и command не задана — runtime был командой (например "true" или "/bin/true")
	if !isKind && len(cmd) == 0 && rtStr != "" {
		cmd = []string{rtStr}
		cfg.Runtime = string(RuntimeExec)
		scriptPath = "" // для exec не нужен
	}

	rt, err := NewRuntime(
		cfg,
		m.store,
		m.log,
		schema,
		cmd,
		scriptPath,
		cfg.FPMNetwork,
		cfg.FPMAddress,
	)
	if err != nil {
		return err
	}

	m.queues[cfg.Name] = rt

	m.log.Info("queue_registered_and_started",
		zap.String("queue", cfg.Name),
		zap.Int("workers", cfg.Workers),
		zap.String("runtime", cfg.Runtime),
		zap.Strings("command", cmd),
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
	m.mu.Lock()
	defer m.mu.Unlock()

	// Останавливаем старую очередь если есть
	if old, ok := m.queues[cfg.Name]; ok && old != nil {
		old.Stop()
	}

	rtStr := strings.TrimSpace(cfg.Runtime)

	isKind := rtStr == string(RuntimePHPFPM) ||
		rtStr == string(RuntimePHPCGI) ||
		rtStr == string(RuntimeExec)

	cmd := cfg.Command
	scriptPath := cfg.Script

	// backward compat: runtime был командой (например "true")
	if !isKind && len(cmd) == 0 && rtStr != "" {
		cmd = []string{rtStr}
		cfg.Runtime = string(RuntimeExec)
		scriptPath = ""
	}

	// создаем runtime
	rt, err := NewRuntime(
		cfg,
		m.store,
		m.log,
		schema,
		cmd,
		scriptPath,
		cfg.FPMNetwork,
		cfg.FPMAddress,
	)
	if err != nil {
		return err
	}

	m.queues[cfg.Name] = rt
	return nil
}

func (m *Manager) DeleteQueue(name string) {
	m.mu.Lock()
	rt, ok := m.queues[name]
	delete(m.queues, name)
	m.mu.Unlock()

	if ok && rt != nil {
		rt.Stop()
	}
	m.log.Warn("queue_deleted", zap.String("queue", name))
}

var ErrUnknownQueue = errors.New("unknown queue")

// ВНИМАНИЕ: В новом дизайне Runtime поля Command и ScriptPath неизменяемы (нет мьютекса mu внутри Runtime).
// Чтобы изменить команду, нужно вызвать ReplaceQueue. 
// Если это критично, нужно вернуть mu в Runtime, но лучше пересоздавать очередь.

func (m *Manager) SetCommand(queueName string, cmd []string) error {
	// Для поддержки этого метода нужно либо возвращать mu в Runtime, 
	// либо делать полную замену Runtime через ReplaceQueue.
	return errors.New("SetCommand is deprecated, use ReplaceQueue")
}

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

	return rt.Enqueue(ctx, job)
}

func (m *Manager) StopAll() {
	m.mu.RLock()
	// Копируем указатели, чтобы не держать замок менеджера во время остановок
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