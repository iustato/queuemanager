package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"os"
	"path/filepath"
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

	storageDir  string
	storageOpts storage.OpenOptions

	workerToken   string
	allowAutoGUID bool
	phpCgiBin     string
	service       *QueueService
}

func NewManager(logger *zap.Logger, storageDir string, opts storage.OpenOptions, workerToken string, allowAutoGUID bool, phpCgiBin string) *Manager {
	if logger == nil {
		logger = zap.NewNop()
	}

	m := &Manager{
		queues:        make(map[string]*Runtime),
		log:           logger.Named("queue"),
		storageDir:    storageDir,
		storageOpts:   opts,
		workerToken:   workerToken,
		allowAutoGUID: allowAutoGUID,
		phpCgiBin:     phpCgiBin,
	}

	// сервис тоже лучше инициализировать тут
	m.service = NewQueueService(m)

	return m
}

// openStoreForQueue opens a per-queue BoltDB file in storageDir.
func (m *Manager) openStoreForQueue(queueName string) (*storage.Store, error) {
	opts := m.storageOpts
	opts.FilePath = filepath.Join(m.storageDir, queueName+".db")

	if err := os.MkdirAll(m.storageDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir storage dir %s: %w", m.storageDir, err)
	}

	return storage.Open(opts)
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

	// open per-queue storage
	st, err := m.openStoreForQueue(cfg.Name)
	if err != nil {
		return fmt.Errorf("open storage for queue %q: %w", cfg.Name, err)
	}

	rt, err := NewRuntime(
		cfg,
		st,
		m.log,
		schema,
		cmd,
		scriptPath,
		cfg.FPMNetwork,
		cfg.FPMAddress,
		WithPhpCgiBin(m.phpCgiBin),
	)
	if err != nil {
		_ = st.Close()
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

// ReplaceQueue stops the old runtime and creates a new one with the given config.
// The per-queue .db file is preserved — existing messages and
// expiration indices carry over to the new runtime.
func (m *Manager) ReplaceQueue(cfg config.QueueConfig, schema *validate.CompiledSchema) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Останавливаем старую очередь если есть
	if old, ok := m.queues[cfg.Name]; ok && old != nil {
		old.Stop()
		if oldStore, ok := old.Store.(interface{ Close() error }); ok {
			_ = oldStore.Close()
		}
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

	// open per-queue storage
	st, err := m.openStoreForQueue(cfg.Name)
	if err != nil {
		return fmt.Errorf("open storage for queue %q: %w", cfg.Name, err)
	}

	// создаем runtime
	rt, err := NewRuntime(
		cfg,
		st,
		m.log,
		schema,
		cmd,
		scriptPath,
		cfg.FPMNetwork,
		cfg.FPMAddress,
		WithPhpCgiBin(m.phpCgiBin),
	)
	if err != nil {
		_ = st.Close()
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
		if st, ok := rt.Store.(interface{ Close() error }); ok {
			_ = st.Close()
		}
	}

	// remove the per-queue .db file from disk
	dbPath := filepath.Join(m.storageDir, name+".db")
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		m.log.Error("failed to remove queue db file",
			zap.String("queue", name),
			zap.String("path", dbPath),
			zap.Error(err),
		)
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

func (m *Manager) Enqueue(ctx context.Context, queueName, guid string, body []byte) error {
	m.mu.RLock()
	rt, ok := m.queues[queueName]
	m.mu.RUnlock()

	if !ok || rt == nil {
		return ErrUnknownQueue
	}

	job := Job{
		Queue:       queueName,
		MessageGUID: guid,
		Body:        body,
		EnqueuedAt:  time.Now(),
		Attempt:     1,
	}

	return rt.Enqueue(ctx, job)
}

func (m *Manager) StopAll() {
	m.mu.Lock()
	// Move all runtimes out of the map under exclusive lock so no other
	// goroutine can access them after this point.
	rts := make([]*Runtime, 0, len(m.queues))
	for _, rt := range m.queues {
		if rt != nil {
			rts = append(rts, rt)
		}
	}
	m.queues = make(map[string]*Runtime)
	m.mu.Unlock()

	for _, rt := range rts {
		rt.Stop()
		if st, ok := rt.Store.(interface{ Close() error }); ok {
			_ = st.Close()
		}
	}
}