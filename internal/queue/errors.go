package queue

import "errors"

var (
	ErrQueueNotFound     = errors.New("queue not found")        // очередь не зарегистрирована в Manager
	ErrQueueStopped      = errors.New("queue stopped")          // Stop() или quit
	ErrQueueBusy         = errors.New("queue full and busy")    // очередь забита/таймаут на enqueue
	ErrRuntimeNotInit    = errors.New("runtime not initialized")
	ErrPendingEnqueue = errors.New("pending enqueue")
)