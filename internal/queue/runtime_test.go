package queue

import (
	"context"
	"errors"
	"sync"

	"go-web-server/internal/storage"
)

// mockStore is an in-memory implementation of QueueStore for tests.
type mockStore struct {
	mu sync.Mutex

	// guid -> record
	msg map[string]mockMsg

	// bookkeeping for asserts
	markQueuedCalls        []string
	markEnqueueFailedCalls []struct {
		GUID   string
		Reason string
	}
	markProcessingCalls []struct {
		GUID    string
		Attempt int
	}
	touchProcessingCalls []string
	requeueForRetryCalls []struct {
		GUID string
		TS   int64
	}
	markDoneCalls []struct {
		GUID   string
		Status storage.Status
		Res    storage.Result
		TTL    int64
	}

	// allow injecting failures if needed
	failPutNewMessage       error
	failMarkQueued          error
	failMarkEnqueueFailed   error
	failMarkProcessing      error
	failTouchProcessing     error
	failRequeueForRetry     error
	failMarkDone            error
	failGetStatusAndResult  error
	failGetInfo             error
	failRequeueStuck        error
	failGC                  error

	// call counters for assertions
	requeueStuckCalled int
	gcCalled           int
}

type mockMsg struct {
	Queue string
	Body  []byte

	Status storage.Status
	Result storage.Result
}

func newMockStore() *mockStore {
	return &mockStore{
		msg: make(map[string]mockMsg),
	}
}

func (ms *mockStore) PutNewMessage(
	ctx context.Context,
	queue, guid string,
	body []byte,
	enqueuedAtMs int64,
	expiresAtMs int64,
) (string, bool, int64, error) {
	if err := ctx.Err(); err != nil {
		return "", false, 0, err
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.failPutNewMessage != nil {
		return "", false, 0, ms.failPutNewMessage
	}

	// Dedup: if guid already exists, return created=false
	if _, ok := ms.msg[guid]; ok {
		return guid, false, enqueuedAtMs, nil
	}

	ms.msg[guid] = mockMsg{
		Queue:  queue,
		Body:   append([]byte(nil), body...),
		Status: storage.StatusCreated,
	}
	return guid, true, enqueuedAtMs, nil
}

func (ms *mockStore) MarkQueued(guid string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.markQueuedCalls = append(ms.markQueuedCalls, guid)
	if ms.failMarkQueued != nil {
		return ms.failMarkQueued
	}
	m, ok := ms.msg[guid]
	if ok {
		m.Status = storage.StatusQueued
		ms.msg[guid] = m
	}
	return nil
}

func (ms *mockStore) MarkEnqueueFailed(guid string, reason string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.markEnqueueFailedCalls = append(ms.markEnqueueFailedCalls, struct {
		GUID   string
		Reason string
	}{GUID: guid, Reason: reason})

	if ms.failMarkEnqueueFailed != nil {
		return ms.failMarkEnqueueFailed
	}
	// Optional: set status if message exists
	m, ok := ms.msg[guid]
	if ok {
		m.Status = storage.StatusFailed
		ms.msg[guid] = m
	}
	return nil
}

func (ms *mockStore) MarkProcessing(guid string, attempt int) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.markProcessingCalls = append(ms.markProcessingCalls, struct {
		GUID    string
		Attempt int
	}{GUID: guid, Attempt: attempt})

	if ms.failMarkProcessing != nil {
		return ms.failMarkProcessing
	}
	m, ok := ms.msg[guid]
	if ok {
		m.Status = storage.StatusProcessing
		ms.msg[guid] = m
	}
	return nil
}

func (ms *mockStore) TouchProcessing(guid string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.touchProcessingCalls = append(ms.touchProcessingCalls, guid)
	if ms.failTouchProcessing != nil {
		return ms.failTouchProcessing
	}
	return nil
}

func (ms *mockStore) RequeueForRetry(guid string, ts int64) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.requeueForRetryCalls = append(ms.requeueForRetryCalls, struct {
		GUID string
		TS   int64
	}{GUID: guid, TS: ts})

	if ms.failRequeueForRetry != nil {
		return ms.failRequeueForRetry
	}
	return nil
}

func (ms *mockStore) MarkDone(guid string, status storage.Status, res storage.Result, ttl int64) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.markDoneCalls = append(ms.markDoneCalls, struct {
		GUID   string
		Status storage.Status
		Res    storage.Result
		TTL    int64
	}{GUID: guid, Status: status, Res: res, TTL: ttl})

	if ms.failMarkDone != nil {
		return ms.failMarkDone
	}
	m, ok := ms.msg[guid]
	if ok {
		m.Status = status
		m.Result = res
		ms.msg[guid] = m
	}
	return nil
}

func (ms *mockStore) GetStatusAndResult(guid string) (storage.Status, storage.Result, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.failGetStatusAndResult != nil {
		var st storage.Status
		return st, storage.Result{}, ms.failGetStatusAndResult
	}

	m, ok := ms.msg[guid]
	if !ok {
		var st storage.Status
		return st, storage.Result{}, errors.New("not found")
	}

	return m.Status, m.Result, nil
}

func (ms *mockStore) GetInfo(queueName string, fromTime int64) (storage.QueueInfo, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.failGetInfo != nil {
		return storage.QueueInfo{}, ms.failGetInfo
	}
	// Minimal stub; tests that need real stats can extend this.
	return storage.QueueInfo{}, nil
}

func (ms *mockStore) RequeueStuck(now int64, limit int) (int, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.requeueStuckCalled++
	if ms.failRequeueStuck != nil {
		return 0, ms.failRequeueStuck
	}
	return 0, nil
}

func (ms *mockStore) GC(now int64, limit int) ([]string, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.gcCalled++
	if ms.failGC != nil {
		return nil, ms.failGC
	}
	return nil, nil
}