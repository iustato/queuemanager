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

	// msgID -> record
	msg map[string]mockMsg

	// idempotencyKey -> msgID (dedup barrier)
	idem map[string]string

	// bookkeeping for asserts
	markQueuedCalls        []string
	markEnqueueFailedCalls []struct {
		MsgID  string
		Reason string
	}
	markProcessingCalls []struct {
		MsgID   string
		Attempt int
	}
	touchProcessingCalls []string
	requeueForRetryCalls []struct {
		MsgID string
		TS    int64
	}
	markDoneCalls []struct {
		MsgID  string
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
	failGetInfoAll          error
	failRequeueStuck        error
	failGC                  error
}

type mockMsg struct {
	Queue string
	Body  []byte

	Status storage.Status
	Result storage.Result
}

func newMockStore() *mockStore {
	return &mockStore{
		msg:  make(map[string]mockMsg),
		idem: make(map[string]string),
	}
}

func (ms *mockStore) PutNewMessage(
	ctx context.Context,
	queue, msgID string,
	body []byte,
	idempotencyKey string,
	enqueuedAtMs int64,
	expiresAtMs int64,
) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.failPutNewMessage != nil {
		return "", false, ms.failPutNewMessage
	}

	// Dedup: if idemKey already exists, return existing msgID and created=false
	if idempotencyKey != "" {
		if existing, ok := ms.idem[idempotencyKey]; ok {
			return existing, false, nil
		}
		ms.idem[idempotencyKey] = msgID
	}

	ms.msg[msgID] = mockMsg{
		Queue:  queue,
		Body:   append([]byte(nil), body...),
		Status: storage.StatusCreated,
	}
	return msgID, true, nil
}

func (ms *mockStore) MarkQueued(msgID string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.markQueuedCalls = append(ms.markQueuedCalls, msgID)
	if ms.failMarkQueued != nil {
		return ms.failMarkQueued
	}
	m, ok := ms.msg[msgID]
	if ok {
		m.Status = storage.StatusQueued
		ms.msg[msgID] = m
	}
	return nil
}

func (ms *mockStore) MarkEnqueueFailed(msgID string, reason string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.markEnqueueFailedCalls = append(ms.markEnqueueFailedCalls, struct {
		MsgID  string
		Reason string
	}{MsgID: msgID, Reason: reason})

	if ms.failMarkEnqueueFailed != nil {
		return ms.failMarkEnqueueFailed
	}
	// Optional: set status if message exists
	m, ok := ms.msg[msgID]
	if ok {
		m.Status = storage.StatusFailed
		ms.msg[msgID] = m
	}
	return nil
}

func (ms *mockStore) MarkProcessing(msgID string, attempt int) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.markProcessingCalls = append(ms.markProcessingCalls, struct {
		MsgID   string
		Attempt int
	}{MsgID: msgID, Attempt: attempt})

	if ms.failMarkProcessing != nil {
		return ms.failMarkProcessing
	}
	m, ok := ms.msg[msgID]
	if ok {
		m.Status = storage.StatusProcessing
		ms.msg[msgID] = m
	}
	return nil
}

func (ms *mockStore) TouchProcessing(msgID string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.touchProcessingCalls = append(ms.touchProcessingCalls, msgID)
	if ms.failTouchProcessing != nil {
		return ms.failTouchProcessing
	}
	return nil
}

func (ms *mockStore) RequeueForRetry(msgID string, ts int64) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.requeueForRetryCalls = append(ms.requeueForRetryCalls, struct {
		MsgID string
		TS    int64
	}{MsgID: msgID, TS: ts})

	if ms.failRequeueForRetry != nil {
		return ms.failRequeueForRetry
	}
	return nil
}

func (ms *mockStore) MarkDone(msgID string, status storage.Status, res storage.Result, ttl int64) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.markDoneCalls = append(ms.markDoneCalls, struct {
		MsgID  string
		Status storage.Status
		Res    storage.Result
		TTL    int64
	}{MsgID: msgID, Status: status, Res: res, TTL: ttl})

	if ms.failMarkDone != nil {
		return ms.failMarkDone
	}
	m, ok := ms.msg[msgID]
	if ok {
		m.Status = status
		m.Result = res
		ms.msg[msgID] = m
	}
	return nil
}

func (ms *mockStore) GetStatusAndResult(msgID string) (storage.Status, storage.Result, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.failGetStatusAndResult != nil {
		var st storage.Status
		return st, storage.Result{}, ms.failGetStatusAndResult
	}

	m, ok := ms.msg[msgID]
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

func (ms *mockStore) GetInfoAll(fromTime int64) (map[string]storage.QueueInfo, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.failGetInfoAll != nil {
		return nil, ms.failGetInfoAll
	}
	return map[string]storage.QueueInfo{}, nil
}

func (ms *mockStore) RequeueStuck(now int64, limit int) (int, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.failRequeueStuck != nil {
		return 0, ms.failRequeueStuck
	}
	return 0, nil
}

func (ms *mockStore) GC(now int64, limit int) (int, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.failGC != nil {
		return 0, ms.failGC
	}
	return 0, nil
}