package queue

import (
	"context"
	"errors"
	"strings"
	"time"

	"go-web-server/internal/config"
	uuidutil "go-web-server/internal/uuid"
	"go-web-server/internal/validate"

	"github.com/google/uuid"
)

type PushRequest struct {
	Queue       string
	Body        []byte
	MessageGUID string

	Method      string
	QueryString string
	ContentType string
	RemoteAddr  string
}

type PushResponse struct {
	MessageGUID  string
	Created      bool  // false => dedup
	Queued       bool  // true => rt.Enqueue succeeded and MarkQueued done
	CreatedAtMs  int64 // first registration timestamp (ms since epoch)
}

type QueueService struct {
	mgr *Manager
}

func NewQueueService(mgr *Manager) *QueueService { return &QueueService{mgr: mgr} }

type ErrInvalidMessageGUID struct{ Msg string }

func (e ErrInvalidMessageGUID) Error() string { return e.Msg }

type ErrInvalidJSON struct{ Cause error }

func (e ErrInvalidJSON) Error() string { return "invalid json" }
func (e ErrInvalidJSON) Unwrap() error { return e.Cause }

type ErrSchemaValidation struct{ Cause error }

func (e ErrSchemaValidation) Error() string { return "schema validation failed" }
func (e ErrSchemaValidation) Unwrap() error { return e.Cause }

type ErrPayloadTooLarge struct{ LimitBytes int64 }

func (e ErrPayloadTooLarge) Error() string { return "payload too large" }

func (s *QueueService) Push(ctx context.Context, req PushRequest) (PushResponse, error) {
	if err := ctx.Err(); err != nil {
		return PushResponse{}, err
	}

	rt, ok := s.mgr.Get(req.Queue)
	if !ok || rt == nil {
		return PushResponse{}, ErrQueueNotFound
	}

	guid, err := normalizeMessageGUID(req.MessageGUID, s.resolveAllowAutoGUID(rt))
	if err != nil {
		return PushResponse{}, err
	}

	limit := int64(maxBodyBytes)
	if rt.Cfg.MaxSize > 0 && int64(rt.Cfg.MaxSize) < limit {
		limit = int64(rt.Cfg.MaxSize)
	}
	if int64(len(req.Body)) > limit {
		return PushResponse{}, ErrPayloadTooLarge{LimitBytes: limit}
	}

	if err := rt.Schema.ValidateBytes(req.Body); err != nil {
		if errors.Is(err, validate.ErrInvalidJSON) {
			return PushResponse{}, ErrInvalidJSON{Cause: err}
		}
		return PushResponse{}, ErrSchemaValidation{Cause: err}
	}

	messageExpiry, _ := config.ParseDurationExt(rt.Cfg.MessageExpiry)
	if messageExpiry <= 0 {
		messageExpiry = 30 * 24 * time.Hour
	}
	expiresAtMs := time.Now().Add(messageExpiry).UnixMilli()

	nowMs := time.Now().UnixMilli()
	createdAtMs := nowMs

	if rt.Store != nil {
		_, wasCreated, storedAtMs, err := rt.Store.PutNewMessage(
			ctx,
			req.Queue,
			guid,
			req.Body,
			nowMs,
			expiresAtMs,
		)
		if err != nil {
			return PushResponse{MessageGUID: guid, CreatedAtMs: storedAtMs}, err
		}
		createdAtMs = storedAtMs
		if !wasCreated {
			return PushResponse{
				MessageGUID: guid,
				Created:     false,
				Queued:      false,
				CreatedAtMs: createdAtMs,
			}, nil
		}
	}

	job := Job{
		Queue:       req.Queue,
		MessageGUID: guid,
		Body:        req.Body,
		EnqueuedAt:  time.Now(),
		Attempt:     1,
		Method:      req.Method,
		QueryString: req.QueryString,
		ContentType: req.ContentType,
		RemoteAddr:  req.RemoteAddr,
	}

	if err := rt.Enqueue(ctx, job); err != nil {
		if rt.Store != nil {
			_ = rt.Store.MarkEnqueueFailed(guid, err.Error())
		}
		return PushResponse{
			MessageGUID: guid,
			Created:     true,
			Queued:      false,
			CreatedAtMs: createdAtMs,
		}, err
	}

	if rt.Store != nil {
		if err := rt.Store.MarkQueued(guid); err != nil {
			return PushResponse{
				MessageGUID: guid,
				Created:     true,
				Queued:      true,
				CreatedAtMs: createdAtMs,
			}, err
		}
	}

	return PushResponse{
		MessageGUID: guid,
		Created:     true,
		Queued:      true,
		CreatedAtMs: createdAtMs,
	}, nil
}

// resolveAllowAutoGUID returns per-queue config if set, otherwise the global env var default.
func (s *QueueService) resolveAllowAutoGUID(rt *Runtime) bool {
	if rt.Cfg.AllowAutoGUID != nil {
		return *rt.Cfg.AllowAutoGUID
	}
	return s.mgr.allowAutoGUID
}

// normalizeMessageGUID validates or auto-generates a UUIDv7 MessageGUID.
func normalizeMessageGUID(raw string, allowAutoGUID bool) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		if !allowAutoGUID {
			return "", ErrInvalidMessageGUID{Msg: "X-Message-GUID header is required"}
		}
		gen, err := uuidutil.NewV7()
		if err != nil {
			return "", err
		}
		return gen, nil
	}

	parsed, err := uuid.Parse(raw)
	if err != nil {
		return "", ErrInvalidMessageGUID{Msg: "invalid X-Message-GUID format"}
	}
	if parsed.Version() != uuid.Version(7) {
		return "", ErrInvalidMessageGUID{Msg: "X-Message-GUID must be UUIDv7"}
	}
	return raw, nil
}

var _ = errors.Is
