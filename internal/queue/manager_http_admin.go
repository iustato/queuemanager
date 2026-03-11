package queue

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"

	"go-web-server/internal/config"
	"go-web-server/internal/validate"
)

type queueEntry struct {
	Name   string             `json:"name"`
	Config config.QueueConfig `json:"config"`
}

// HandleListQueues returns all registered queues with their configs.
func (m *Manager) HandleListQueues(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "GET only", nil)
		return
	}

	m.mu.RLock()
	entries := make([]queueEntry, 0, len(m.queues))
	for name, rt := range m.queues {
		if rt != nil {
			entries = append(entries, queueEntry{Name: name, Config: rt.Cfg})
		}
	}
	m.mu.RUnlock()

	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	writeAPIOK(w, r, http.StatusOK, map[string]any{"queues": entries})
}

type adminQueueRequest struct {
	Config config.QueueConfig `json:"config"`
	Schema json.RawMessage    `json:"schema"`
}

// HandleAddQueue creates a new queue from JSON config + optional inline schema.
func (m *Manager) HandleAddQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "POST only", nil)
		return
	}

	var req adminQueueRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeAPIError(w, r, http.StatusBadRequest, "invalid_json", err.Error(), nil)
		return
	}

	cfg := req.Config
	config.NormalizeConfigForAPI(&cfg)

	if err := config.ValidateConfigForAPI(cfg); err != nil {
		writeAPIError(w, r, http.StatusUnprocessableEntity, "invalid_config", err.Error(), nil)
		return
	}

	var schema *validate.CompiledSchema
	if len(req.Schema) > 0 && string(req.Schema) != "null" {
		var err error
		schema, err = validate.CompileSchemaFromBytes(req.Schema)
		if err != nil {
			writeAPIError(w, r, http.StatusUnprocessableEntity, "invalid_schema", err.Error(), nil)
			return
		}
	}

	if err := m.AddQueue(cfg, schema, req.Schema); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			writeAPIError(w, r, http.StatusConflict, "queue_exists", err.Error(), nil)
			return
		}
		writeAPIError(w, r, http.StatusInternalServerError, "internal", err.Error(), nil)
		return
	}

	writeAPIOK(w, r, http.StatusCreated, map[string]string{"name": cfg.Name})
}

// HandleUpdateQueue replaces a queue's config (stops old runtime, starts new one).
func (m *Manager) HandleUpdateQueue(queueName string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		writeAPIError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "PUT only", nil)
		return
	}

	var req adminQueueRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeAPIError(w, r, http.StatusBadRequest, "invalid_json", err.Error(), nil)
		return
	}

	cfg := req.Config
	cfg.Name = queueName
	config.NormalizeConfigForAPI(&cfg)

	if err := config.ValidateConfigForAPI(cfg); err != nil {
		writeAPIError(w, r, http.StatusUnprocessableEntity, "invalid_config", err.Error(), nil)
		return
	}

	var schema *validate.CompiledSchema
	if len(req.Schema) > 0 && string(req.Schema) != "null" {
		var err error
		schema, err = validate.CompileSchemaFromBytes(req.Schema)
		if err != nil {
			writeAPIError(w, r, http.StatusUnprocessableEntity, "invalid_schema", err.Error(), nil)
			return
		}
	}

	if err := m.ReplaceQueue(cfg, schema, req.Schema); err != nil {
		writeAPIError(w, r, http.StatusInternalServerError, "internal", err.Error(), nil)
		return
	}

	writeAPIOK(w, r, http.StatusOK, map[string]string{"name": cfg.Name})
}

// HandleDeleteQueue deletes a queue (stops runtime, removes .db file).
func (m *Manager) HandleDeleteQueue(queueName string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeAPIError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "DELETE only", nil)
		return
	}

	if _, ok := m.Get(queueName); !ok {
		writeAPIError(w, r, http.StatusNotFound, "queue_not_found", "queue "+queueName+" not found", nil)
		return
	}

	m.DeleteQueue(queueName)
	writeAPIOK(w, r, http.StatusOK, map[string]string{"deleted": queueName})
}
