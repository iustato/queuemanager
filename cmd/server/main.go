package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"go-web-server/internal/config"
	"go-web-server/internal/httpserver"
	"go-web-server/internal/logging"
	"go-web-server/internal/queue"
	"go-web-server/internal/storage"
	"go-web-server/internal/validate"

	"go.uber.org/zap"
)

func main() {
	logger, err := logging.NewProductionLogger()
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = logger.Sync() }()

	// --- load server config from .env file ---
	sc := config.LoadServerConfig(".env")

	cfgs, err := config.LoadQueueConfigs(sc.QueuesConfigDir)
	if err != nil {
		logger.Fatal("load queue configs",
			zap.String("dir", sc.QueuesConfigDir),
			zap.Error(err),
		)
	}

	// --- storage (bbolt) — per-queue files ---
	if err := os.MkdirAll(sc.QueueStorageDir, 0o755); err != nil {
		logger.Fatal("mkdir storage dir", zap.String("path", sc.QueueStorageDir), zap.Error(err))
	}

	storageOpts := storage.OpenOptions{
		Timeout:             time.Duration(sc.StorageOpenTimeoutMs) * time.Millisecond,
		ProcessingTimeoutMs: sc.ProcessingTimeoutMs,
		GCProcessingGraceMs: sc.GCProcessingGraceMs,
	}

	// --- queues ---
	qm := queue.NewManager(logger, sc.QueueStorageDir, storageOpts, sc.WorkerToken, sc.AllowAutoGUID, sc.PhpCgiBin)

	for _, qc := range cfgs {
		sch, err := validate.LoadSchemaFromFile(qc.SchemaFile)
		if err != nil {
			logger.Fatal("load schema",
				zap.String("queue", qc.Name),
				zap.String("schema_file", qc.SchemaFile),
				zap.Error(err),
			)
		}
		if err := qm.AddQueue(qc, sch); err != nil {
			logger.Fatal("add queue",
				zap.String("queue", qc.Name),
				zap.Error(err),
			)
		}
	}

	logger.Info("queues loaded",
		zap.String("queues_dir", sc.QueuesConfigDir),
		zap.Strings("queues", qm.ListNames()),
	)

	// --- public mux ---
	publicMux := http.NewServeMux()

	publicMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	publicMux.HandleFunc("/health/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/health", http.StatusPermanentRedirect)
	})

	fs := http.FileServer(http.Dir(sc.StaticDir))
	publicMux.Handle("/static/", http.StripPrefix("/static/", fs))

	// --- admin API (CRUD queues) ---
	publicMux.HandleFunc("/api/queues", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			qm.HandleListQueues(w, r)
		case http.MethodPost:
			qm.HandleAddQueue(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	publicMux.HandleFunc("/api/queues/", func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/api/queues/")
		name = strings.TrimRight(name, "/")
		if name == "" {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodPut:
			qm.HandleUpdateQueue(name, w, r)
		case http.MethodDelete:
			qm.HandleDeleteQueue(name, w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	publicMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" && r.Method == http.MethodGet {
			http.ServeFile(w, r, filepath.Join(sc.StaticDir, "index.html"))
			return
		}

		path := strings.Trim(r.URL.Path, "/")
		parts := strings.Split(path, "/")

		if len(parts) == 1 && parts[0] == "info" {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			qm.HandleGetInfoAll(w, r)
			return
		}

		if len(parts) == 2 && parts[1] == "newmessage" {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			qm.HandleNewMessage(parts[0], w, r)
			return
		}

		if len(parts) == 2 && parts[1] == "info" {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			qm.HandleGetInfo(parts[0], w, r)
			return
		}

		if len(parts) == 3 && parts[1] == "status" {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			qm.HandleGetStatus(parts[0], parts[2], w, r)
			return
		}

		if len(parts) == 3 && parts[1] == "result" {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			qm.HandleGetResult(parts[0], parts[2], w, r)
			return
		}

		http.NotFound(w, r)
	})

	var publicHandler http.Handler = publicMux
	publicHandler = httpserver.RequestID(publicHandler)
	publicHandler = httpserver.AccessLog(logger, publicHandler, sc.DisableAccessLog)

	readHeaderTimeout := time.Duration(sc.ReadHeaderTimeoutSec) * time.Second

	publicSrv := httpserver.New(httpserver.Config{
		Addr:              sc.PublicAddr,
		ReadHeaderTimeout: readHeaderTimeout,
		Handler:           publicHandler,
	})

	// --- internal mux (worker callbacks) ---
	internalMux := http.NewServeMux()
	internalMux.HandleFunc("/internal/", func(w http.ResponseWriter, r *http.Request) {
		// /internal/{queue}/done/{guid}
		path := strings.Trim(r.URL.Path, "/")
		parts := strings.Split(path, "/")

		if len(parts) == 4 && parts[0] == "internal" && parts[2] == "done" {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			qm.HandleReportDone(parts[1], parts[3], w, r)
			return
		}

		http.NotFound(w, r)
	})

	var internalHandler http.Handler = internalMux
	internalHandler = httpserver.RequestID(internalHandler)
	internalHandler = httpserver.AccessLog(logger.Named("internal"), internalHandler, sc.DisableAccessLog)

	internalSrv := httpserver.New(httpserver.Config{
		Addr:              sc.InternalAddr,
		ReadHeaderTimeout: readHeaderTimeout,
		Handler:           internalHandler,
	})

	// --- start servers ---
	errCh := make(chan error, 2)

	go func() {
		logger.Info("starting public server", zap.String("addr", sc.PublicAddr))
		errCh <- publicSrv.ListenAndServe()
	}()

	go func() {
		logger.Info("starting internal server", zap.String("addr", sc.InternalAddr))
		errCh <- internalSrv.ListenAndServe()
	}()

	// --- wait for signal or server error ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("shutdown signal received", zap.String("signal", sig.String()))
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			logger.Error("server error", zap.Error(err))
		}
	}

	// --- graceful shutdown ---
	shutdownTimeout := time.Duration(sc.ShutdownTimeoutSec) * time.Second
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	if err := publicSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("public shutdown failed", zap.Error(err))
	}
	if err := internalSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("internal shutdown failed", zap.Error(err))
	}

	// stop runtimes (workers + per-queue maintenance) with a timeout
	// to prevent hanging indefinitely on stuck workers
	stopDone := make(chan struct{})
	go func() {
		qm.StopAll()
		close(stopDone)
	}()

	stopTimeout := time.Duration(sc.StopAllTimeoutSec) * time.Second
	select {
	case <-stopDone:
		logger.Info("all queues stopped gracefully")
	case <-time.After(stopTimeout):
		logger.Error("StopAll timed out, forcing exit",
			zap.Duration("timeout", stopTimeout),
		)
	}
}
