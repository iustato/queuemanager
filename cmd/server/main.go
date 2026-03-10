package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
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

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envInt64(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

func envStr(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}

func main() {
	logger, err := logging.NewProductionLogger()
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = logger.Sync() }()

	queuesDir := envStr("QUEUES_CONFIG_DIR", "./configs")

	cfgs, err := config.LoadQueueConfigs(queuesDir)
	if err != nil {
		logger.Fatal("load queue configs",
			zap.String("dir", queuesDir),
			zap.Error(err),
		)
	}

	// --- storage (bbolt) — per-queue files ---
	storageDir := envStr("QUEUE_STORAGE_DIR", "./data")
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		logger.Fatal("mkdir storage dir", zap.String("path", storageDir), zap.Error(err))
	}

	storageOpenTimeout := time.Duration(envInt("STORAGE_OPEN_TIMEOUT_MS", 2000)) * time.Millisecond
	processingTimeoutMs := envInt64("PROCESSING_TIMEOUT_MS", 120_000)
	gcProcessingGraceMs := envInt64("GC_PROCESSING_GRACE_MS", 120_000)

	storageOpts := storage.OpenOptions{
		Timeout:             storageOpenTimeout,
		ProcessingTimeoutMs: processingTimeoutMs,
		GCProcessingGraceMs: gcProcessingGraceMs,
	}

	// --- queues ---
	qm := queue.NewManager(logger, storageDir, storageOpts)

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
		zap.String("queues_dir", queuesDir),
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

	staticDir := envStr("STATIC_DIR", "web/static")
	fs := http.FileServer(http.Dir(staticDir))
	publicMux.Handle("/static/", http.StripPrefix("/static/", fs))

	publicMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" && r.Method == http.MethodGet {
			http.ServeFile(w, r, filepath.Join(staticDir, "index.html"))
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
	publicHandler = httpserver.AccessLog(logger, publicHandler)

	publicAddr := envStr("PUBLIC_ADDR", "0.0.0.0:8080")

	readHeaderTimeout := time.Duration(envInt("READ_HEADER_TIMEOUT_SEC", 5)) * time.Second

	publicSrv := httpserver.New(httpserver.Config{
		Addr:              publicAddr,
		ReadHeaderTimeout: readHeaderTimeout,
		Handler:           publicHandler,
	})

	// --- internal mux (worker callbacks) ---
	internalMux := http.NewServeMux()
	internalMux.HandleFunc("/internal/", func(w http.ResponseWriter, r *http.Request) {
		// /internal/{queue}/done/{msg_id}
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

	internalAddr := envStr("INTERNAL_ADDR", "127.0.0.1:8081")

	var internalHandler http.Handler = internalMux
	internalHandler = httpserver.RequestID(internalHandler)
	internalHandler = httpserver.AccessLog(logger.Named("internal"), internalHandler)

	internalSrv := httpserver.New(httpserver.Config{
		Addr:              internalAddr,
		ReadHeaderTimeout: readHeaderTimeout,
		Handler:           internalHandler,
	})

	// --- start servers ---
	errCh := make(chan error, 2)

	go func() {
		logger.Info("starting public server", zap.String("addr", publicAddr))
		errCh <- publicSrv.ListenAndServe()
	}()

	go func() {
		logger.Info("starting internal server", zap.String("addr", internalAddr))
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
	shutdownTimeout := time.Duration(envInt("SHUTDOWN_TIMEOUT_SEC", 10)) * time.Second
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

	stopTimeout := time.Duration(envInt("STOP_ALL_TIMEOUT_SEC", 30)) * time.Second
	select {
	case <-stopDone:
		logger.Info("all queues stopped gracefully")
	case <-time.After(stopTimeout):
		logger.Error("StopAll timed out, forcing exit",
			zap.Duration("timeout", stopTimeout),
		)
	}
}