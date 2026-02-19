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

	queuesDir := os.Getenv("QUEUES_CONFIG_DIR")
	if queuesDir == "" {
		queuesDir = "./configs"
	}

	cfgs, err := config.LoadQueueConfigs(queuesDir)
	if err != nil {
		logger.Fatal("load queue configs",
			zap.String("dir", queuesDir),
			zap.Error(err),
		)
	}

	// --- storage (bbolt) ---
	stPath := os.Getenv("QUEUE_STORAGE_PATH")
	if stPath == "" {
		stPath = "./data/queue.db"
	}
	if err := os.MkdirAll(filepath.Dir(stPath), 0o755); err != nil {
		logger.Fatal("mkdir storage dir", zap.String("path", stPath), zap.Error(err))
	}

	st, err := storage.Open(storage.OpenOptions{
		FilePath: stPath,
		Timeout:  2 * time.Second,
	})
	if err != nil {
		logger.Fatal("open storage", zap.String("path", stPath), zap.Error(err))
	}
	defer func() { _ = st.Close() }()

	// --- queues ---
	qm := queue.NewManagerWithStore(logger, st)

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

	fs := http.FileServer(http.Dir("web/static"))
	publicMux.Handle("/static/", http.StripPrefix("/static/", fs))

	publicMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" && r.Method == http.MethodGet {
			http.ServeFile(w, r, "web/static/index.html")
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

	publicAddr := os.Getenv("PUBLIC_ADDR")
	if publicAddr == "" {
		publicAddr = "0.0.0.0:8080"
	}

	publicSrv := httpserver.New(httpserver.Config{
		Addr:              publicAddr,
		ReadHeaderTimeout: 5 * time.Second,
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

	internalAddr := os.Getenv("INTERNAL_ADDR")
	if internalAddr == "" {
		internalAddr = "127.0.0.1:8081"
	}

	var internalHandler http.Handler = internalMux
	internalHandler = httpserver.RequestID(internalHandler)
	internalHandler = httpserver.AccessLog(logger.Named("internal"), internalHandler)

	internalSrv := httpserver.New(httpserver.Config{
		Addr:              internalAddr,
		ReadHeaderTimeout: 5 * time.Second,
		Handler:           internalHandler,
	})

	// --- background tasks + graceful shutdown ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go qm.StartBackgroundTasks(ctx)

	errCh := make(chan error, 2)

	go func() {
		logger.Info("starting public server", zap.String("addr", publicAddr))
		errCh <- publicSrv.ListenAndServe()
	}()

	go func() {
		logger.Info("starting internal server", zap.String("addr", internalAddr))
		errCh <- internalSrv.ListenAndServe()
	}()

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

	// stop background tasks
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := publicSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("public shutdown failed", zap.Error(err))
	}
	if err := internalSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("internal shutdown failed", zap.Error(err))
	}

	qm.StopAll()
}
