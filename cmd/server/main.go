package main

import (
	"log"
	"net/http"
	"os"
	"strings"
	"time"
	"path/filepath"

	"go-web-server/internal/config"
	"go-web-server/internal/httpserver"
	"go-web-server/internal/logging"
	"go-web-server/internal/queue"
	"go-web-server/internal/validate"
	"go-web-server/internal/storage"


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
	// создадим директорию, если её нет
	if err := os.MkdirAll(filepath.Dir(stPath), 0o755); err != nil {
		logger.Fatal("mkdir storage dir", zap.String("path", stPath), zap.Error(err))
	}

	st, err := storage.Open(storage.OpenOptions{
		FilePath:    stPath,
		Timeout: 2 * time.Second,
	})
	if err != nil {
		logger.Fatal("open storage", zap.String("path", stPath), zap.Error(err))
	}
	defer func() { _ = st.Close() }()


	qm := queue.NewManagerWithStore(logger, st)
	defer qm.StopAll()

	for _, qc := range cfgs {
		sch, err := validate.LoadSchemaFromFile(qc.SchemaFile)
		if err != nil {
			logger.Fatal("load schema",
				zap.String("queue", qc.Name),
				zap.String("schema_file", qc.SchemaFile),
				zap.Error(err),
			)
		}

		// ВАЖНО: AddQueue теперь сам:
		// - читает из qc runtime/script/timeout/max_queue и т.д.
		// - выставляет rt.Command / rt.ScriptPath
		// - стартует воркеры
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

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/health/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/health", http.StatusPermanentRedirect)
	})

	fs := http.FileServer(http.Dir("web/static"))
	mux.Handle("/static/", http.StripPrefix("/static/", fs))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" && r.Method == http.MethodGet {
			http.ServeFile(w, r, "web/static/index.html")
			return
		}

		path := strings.Trim(r.URL.Path, "/")
		parts := strings.Split(path, "/")

		if len(parts) == 2 && parts[1] == "newmessage" {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			queueName := parts[0]
			qm.HandleNewMessage(queueName, w, r)
			return
		}

		http.NotFound(w, r)
	})

	var handler http.Handler = mux
	handler = httpserver.RequestID(handler)
	handler = httpserver.AccessLog(logger, handler)

	addr := "0.0.0.0:8080"
	srv := httpserver.New(httpserver.Config{
		Addr:              addr,
		ReadHeaderTimeout: 5 * time.Second,
		Handler:           handler,
	})

	logger.Info("starting server", zap.String("addr", addr))

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatal("server stopped", zap.Error(err))
	}
}
