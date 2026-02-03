package main

import (
	"log"
	"net/http"
	"time"

	"go-web-server/internal/httpserver"
	"go-web-server/internal/logging"
)

func main() {
	// 1) логгер создаём ДО запуска сервера
	logger, err := logging.NewDevelopmentLogger() // локально
	// logger, err := logging.NewProductionLogger() // прод
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = logger.Sync() }()

	// 2) роуты
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	fileServer := http.FileServer(http.Dir("web/static"))
	mux.Handle("/", fileServer)


// 3) middleware (RequestID снаружи)
handler := httpserver.AccessLog(logger, mux)
handler = httpserver.RequestID(handler)

// 4) сервер
srv := httpserver.New(httpserver.Config{
	Addr:              ":8080",
	ReadHeaderTimeout: 5 * time.Second,
	Handler:           handler,
})


	logger.Info("starting server") // если хочешь — добавим поля addr
	log.Println("listening on http://localhost:8080")
	log.Fatal(srv.ListenAndServe())
}
