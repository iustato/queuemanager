package httpserver

import (
	"net/http"
	"time"
)

type Config struct {
	Addr              string
	ReadHeaderTimeout time.Duration
	Handler           http.Handler
}

func New(cfg Config) *http.Server {
	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           cfg.Handler,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}
}
