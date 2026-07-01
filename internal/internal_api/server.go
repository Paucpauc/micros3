package internal_api

import (
	"context"
	"net/http"

	"go.uber.org/zap"
)

type Server struct {
	httpServer *http.Server
	logger     *zap.Logger
}

func NewServer(listenAddr string, handler http.Handler, logger *zap.Logger) *Server {
	return &Server{
		httpServer: &http.Server{
			Addr:    listenAddr,
			Handler: handler,
		},
		logger: logger,
	}
}

func (s *Server) Start() error {
	s.logger.Info("Starting internal API server", zap.String("address", s.httpServer.Addr))
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down internal API server...")
	return s.httpServer.Shutdown(ctx)
}
