package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	dispatcherv1 "webhook-dispatcher/api/proto/dispatcher/v1"
	"webhook-dispatcher/internal/config"
	"webhook-dispatcher/internal/domain"
	"webhook-dispatcher/internal/metrics"
	"webhook-dispatcher/internal/repository/postgres"
	"webhook-dispatcher/internal/worker"
)

type Server struct {
	dispatcherv1.UnimplementedDispatcherServiceServer
	cfg        *config.Config
	repo       worker.Repository
	dispatcher *worker.Dispatcher
	metrics    *metrics.Metrics
	grpcServer *grpc.Server
	listener   net.Listener
}

func NewServer(
	cfg *config.Config,
	repo worker.Repository,
	dispatcher *worker.Dispatcher,
	m *metrics.Metrics,
) *Server {
	s := &Server{
		cfg:        cfg,
		repo:       repo,
		dispatcher: dispatcher,
		metrics:    m,
	}

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(s.unaryInterceptor),
	}

	s.grpcServer = grpc.NewServer(opts...)
	dispatcherv1.RegisterDispatcherServiceServer(s.grpcServer, s)
	reflection.Register(s.grpcServer)

	return s
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.cfg.GRPCPort)
	if err != nil {
		return fmt.Errorf("failed to listen on gRPC port %s: %w", s.cfg.GRPCPort, err)
	}
	s.listener = lis

	slog.Info("gRPC server listening", "port", s.cfg.GRPCPort)
	return s.grpcServer.Serve(lis)
}

func (s *Server) GracefulStop() {
	if s.grpcServer != nil {
		slog.Info("Stopping gRPC server gracefully...")
		s.grpcServer.GracefulStop()
	}
}

func (s *Server) DispatchEvent(ctx context.Context, req *dispatcherv1.EventRequest) (*dispatcherv1.EventResponse, error) {
	// Validate inputs
	if req.TenantId == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id cannot be empty")
	}
	if req.EventType == "" {
		return nil, status.Error(codes.InvalidArgument, "event_type cannot be empty")
	}
	if len(req.Payload) == 0 {
		return nil, status.Error(codes.InvalidArgument, "payload cannot be empty")
	}

	// Validate JSON format
	if !json.Valid(req.Payload) {
		return nil, status.Error(codes.InvalidArgument, "payload must be valid JSON")
	}

	eventID := req.EventId
	if eventID == "" {
		eventID = uuid.NewString()
	}

	now := time.Now().UTC()

	// Persist event in PostgreSQL
	event := &domain.EventMessage{
		EventID:    eventID,
		TenantID:   req.TenantId,
		EventType:  req.EventType,
		Payload:    json.RawMessage(req.Payload),
		Status:     domain.StatusPending,
		RetryCount: 0,
		MaxRetries: s.cfg.MaxRetries,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	err := s.repo.CreateEvent(ctx, event)
	if err != nil {
		if errors.Is(err, postgres.ErrAlreadyExists) {
			s.metrics.EventsIngested.WithLabelValues(req.TenantId, req.EventType, "duplicate").Inc()
			return &dispatcherv1.EventResponse{
				EventId:            eventID,
				Status:             dispatcherv1.EventResponse_STATUS_DUPLICATE,
				Message:            "Event already exists and is being processed or was processed",
				AcceptedAtUnixMs:   now.UnixMilli(),
			}, nil
		}
		slog.Error("Failed to persist event in db", "event_id", eventID, "error", err)
		s.metrics.EventsIngested.WithLabelValues(req.TenantId, req.EventType, "error").Inc()
		return nil, status.Error(codes.Internal, "failed to persist event")
	}

	// Enqueue into dispatcher worker pool
	job := &worker.DispatchJob{
		EventID:    eventID,
		TenantID:   req.TenantId,
		EventType:  req.EventType,
		Payload:    json.RawMessage(req.Payload),
		RetryCount: 0,
		MaxRetries: s.cfg.MaxRetries,
	}

	if err := s.dispatcher.Enqueue(job); err != nil {
		slog.Warn("Dispatcher channel full, event will be picked up by fallback", "event_id", eventID, "error", err)
	}

	s.metrics.EventsIngested.WithLabelValues(req.TenantId, req.EventType, "accepted").Inc()

	return &dispatcherv1.EventResponse{
		EventId:          eventID,
		Status:           dispatcherv1.EventResponse_STATUS_ACCEPTED,
		Message:          "Event accepted for delivery",
		AcceptedAtUnixMs: now.UnixMilli(),
	}, nil
}

func (s *Server) unaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp interface{}, err error) {
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			slog.Error("gRPC panic recovered", "method", info.FullMethod, "panic", r)
			err = status.Errorf(codes.Internal, "internal server error: %v", r)
		}
	}()

	resp, err = handler(ctx, req)
	duration := time.Since(start)

	if err != nil {
		slog.Warn("gRPC request handled with error",
			"method", info.FullMethod,
			"duration_ms", duration.Milliseconds(),
			"error", err,
		)
	} else {
		slog.Debug("gRPC request handled successfully",
			"method", info.FullMethod,
			"duration_ms", duration.Milliseconds(),
		)
	}

	return resp, err
}
