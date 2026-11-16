package http

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"webhook-dispatcher/internal/config"
	"webhook-dispatcher/internal/domain"
	"webhook-dispatcher/internal/metrics"
	"webhook-dispatcher/internal/repository/postgres"
	"webhook-dispatcher/internal/worker"
)

//go:embed dashboard.html
var dashboardHTML string

type Repository interface {
	worker.Repository
	Ping(ctx context.Context) error
	CreateEndpoint(ctx context.Context, ep *domain.WebhookEndpoint) error
	ListEndpoints(ctx context.Context, tenantID string) ([]*domain.WebhookEndpoint, error)
	GetEvent(ctx context.Context, eventID string) (*domain.EventMessage, error)
	GetAttemptsByEvent(ctx context.Context, eventID string) ([]*domain.DeliveryAttempt, error)
	ListDLQ(ctx context.Context, limit, offset int) ([]*domain.DLQItem, error)
	GetDLQItem(ctx context.Context, id string) (*domain.DLQItem, error)
	MarkDLQReplayed(ctx context.Context, id string) error
}

type Server struct {
	echo       *echo.Echo
	cfg        *config.Config
	repo       Repository
	dispatcher *worker.Dispatcher
	metrics    *metrics.Metrics
}

type CreateEndpointRequest struct {
	TenantID        string `json:"tenant_id"`
	EventType       string `json:"event_type"`
	TargetURL       string `json:"target_url"`
	SecretKey       string `json:"secret_key"`
	RateLimitPerSec int    `json:"rate_limit_per_sec"`
}

type DispatchTestRequest struct {
	EventID   string `json:"event_id"`
	TenantID  string `json:"tenant_id"`
	EventType string `json:"event_type"`
	Payload   string `json:"payload"`
}

func NewServer(
	cfg *config.Config,
	repo Repository,
	dispatcher *worker.Dispatcher,
	m *metrics.Metrics,
) *Server {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	s := &Server{
		echo:       e,
		cfg:        cfg,
		repo:       repo,
		dispatcher: dispatcher,
		metrics:    m,
	}

	e.Use(middleware.Recover())
	e.Use(middleware.CORS())

	s.setupRoutes()
	return s
}

func (s *Server) setupRoutes() {
	// Web UI Dashboard
	s.echo.GET("/", s.handleDashboard)
	s.echo.GET("/dashboard", s.handleDashboard)

	// Prometheus metrics
	s.echo.GET("/metrics", echo.WrapHandler(promhttp.Handler()))

	// Health check
	s.echo.GET("/healthz", s.handleHealthz)

	api := s.echo.Group("/api/v1")
	{
		// Webhook Endpoints
		api.POST("/endpoints", s.handleCreateEndpoint)
		api.GET("/endpoints", s.handleListEndpoints)
		api.GET("/endpoints/:id", s.handleGetEndpoint)

		// Event inspection & manual dispatch
		api.POST("/dispatch", s.handleManualDispatch)
		api.GET("/events/:id", s.handleGetEvent)

		// Dead Letter Queue (DLQ)
		api.GET("/dlq", s.handleListDLQ)
		api.GET("/dlq/:id", s.handleGetDLQ)
		api.POST("/dlq/:id/replay", s.handleReplayDLQ)
	}
}

func (s *Server) Start() error {
	slog.Info("HTTP management & monitoring server listening", "port", s.cfg.HTTPPort)
	return s.echo.Start(s.cfg.HTTPPort)
}

func (s *Server) GracefulStop(ctx context.Context) error {
	slog.Info("Stopping HTTP server gracefully...")
	return s.echo.Shutdown(ctx)
}

// GET /healthz
func (s *Server) handleHealthz(c echo.Context) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), 2*time.Second)
	defer cancel()

	if err := s.repo.Ping(ctx); err != nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]string{
			"status": "unhealthy",
			"error":  "storage connection failure: " + err.Error(),
		})
	}

	return c.JSON(http.StatusOK, map[string]string{
		"status": "healthy",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

// POST /api/v1/dispatch
func (s *Server) handleManualDispatch(c echo.Context) error {
	var req DispatchTestRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid request body"})
	}

	if req.TenantID == "" || req.EventType == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "tenant_id and event_type are required"})
	}

	if req.EventID == "" {
		req.EventID = "evt-web-" + strconv.FormatInt(time.Now().UnixNano()%1000000, 10)
	}

	payloadBytes := []byte(req.Payload)
	if len(payloadBytes) == 0 {
		payloadBytes = []byte(fmt.Sprintf(`{"payment_id":"pay_%s","amount":59.99,"currency":"USD","status":"completed"}`, req.EventID))
	}

	now := time.Now().UTC()
	event := &domain.EventMessage{
		EventID:    req.EventID,
		TenantID:   req.TenantID,
		EventType:  req.EventType,
		Payload:    payloadBytes,
		Status:     domain.StatusPending,
		RetryCount: 0,
		MaxRetries: s.cfg.MaxRetries,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := s.repo.CreateEvent(c.Request().Context(), event); err != nil {
		if !errors.Is(err, postgres.ErrAlreadyExists) {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": "create event: " + err.Error()})
		}
	}

	job := &worker.DispatchJob{
		EventID:    req.EventID,
		TenantID:   req.TenantID,
		EventType:  req.EventType,
		Payload:    payloadBytes,
		RetryCount: 0,
		MaxRetries: s.cfg.MaxRetries,
	}

	if err := s.dispatcher.Enqueue(job); err != nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "worker queue full: " + err.Error()})
	}

	return c.JSON(http.StatusAccepted, map[string]interface{}{
		"status":   "ACCEPTED",
		"event_id": req.EventID,
		"message":  "Event enqueued for delivery",
	})
}

// POST /api/v1/endpoints
func (s *Server) handleCreateEndpoint(c echo.Context) error {
	var req CreateEndpointRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid request body"})
	}

	if req.TenantID == "" || req.TargetURL == "" || req.EventType == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "tenant_id, target_url, and event_type are required"})
	}

	if req.SecretKey == "" {
		req.SecretKey = uuid.NewString()
	}

	if req.RateLimitPerSec <= 0 {
		req.RateLimitPerSec = 100
	}

	endpoint := &domain.WebhookEndpoint{
		ID:              uuid.NewString(),
		TenantID:        req.TenantID,
		EventType:       req.EventType,
		TargetURL:       req.TargetURL,
		SecretKey:       req.SecretKey,
		RateLimitPerSec: req.RateLimitPerSec,
		IsActive:        true,
	}

	if err := s.repo.CreateEndpoint(c.Request().Context(), endpoint); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusCreated, endpoint)
}

// GET /api/v1/endpoints
func (s *Server) handleListEndpoints(c echo.Context) error {
	tenantID := c.QueryParam("tenant_id")
	endpoints, err := s.repo.ListEndpoints(c.Request().Context(), tenantID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, endpoints)
}

// GET /api/v1/endpoints/:id
func (s *Server) handleGetEndpoint(c echo.Context) error {
	id := c.Param("id")
	endpoint, err := s.repo.GetEndpoint(c.Request().Context(), id)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"error": "endpoint not found"})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, endpoint)
}

// GET /api/v1/events/:id
func (s *Server) handleGetEvent(c echo.Context) error {
	id := c.Param("id")
	ctx := c.Request().Context()

	event, err := s.repo.GetEvent(ctx, id)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"error": "event not found"})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	attempts, err := s.repo.GetAttemptsByEvent(ctx, id)
	if err != nil {
		slog.Warn("Failed to fetch attempts for event", "event_id", id, "error", err)
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"event":    event,
		"attempts": attempts,
	})
}

// GET /api/v1/dlq
func (s *Server) handleListDLQ(c echo.Context) error {
	limit, _ := strconv.Atoi(c.QueryParam("limit"))
	offset, _ := strconv.Atoi(c.QueryParam("offset"))

	items, err := s.repo.ListDLQ(c.Request().Context(), limit, offset)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, items)
}

// GET /api/v1/dlq/:id
func (s *Server) handleGetDLQ(c echo.Context) error {
	id := c.Param("id")
	item, err := s.repo.GetDLQItem(c.Request().Context(), id)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"error": "dlq item not found"})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, item)
}

// POST /api/v1/dlq/:id/replay
func (s *Server) handleReplayDLQ(c echo.Context) error {
	id := c.Param("id")
	ctx := c.Request().Context()

	item, err := s.repo.GetDLQItem(ctx, id)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"error": "dlq item not found"})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	if err := s.repo.UpdateEventStatus(ctx, item.EventID, domain.StatusPending, 0, nil); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to reset event status: " + err.Error()})
	}

	if err := s.repo.MarkDLQReplayed(ctx, item.ID); err != nil {
		slog.Warn("Failed to mark dlq replayed", "dlq_id", item.ID, "error", err)
	}

	job := &worker.DispatchJob{
		EventID:    item.EventID,
		TenantID:   item.TenantID,
		EventType:  item.EventType,
		Payload:    item.Payload,
		RetryCount: 0,
		MaxRetries: s.cfg.MaxRetries,
	}

	if err := s.dispatcher.Enqueue(job); err != nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "worker queue full: " + err.Error()})
	}

	s.metrics.DLQTotal.WithLabelValues(item.TenantID, item.EventType, "replayed").Inc()

	return c.JSON(http.StatusOK, map[string]interface{}{
		"message":  "Event successfully replayed",
		"event_id": item.EventID,
		"dlq_id":   item.ID,
	})
}

// GET / and GET /dashboard
func (s *Server) handleDashboard(c echo.Context) error {
	return c.HTML(http.StatusOK, dashboardHTML)
}
