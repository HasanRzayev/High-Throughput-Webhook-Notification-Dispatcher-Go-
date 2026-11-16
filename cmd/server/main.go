package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"webhook-dispatcher/internal/config"
	"webhook-dispatcher/internal/metrics"
	memSched "webhook-dispatcher/internal/queue/memory"
	"webhook-dispatcher/internal/queue/redis"
	memRepo "webhook-dispatcher/internal/repository/memory"
	"webhook-dispatcher/internal/repository/postgres"
	grpcTransport "webhook-dispatcher/internal/transport/grpc"
	httpTransport "webhook-dispatcher/internal/transport/http"
	"webhook-dispatcher/internal/worker"
	"webhook-dispatcher/migrations"
)

func main() {
	cfg := config.Load()
	setupLogger(cfg.LogLevel)

	slog.Info("Starting webhook-dispatcher microservice",
		"env", cfg.Env,
		"grpc_port", cfg.GRPCPort,
		"http_port", cfg.HTTPPort,
		"worker_concurrency", cfg.WorkerConcurrency,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize Storage (PostgreSQL with In-Memory fallback)
	var repo httpTransport.Repository
	pgRepo, err := postgres.NewRepository(ctx, cfg)
	if err != nil {
		slog.Warn("PostgreSQL not reachable, running in high-performance IN-MEMORY mode", "error", err)
		repo = memRepo.NewRepository()
	} else {
		defer pgRepo.Close()
		if err := pgRepo.EnsureSchema(ctx, migrations.InitSchemaSQL); err != nil {
			slog.Warn("Failed to run postgres schema migrations, falling back to in-memory", "error", err)
			repo = memRepo.NewRepository()
		} else {
			slog.Info("PostgreSQL connection established & schema verified")
			repo = pgRepo
		}
	}

	// 2. Initialize Scheduler (Redis with In-Memory fallback)
	var scheduler worker.RetryScheduler
	redisSched, err := redis.NewScheduler(cfg)
	if err != nil {
		slog.Warn("Redis not reachable, running in high-performance IN-MEMORY scheduler mode", "error", err)
		scheduler = memSched.NewScheduler()
	} else {
		defer redisSched.Close()
		slog.Info("Redis connection established")
		scheduler = redisSched
	}

	// 3. Initialize Prometheus Metrics
	reg := prometheus.DefaultRegisterer
	appMetrics := metrics.NewMetrics(reg)

	// 4. Initialize Worker Pool Dispatcher
	dispatcher := worker.NewDispatcher(cfg, repo, scheduler, appMetrics)
	dispatcher.Start()

	// 5. Initialize gRPC Transport Server
	grpcServer := grpcTransport.NewServer(cfg, repo, dispatcher, appMetrics)
	go func() {
		if err := grpcServer.Start(); err != nil {
			slog.Error("gRPC server error", "error", err)
		}
	}()

	// 6. Initialize HTTP Transport Server & Dashboard
	httpServer := httpTransport.NewServer(cfg, repo, dispatcher, appMetrics)
	go func() {
		if err := httpServer.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server error", "error", err)
		}
	}()

	slog.Info("All servers running. Open Web UI Dashboard at http://localhost" + cfg.HTTPPort)

	// 7. Wait for OS Termination Signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	sig := <-sigChan

	slog.Info("Shutdown signal received, starting graceful teardown", "signal", sig.String())

	// Phase 1: Stop accepting incoming requests
	grpcServer.GracefulStop()

	httpCtx, httpCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer httpCancel()
	_ = httpServer.GracefulStop(httpCtx)

	// Phase 2: Drain worker pool and retry poller
	dispatcher.Stop(10 * time.Second)

	slog.Info("Webhook dispatcher microservice successfully shut down")
}

func setupLogger(levelStr string) {
	var level slog.Level
	switch levelStr {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: level,
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
	slog.SetDefault(logger)
}
