package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Env      string
	LogLevel string

	// Server configurations
	GRPCPort string
	HTTPPort string

	// PostgreSQL
	PostgresURL            string
	PostgresMaxConns       int32
	PostgresMinConns       int32
	PostgresMaxConnIdle    time.Duration
	PostgresMaxConnLife    time.Duration

	// Redis
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// Worker Pool & Dispatcher
	WorkerConcurrency     int
	QueueBufferSize       int
	HTTPConnectTimeout    time.Duration
	HTTPTotalTimeout      time.Duration
	HTTPMaxIdleConns      int
	HTTPMaxConnsPerHost   int
	HTTPIdleConnTimeout   time.Duration

	// Retries & DLQ
	MaxRetries            int
	InitialRetryInterval  time.Duration
	MaxRetryInterval      time.Duration
	BackoffMultiplier     float64
	SchedulerPollInterval time.Duration
}

func Load() *Config {
	return &Config{
		Env:      getEnv("ENV", "development"),
		LogLevel: getEnv("LOG_LEVEL", "info"),

		GRPCPort: getEnv("GRPC_PORT", ":50051"),
		HTTPPort: getEnv("HTTP_PORT", ":8080"),

		PostgresURL:         getEnv("POSTGRES_URL", "postgres://postgres:postgres@localhost:5432/dispatcher_db?sslmode=disable"),
		PostgresMaxConns:    int32(getEnvAsInt("POSTGRES_MAX_CONNS", 50)),
		PostgresMinConns:    int32(getEnvAsInt("POSTGRES_MIN_CONNS", 10)),
		PostgresMaxConnIdle: getEnvAsDuration("POSTGRES_MAX_CONN_IDLE", 5*time.Minute),
		PostgresMaxConnLife: getEnvAsDuration("POSTGRES_MAX_CONN_LIFE", 1*time.Hour),

		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		RedisDB:       getEnvAsInt("REDIS_DB", 0),

		WorkerConcurrency:   getEnvAsInt("WORKER_CONCURRENCY", 50),
		QueueBufferSize:     getEnvAsInt("QUEUE_BUFFER_SIZE", 10000),
		HTTPConnectTimeout:  getEnvAsDuration("HTTP_CONNECT_TIMEOUT", 5*time.Second),
		HTTPTotalTimeout:    getEnvAsDuration("HTTP_TOTAL_TIMEOUT", 10*time.Second),
		HTTPMaxIdleConns:    getEnvAsInt("HTTP_MAX_IDLE_CONNS", 500),
		HTTPMaxConnsPerHost: getEnvAsInt("HTTP_MAX_CONNS_PER_HOST", 100),
		HTTPIdleConnTimeout: getEnvAsDuration("HTTP_IDLE_CONN_TIMEOUT", 90*time.Second),

		MaxRetries:            getEnvAsInt("MAX_RETRIES", 5),
		InitialRetryInterval:  getEnvAsDuration("INITIAL_RETRY_INTERVAL", 10*time.Second),
		MaxRetryInterval:      getEnvAsDuration("MAX_RETRY_INTERVAL", 1*time.Hour),
		BackoffMultiplier:     getEnvAsFloat("BACKOFF_MULTIPLIER", 2.0),
		SchedulerPollInterval: getEnvAsDuration("SCHEDULER_POLL_INTERVAL", 1*time.Second),
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvAsInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvAsFloat(key string, defaultVal float64) float64 {
	if val := os.Getenv(key); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return defaultVal
}

func getEnvAsDuration(key string, defaultVal time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}
