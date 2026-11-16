package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"webhook-dispatcher/internal/config"
)

const (
	RetryZSetKey         = "dispatcher:retry_zset"
	EndpointRateLimitKey = "dispatcher:ratelimit:endpoint:"
)

// DueItemsScript atomically fetches and removes items with score <= max_score.
var popDueItemsScript = redis.NewScript(`
	local key = KEYS[1]
	local max_score = ARGV[1]
	local limit = tonumber(ARGV[2])
	local items = redis.call('ZRANGEBYSCORE', key, '-inf', max_score, 'LIMIT', 0, limit)
	if #items > 0 then
		for i = 1, #items do
			redis.call('ZREM', key, items[i])
		end
	end
	return items
`)

// RateLimitScript checks sliding window rate limit (1 second window).
// KEYS[1]: ratelimit key
// ARGV[1]: current timestamp in milliseconds
// ARGV[2]: rate limit per second
var rateLimitScript = redis.NewScript(`
	local key = KEYS[1]
	local now = tonumber(ARGV[1])
	local limit = tonumber(ARGV[2])
	local window_start = now - 1000

	-- Remove items older than 1 second window
	redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

	local current_count = redis.call('ZCARD', key)
	if current_count < limit then
		redis.call('ZADD', key, now, now .. '-' .. math.random(1000000))
		redis.call('PEXPIRE', key, 2000)
		return 1
	else
		return 0
	end
`)

type RetryTask struct {
	EventID    string          `json:"event_id"`
	TenantID   string          `json:"tenant_id"`
	EventType  string          `json:"event_type"`
	Payload    json.RawMessage `json:"payload"`
	RetryCount int             `json:"retry_count"`
	MaxRetries int             `json:"max_retries"`
	EndpointID string          `json:"endpoint_id,omitempty"`
}

type Scheduler struct {
	client *redis.Client
}

func NewScheduler(cfg *config.Config) (*Scheduler, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
		PoolSize: 50,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return &Scheduler{client: client}, nil
}

func (s *Scheduler) Client() *redis.Client {
	return s.client
}

func (s *Scheduler) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

// ScheduleRetry adds an event task to the Redis Sorted Set with nextRetryAt as score.
func (s *Scheduler) ScheduleRetry(ctx context.Context, task *RetryTask, executeAt time.Time) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal retry task: %w", err)
	}

	score := float64(executeAt.UnixMilli())
	err = s.client.ZAdd(ctx, RetryZSetKey, redis.Z{
		Score:  score,
		Member: string(data),
	}).Err()
	if err != nil {
		return fmt.Errorf("zadd retry task: %w", err)
	}
	return nil
}

// PopDueTasks atomically retrieves and removes up to batchSize tasks due for execution.
func (s *Scheduler) PopDueTasks(ctx context.Context, batchSize int) ([]*RetryTask, error) {
	if batchSize <= 0 {
		batchSize = 100
	}

	nowMs := time.Now().UnixMilli()
	res, err := popDueItemsScript.Run(ctx, s.client, []string{RetryZSetKey}, nowMs, batchSize).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("pop due tasks lua: %w", err)
	}

	rawItems, ok := res.([]interface{})
	if !ok || len(rawItems) == 0 {
		return nil, nil
	}

	var tasks []*RetryTask
	for _, item := range rawItems {
		itemStr, ok := item.(string)
		if !ok {
			continue
		}

		var t RetryTask
		if err := json.Unmarshal([]byte(itemStr), &t); err != nil {
			continue
		}
		tasks = append(tasks, &t)
	}

	return tasks, nil
}

// GetQueueSize returns the count of scheduled retry tasks pending in Redis.
func (s *Scheduler) GetQueueSize(ctx context.Context) (int64, error) {
	return s.client.ZCard(ctx, RetryZSetKey).Result()
}

// AllowEndpoint checks whether the endpoint has exceeded its rate limit in the last 1 second window.
func (s *Scheduler) AllowEndpoint(ctx context.Context, endpointID string, limitPerSec int) (bool, error) {
	if limitPerSec <= 0 {
		return true, nil
	}

	key := EndpointRateLimitKey + endpointID
	now := time.Now().UnixMilli()

	res, err := rateLimitScript.Run(ctx, s.client, []string{key}, now, limitPerSec).Int()
	if err != nil {
		// In case of Redis script error, allow through gracefully rather than completely dropping delivery
		return true, fmt.Errorf("eval rate limit script: %w", err)
	}

	return res == 1, nil
}
