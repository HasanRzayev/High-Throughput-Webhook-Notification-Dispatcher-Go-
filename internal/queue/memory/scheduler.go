package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"webhook-dispatcher/internal/queue/redis"
)

type scheduledItem struct {
	task      *redis.RetryTask
	executeAt time.Time
}

type Scheduler struct {
	mu          sync.Mutex
	items       []*scheduledItem
	rateWindows map[string][]int64
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		rateWindows: make(map[string][]int64),
	}
}

func (s *Scheduler) Close() error {
	return nil
}

func (s *Scheduler) ScheduleRetry(ctx context.Context, task *redis.RetryTask, executeAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items = append(s.items, &scheduledItem{
		task:      task,
		executeAt: executeAt,
	})

	sort.Slice(s.items, func(i, j int) bool {
		return s.items[i].executeAt.Before(s.items[j].executeAt)
	})

	return nil
}

func (s *Scheduler) PopDueTasks(ctx context.Context, batchSize int) ([]*redis.RetryTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	var due []*redis.RetryTask
	var remaining []*scheduledItem

	for _, item := range s.items {
		if (item.executeAt.Before(now) || item.executeAt.Equal(now)) && len(due) < batchSize {
			due = append(due, item.task)
		} else {
			remaining = append(remaining, item)
		}
	}

	s.items = remaining
	return due, nil
}

func (s *Scheduler) GetQueueSize(ctx context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.items)), nil
}

func (s *Scheduler) AllowEndpoint(ctx context.Context, endpointID string, limitPerSec int) (bool, error) {
	if limitPerSec <= 0 {
		return true, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UnixMilli()
	windowStart := now - 1000

	timestamps := s.rateWindows[endpointID]
	var valid []int64
	for _, ts := range timestamps {
		if ts > windowStart {
			valid = append(valid, ts)
		}
	}

	if len(valid) < limitPerSec {
		valid = append(valid, now)
		s.rateWindows[endpointID] = valid
		return true, nil
	}

	s.rateWindows[endpointID] = valid
	return false, nil
}
