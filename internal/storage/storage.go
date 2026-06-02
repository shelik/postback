package storage

import (
	"context"
	"time"

	"github.com/shelik/postback/internal/domain"
)

// Repository defines the storage contract used by workers and public wrappers.
type Repository interface {
	Enqueue(ctx context.Context, pb domain.Postback) (int64, error)
	Claim(ctx context.Context, now time.Time, limit int, owner string, ttl time.Duration) ([]domain.Postback, error)
	MarkDelivered(ctx context.Context, id int64, deliveredAt time.Time) error
	ScheduleRetry(ctx context.Context, id int64, attempts int, retryAt time.Time, lastErr string) error
	MarkDead(ctx context.Context, id int64, attempts int, lastErr string) error
}