package postback

import (
	"context"
	"time"

	"github.com/shelik/postback/internal/domain"
	"github.com/shelik/postback/internal/storage"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	StatusPending   = domain.StatusPending
	StatusDelivered = domain.StatusDelivered
	StatusDead      = domain.StatusDead
)

type Config = domain.Config

type Postback = domain.Postback

type Repository = storage.Repository

// ErrInvalidConfig creates a configuration validation error.
func ErrInvalidConfig(reason string) error {
	return domain.ErrInvalidConfig(reason)
}

// MongoRepository is a public wrapper over the internal Mongo storage implementation.
type MongoRepository struct {
	impl *storage.MongoRepository
}

func NewMongoRepository(postbacks *mongo.Collection) (*MongoRepository, error) {
	impl, err := storage.NewMongoRepository(postbacks)
	if err != nil {
		return nil, err
	}

	return &MongoRepository{impl: impl}, nil
}

func (r *MongoRepository) EnsureIndexes(ctx context.Context) error {
	return r.impl.EnsureIndexes(ctx)
}

func (r *MongoRepository) Enqueue(ctx context.Context, pb Postback) (string, error) {
	return r.impl.Enqueue(ctx, pb)
}

func (r *MongoRepository) Claim(ctx context.Context, now time.Time, limit int, owner string, ttl time.Duration) ([]Postback, error) {
	return r.impl.Claim(ctx, now, limit, owner, ttl)
}

func (r *MongoRepository) MarkDelivered(ctx context.Context, id string, deliveredAt time.Time) error {
	return r.impl.MarkDelivered(ctx, id, deliveredAt)
}

func (r *MongoRepository) ScheduleRetry(ctx context.Context, id string, attempts int, retryAt time.Time, lastErr string) error {
	return r.impl.ScheduleRetry(ctx, id, attempts, retryAt, lastErr)
}

func (r *MongoRepository) MarkDead(ctx context.Context, id string, attempts int, lastErr string) error {
	return r.impl.MarkDead(ctx, id, attempts, lastErr)
}
