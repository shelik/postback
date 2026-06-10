package storage

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/shelik/postback/internal/domain"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoRepository stores postbacks in MongoDB.
type MongoRepository struct {
	postbacks *mongo.Collection
}

func NewMongoRepository(db *mongo.Database) (*MongoRepository, error) {
	if db == nil {
		return nil, errors.New("db is required")
	}
	return &MongoRepository{
		postbacks: db.Collection("postbacks"),
	}, nil
}

// EnsureIndexes creates required indexes for postback processing.
func (r *MongoRepository) EnsureIndexes(ctx context.Context) error {
	models := []mongo.IndexModel{{
		Keys: bson.D{
			{Key: "status", Value: 1},
			{Key: "next_attempt_at", Value: 1},
			{Key: "locked_until", Value: 1},
			{Key: "created_at", Value: 1},
		},
	}}
	_, err := r.postbacks.Indexes().CreateMany(ctx, models)
	return err
}

func (r *MongoRepository) Enqueue(ctx context.Context, pb domain.Postback) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	id := uuid.NewString()

	now := time.Now().UTC()
	if pb.NextAttemptAt.IsZero() {
		pb.NextAttemptAt = now
	}
	if pb.Method == "" {
		pb.Method = "POST"
	}

	doc := bson.M{
		"_id":             id,
		"url":             pb.URL,
		"method":          pb.Method,
		"headers":         pb.Headers,
		"body":            pb.Body,
		"status":          domain.StatusPending,
		"attempts":        0,
		"last_error":      "",
		"next_attempt_at": pb.NextAttemptAt.UTC(),
		"locked_until":    nil,
		"locked_by":       nil,
		"created_at":      now,
		"updated_at":      now,
		"delivered_at":    nil,
	}

	_, err := r.postbacks.InsertOne(ctx, doc)
	if err != nil {
		return "", err
	}

	return id, nil
}

func (r *MongoRepository) Claim(ctx context.Context, now time.Time, limit int, owner string, ttl time.Duration) ([]domain.Postback, error) {
	if limit <= 0 {
		return nil, nil
	}

	lockUntil := now.UTC().Add(ttl)
	result := make([]domain.Postback, 0, limit)

	for i := 0; i < limit; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		filter := bson.M{
			"status":          domain.StatusPending,
			"next_attempt_at": bson.M{"$lte": now.UTC()},
			"$or": []bson.M{
				{"locked_until": bson.M{"$exists": false}},
				{"locked_until": nil},
				{"locked_until": bson.M{"$lte": now.UTC()}},
			},
		}
		update := bson.M{
			"$set": bson.M{
				"locked_until": lockUntil,
				"locked_by":    owner,
				"updated_at":   time.Now().UTC(),
			},
		}

		opts := options.FindOneAndUpdate().
			SetSort(bson.D{{Key: "created_at", Value: 1}}).
			SetReturnDocument(options.After)

		var doc postbackDoc
		err := r.postbacks.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)
		if errors.Is(err, mongo.ErrNoDocuments) {
			break
		}
		if err != nil {
			return nil, err
		}
		result = append(result, doc.toModel())
	}

	return result, nil
}

func (r *MongoRepository) MarkDelivered(ctx context.Context, id string, deliveredAt time.Time) error {
	update := bson.M{
		"$set": bson.M{
			"status":       domain.StatusDelivered,
			"delivered_at": deliveredAt.UTC(),
			"updated_at":   time.Now().UTC(),
			"locked_until": nil,
			"locked_by":    nil,
		},
	}
	_, err := r.postbacks.UpdateByID(ctx, id, update)
	return err
}

func (r *MongoRepository) ScheduleRetry(ctx context.Context, id string, attempts int, retryAt time.Time, lastErr string) error {
	update := bson.M{
		"$set": bson.M{
			"attempts":        attempts,
			"status":          domain.StatusPending,
			"last_error":      lastErr,
			"next_attempt_at": retryAt.UTC(),
			"updated_at":      time.Now().UTC(),
			"locked_until":    nil,
			"locked_by":       nil,
		},
	}
	_, err := r.postbacks.UpdateByID(ctx, id, update)
	return err
}

func (r *MongoRepository) MarkDead(ctx context.Context, id string, attempts int, lastErr string) error {
	update := bson.M{
		"$set": bson.M{
			"attempts":     attempts,
			"status":       domain.StatusDead,
			"last_error":   lastErr,
			"updated_at":   time.Now().UTC(),
			"locked_until": nil,
			"locked_by":    nil,
		},
	}
	_, err := r.postbacks.UpdateByID(ctx, id, update)
	return err
}

type postbackDoc struct {
	ID            string            `bson:"_id"`
	URL           string            `bson:"url"`
	Method        string            `bson:"method"`
	Headers       map[string]string `bson:"headers"`
	Body          []byte            `bson:"body"`
	Status        string            `bson:"status"`
	Attempts      int               `bson:"attempts"`
	LastError     string            `bson:"last_error"`
	NextAttemptAt time.Time         `bson:"next_attempt_at"`
	CreatedAt     time.Time         `bson:"created_at"`
	UpdatedAt     time.Time         `bson:"updated_at"`
	DeliveredAt   *time.Time        `bson:"delivered_at"`
}

func (d postbackDoc) toModel() domain.Postback {
	return domain.Postback{
		ID:            d.ID,
		URL:           d.URL,
		Method:        d.Method,
		Headers:       d.Headers,
		Body:          d.Body,
		Status:        d.Status,
		Attempts:      d.Attempts,
		LastError:     d.LastError,
		NextAttemptAt: d.NextAttemptAt,
		CreatedAt:     d.CreatedAt,
		UpdatedAt:     d.UpdatedAt,
		DeliveredAt:   d.DeliveredAt,
	}
}
