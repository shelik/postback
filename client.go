package postback

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"time"

	"gitlab.com/libs/go/postback/internal/domain"
	"gitlab.com/libs/go/postback/internal/storage"
	"gitlab.com/libs/go/postback/internal/worker"
	"gitlab.com/libs/go/postback/telemetry"
)

// Option configures Client.
type Option func(*Client)

// WithHTTPClient injects a custom HTTP client.
func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) {
		if client != nil {
			c.httpClient = client
		}
	}
}

// WithLogger injects custom logger.
func WithLogger(logger *log.Logger) Option {
	return func(c *Client) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithMetrics injects metrics collector.
func WithMetrics(metrics telemetry.Metrics) Option {
	return func(c *Client) {
		if metrics != nil {
			c.metrics = metrics
		}
	}
}

// Client is the public entrypoint for scheduling and delivering postbacks.
type Client struct {
	repo       storage.Repository
	cfg        domain.Config
	httpClient *http.Client
	logger     *log.Logger
	metrics    telemetry.Metrics
	pool       *worker.Pool
}

// New creates a new postback client.
func New(repo Repository, cfg Config, opts ...Option) (*Client, error) {
	if repo == nil {
		return nil, errors.New("repository is required")
	}

	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	client := &Client{
		repo: repo,
		cfg:  cfg,
		httpClient: &http.Client{
			Timeout: cfg.RequestTimeout,
		},
		logger:  log.New(io.Discard, "", 0),
		metrics: telemetry.NoopMetrics{},
	}
	for _, opt := range opts {
		opt(client)
	}

	client.pool = worker.NewPool(repo, cfg, client.httpClient, client.logger, client.metrics)
	return client, nil
}

// Enqueue schedules a postback for async delivery.
func (c *Client) Enqueue(ctx context.Context, pb Postback) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if pb.URL == "" {
		return 0, errors.New("url is required")
	}
	if pb.Method == "" {
		pb.Method = http.MethodPost
	}
	pb.Status = StatusPending
	if pb.NextAttemptAt.IsZero() {
		pb.NextAttemptAt = time.Now().UTC()
	}

	return c.repo.Enqueue(ctx, pb)
}

// Start launches worker goroutines and blocks until context cancellation.
func (c *Client) Start(ctx context.Context) error {
	return c.pool.Start(ctx)
}

func (c *Client) retryDelayForAttempt(attempt int) time.Duration {
	return c.pool.RetryDelayForAttempt(attempt)
}
