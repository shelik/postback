package postback

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/shelik/postback/internal/domain"
	"github.com/shelik/postback/internal/storage"
	"github.com/shelik/postback/internal/worker"
	"github.com/shelik/postback/telemetry"
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

func WithRepository(repo storage.Repository) Option {
	return func(c *Client) {
		if repo != nil {
			c.repo = repo
		}
	}
}

func WithConfig(cfg domain.Config) Option {
	return func(c *Client) {
		cfg = cfg.WithDefaults()
		if err := cfg.Validate(); err == nil {
			c.cfg = cfg
		}
	}
}

// Client is the public entrypoint for scheduling and delivering postbacks.
type Client struct {
	name       string
	repo       storage.Repository
	cfg        domain.Config
	httpClient *http.Client
	logger     *log.Logger
	metrics    telemetry.Metrics
	pool       *worker.Pool
}

// New creates a new postback client.
func New(name string, opts ...Option) (*Client, error) {
	cfg := domain.Config{}.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	client := &Client{
		name: name,
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

	if client.repo == nil {
		return nil, errors.New("repository is required")
	}

	client.pool = worker.NewPool(client.repo, client.cfg, client.httpClient, client.logger, client.metrics)
	return client, nil
}

// Enqueue schedules a postback for async delivery.
func (c *Client) Enqueue(ctx context.Context, pb Postback) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if pb.URL == "" {
		return "", errors.New("url is required")
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
