package domain

import (
	"fmt"
	"time"
)

const (
	StatusPending   = "pending"
	StatusDelivered = "delivered"
	StatusDead      = "dead"
)

// Config controls worker behavior and retry policy.
type Config struct {
	Workers           int
	MaxRetries        int
	RetryDelay        time.Duration
	BackoffMultiplier float64
	MaxRetryDelay     time.Duration
	PollInterval      time.Duration
	ClaimBatchSize    int
	ClaimTTL          time.Duration
	RequestTimeout    time.Duration
	ShutdownWaitTime  time.Duration
}

func (c Config) WithDefaults() Config {
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 5
	}
	if c.RetryDelay <= 0 {
		c.RetryDelay = 3 * time.Second
	}
	if c.BackoffMultiplier <= 0 {
		c.BackoffMultiplier = 2
	}
	if c.MaxRetryDelay <= 0 {
		c.MaxRetryDelay = 30 * time.Second
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 500 * time.Millisecond
	}
	if c.ClaimBatchSize <= 0 {
		c.ClaimBatchSize = 25
	}
	if c.ClaimTTL <= 0 {
		c.ClaimTTL = 30 * time.Second
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 10 * time.Second
	}
	if c.ShutdownWaitTime <= 0 {
		c.ShutdownWaitTime = 10 * time.Second
	}

	return c
}

func (c Config) Validate() error {
	if c.Workers < 1 {
		return ErrInvalidConfig("workers must be >= 1")
	}
	if c.MaxRetries < 1 {
		return ErrInvalidConfig("max retries must be >= 1")
	}
	if c.RetryDelay < 0 {
		return ErrInvalidConfig("retry delay must be >= 0")
	}
	if c.BackoffMultiplier < 1 {
		return ErrInvalidConfig("backoff multiplier must be >= 1")
	}
	if c.MaxRetryDelay <= 0 {
		return ErrInvalidConfig("max retry delay must be > 0")
	}
	if c.PollInterval <= 0 {
		return ErrInvalidConfig("poll interval must be > 0")
	}
	if c.ClaimBatchSize < 1 {
		return ErrInvalidConfig("claim batch size must be >= 1")
	}
	if c.ClaimTTL <= 0 {
		return ErrInvalidConfig("claim ttl must be > 0")
	}
	if c.RequestTimeout <= 0 {
		return ErrInvalidConfig("request timeout must be > 0")
	}
	if c.ShutdownWaitTime <= 0 {
		return ErrInvalidConfig("shutdown wait time must be > 0")
	}

	return nil
}

// Postback stores one delivery job.
type Postback struct {
	ID            int64
	URL           string
	Method        string
	Headers       map[string]string
	Body          []byte
	Status        string
	Attempts      int
	LastError     string
	NextAttemptAt time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
	DeliveredAt   *time.Time
}

type invalidConfigError struct {
	reason string
}

func (e invalidConfigError) Error() string {
	return fmt.Sprintf("invalid config: %s", e.reason)
}

// ErrInvalidConfig creates a configuration validation error.
func ErrInvalidConfig(reason string) error {
	return invalidConfigError{reason: reason}
}
