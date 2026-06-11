package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/shelik/postback/internal/domain"
	"github.com/shelik/postback/telemetry"
)

// Repository defines the storage operations required by the worker pool.
type Repository interface {
	Claim(ctx context.Context, now time.Time, limit int, owner string, ttl time.Duration) ([]domain.Postback, error)
	MarkDelivered(ctx context.Context, id string, deliveredAt time.Time) error
	ScheduleRetry(ctx context.Context, id string, attempts int, retryAt time.Time, lastErr string) error
	MarkDead(ctx context.Context, id string, attempts int, lastErr string) error
}

// Pool executes queued postbacks with retry semantics.
type Pool struct {
	repo       Repository
	cfg        domain.Config
	httpClient *http.Client
	logger     *log.Logger
	metrics    telemetry.Metrics
}

func NewPool(repo Repository, cfg domain.Config, httpClient *http.Client, logger *log.Logger, metrics telemetry.Metrics) *Pool {
	return &Pool{
		repo:       repo,
		cfg:        cfg,
		httpClient: httpClient,
		logger:     logger,
		metrics:    metrics,
	}
}

func (p *Pool) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var wg sync.WaitGroup
	for i := 0; i < p.cfg.Workers; i++ {
		wg.Add(1)
		owner := fmt.Sprintf("worker-%d", i+1)
		go func(owner string) {
			defer wg.Done()
			p.workerLoop(ctx, owner)
		}(owner)
	}

	<-ctx.Done()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(p.cfg.ShutdownWaitTime):
		p.logger.Printf("postback: shutdown wait timeout reached")
		return nil
	}
}

func (p *Pool) workerLoop(ctx context.Context, owner string) {
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		now := time.Now().UTC()
		batch, err := p.repo.Claim(ctx, now, p.cfg.ClaimBatchSize, owner, p.cfg.ClaimTTL)
		if err != nil {
			p.logger.Printf("postback: claim failed for %s: %v", owner, err)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			continue
		}

		if len(batch) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			continue
		}

		for _, pb := range batch {
			if err := ctx.Err(); err != nil {
				return
			}
			p.handlePostback(ctx, pb)
		}
	}
}

func (p *Pool) handlePostback(ctx context.Context, pb domain.Postback) {
	if err := ctx.Err(); err != nil {
		return
	}

	attempts := pb.Attempts + 1
	started := time.Now()
	if err := p.send(ctx, pb); err != nil {
		errText := err.Error()
		p.metrics.ObserveDeliveryFailure(errText, time.Since(started))
		if attempts >= p.cfg.MaxRetries {
			if markErr := p.repo.MarkDead(ctx, pb.ID, attempts, errText); markErr != nil {
				p.logger.Printf("postback: mark dead failed id=%s: %v", pb.ID, markErr)
			}
			p.metrics.IncDead()
			return
		}

		retryAt := time.Now().UTC().Add(p.RetryDelayForAttempt(attempts))
		if retryErr := p.repo.ScheduleRetry(ctx, pb.ID, attempts, retryAt, errText); retryErr != nil {
			p.logger.Printf("postback: schedule retry failed id=%s: %v", pb.ID, retryErr)
		}
		p.metrics.IncRetryScheduled()
		return
	}

	deliveredAt := time.Now().UTC()
	if err := p.repo.MarkDelivered(ctx, pb.ID, deliveredAt); err != nil {
		p.logger.Printf("postback: mark delivered failed id=%s: %v", pb.ID, err)
		p.metrics.ObserveDeliveryFailure(err.Error(), time.Since(started))
		return
	}
	p.metrics.ObserveDeliverySuccess(time.Since(started))
}

func (p *Pool) RetryDelayForAttempt(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	multiplier := 1.0
	for i := 1; i < attempt; i++ {
		multiplier *= p.cfg.BackoffMultiplier
	}

	delay := time.Duration(float64(p.cfg.RetryDelay) * multiplier)
	if delay > p.cfg.MaxRetryDelay {
		return p.cfg.MaxRetryDelay
	}

	return delay
}

func (p *Pool) send(ctx context.Context, pb domain.Postback) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, pb.Method, pb.URL, bytes.NewReader(pb.Body))
	if err != nil {
		return err
	}
	for key, value := range pb.Headers {
		req.Header.Set(key, value)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if pb.SuccessStatusCodes == nil {
		pb.SuccessStatusCodes = []int{200, 201, 202, 204}
	}
	if !slices.Contains(pb.SuccessStatusCodes, resp.StatusCode) {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
