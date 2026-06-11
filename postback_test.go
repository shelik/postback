package postback

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

type fakeRepo struct {
	mu              sync.Mutex
	queue           []Postback
	enqueued        []Postback
	nextID          int64
	delivered       map[string]bool
	dead            map[string]bool
	retries         map[string]int
	lastRetryErr    map[string]string
	lastDeadErr     map[string]string
	lastRetryAt     map[string]time.Time
	claimedAttempts int
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		nextID:       1,
		delivered:    map[string]bool{},
		dead:         map[string]bool{},
		retries:      map[string]int{},
		lastRetryErr: map[string]string{},
		lastDeadErr:  map[string]string{},
		lastRetryAt:  map[string]time.Time{},
	}
}

func (f *fakeRepo) Enqueue(ctx context.Context, pb Postback) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	pb.ID = strconv.FormatInt(f.nextID, 10)
	f.nextID++
	f.enqueued = append(f.enqueued, pb)
	f.queue = append(f.queue, pb)
	return pb.ID, nil
}

func (f *fakeRepo) Claim(ctx context.Context, now time.Time, limit int, owner string, ttl time.Duration) ([]Postback, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimedAttempts++
	if len(f.queue) == 0 {
		return nil, nil
	}
	if limit > len(f.queue) {
		limit = len(f.queue)
	}
	out := make([]Postback, limit)
	copy(out, f.queue[:limit])
	f.queue = f.queue[limit:]
	return out, nil
}

func (f *fakeRepo) MarkDelivered(ctx context.Context, id string, deliveredAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delivered[id] = true
	return nil
}

func (f *fakeRepo) ScheduleRetry(ctx context.Context, id string, attempts int, retryAt time.Time, lastErr string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retries[id] = attempts
	f.lastRetryErr[id] = lastErr
	f.lastRetryAt[id] = retryAt
	for _, pb := range f.enqueued {
		if pb.ID == id {
			pb.Attempts = attempts
			f.queue = append(f.queue, pb)
			break
		}
	}
	return nil
}

func (f *fakeRepo) MarkDead(ctx context.Context, id string, attempts int, lastErr string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dead[id] = true
	f.retries[id] = attempts
	f.lastDeadErr[id] = lastErr
	return nil
}

func TestEnqueue_ContextCanceled(t *testing.T) {
	repo := newFakeRepo()
	svc, err := New(
		"TestService",
		WithConfig(Config{}),
		WithRepository(repo),
	)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = svc.Enqueue(ctx, Postback{URL: "http://example.org", Body: []byte("ok")})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got: %v", err)
	}
}

func TestWorker_RetryThenDeadAfterMaxRetries(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	repo := newFakeRepo()

	svc, err := New(
		"TestService",
		WithConfig(Config{
			RetryDelay:        50 * time.Millisecond,
			BackoffMultiplier: 1,
			MaxRetryDelay:     50 * time.Millisecond,
			MaxRetries:        2,
		}),
		WithRepository(repo),
	)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	_, err = svc.Enqueue(context.Background(), Postback{
		URL:    ts.URL,
		Method: http.MethodPost,
		Body:   []byte("payload"),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = svc.Start(ctx)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		repo.mu.Lock()
		isDead := repo.dead["1"]
		attempts := repo.retries["1"]
		retryAt := repo.lastRetryAt["1"]
		repo.mu.Unlock()
		if isDead && attempts == 2 && !retryAt.IsZero() {
			cancel()
			<-done
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	<-done
	t.Fatalf("postback did not reach dead state with expected attempts")
}

func TestRetryDelayForAttempt_ExponentialAndCap(t *testing.T) {
	svc, err := New(

		"TestService",
		WithConfig(Config{
			RetryDelay:        100 * time.Millisecond,
			BackoffMultiplier: 2,
			MaxRetryDelay:     350 * time.Millisecond,
		}),
		WithRepository(newFakeRepo()),
	)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 100 * time.Millisecond},
		{attempt: 2, want: 200 * time.Millisecond},
		{attempt: 3, want: 350 * time.Millisecond},
		{attempt: 4, want: 350 * time.Millisecond},
	}

	for _, tc := range cases {
		got := svc.retryDelayForAttempt(tc.attempt)
		if math.Abs(float64(got-tc.want)) > float64(5*time.Millisecond) {
			t.Fatalf("attempt=%d got=%s want=%s", tc.attempt, got, tc.want)
		}
	}
}
