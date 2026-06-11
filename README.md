# postback

A Go package for reliable (at-least-once) HTTP postback delivery with clean separation between public API and internal implementation.

## Structure

```text
postback/
├── go.mod
├── README.md
├── postback_test.go
├── postback.go
├── client.go
├── internal/
│   ├── domain/
│   │   └── domain.go
│   ├── storage/
│   │   └── mongo.go
│   └── worker/
│       └── pool.go
└── telemetry/
    └── telemetry.go
```

## Features

- Queue postbacks to durable storage via the public `Client`.
- Asynchronous processing with multiple workers.
- Retries with exponential backoff and maximum delay cap.
- Custom `http.Client` injection.
- Canceled context checks before enqueue and delivery.
- Optional metrics via the `telemetry` package with OpenTelemetry.
- MongoDB storage with UUID IDs, atomic claim, and indexes.

## Configuration

```go
cfg := postback.Config{
    Workers:           8,
    MaxRetries:        5,
    RetryDelay:        2 * time.Second,
    BackoffMultiplier: 2,
    MaxRetryDelay:     30 * time.Second,
    PollInterval:      500 * time.Millisecond,
    ClaimBatchSize:    50,
    ClaimTTL:          30 * time.Second,
    RequestTimeout:    5 * time.Second,
}
```
Or it could be injected in global env config like:

```go
type BasicConfig struct {
    postback postback.Config
}

```

## Quick Start

```go
import "go.opentelemetry.io/otel/sdk/metric"

repo, err := postback.NewMongoRepository(db)
if err != nil {
    panic(err)
}

if err := repo.EnsureIndexes(ctx); err != nil {
    panic(err)
}

reader := metric.NewPeriodicReader(/* exporter */)
provider := metric.NewMeterProvider(metric.WithReader(reader))
meter := provider.Meter("gitlab.com/hglobal/base/source/base-legacy/cabinet")

metrics, err := telemetry.NewOTelMetrics(meter)
if err != nil {
    panic(err)
}

client, err := postback.New(
    "TestService",
    WithConfig(Config{
        RetryDelay:        50 * time.Millisecond,
        BackoffMultiplier: 1,
        MaxRetryDelay:     50 * time.Millisecond,
        MaxRetries:        2,
    }),
    WithRepository(repo),
    postback.WithHTTPClient(&http.Client{Timeout: 3 * time.Second}),
    postback.WithMetrics(metrics),
)
if err != nil {
    panic(err)
}

go func() {
    _ = client.Start(ctx)
}()

_, err = client.Enqueue(ctx, postback.Postback{
    URL:    "https://example.org/callback",
    Method: http.MethodPost,
    Headers: map[string]string{
        "Content-Type": "application/json",
    },
    Body: []byte(`{"event":"payment.success"}`),
})
if err != nil {
    panic(err)
}
```

## MongoDB

`MongoRepository` claims tasks from MongoDB atomically: a single `FindOneAndUpdate` query finds a suitable record and immediately marks it as claimed. This prevents multiple workers from claiming the same task simultaneously.
