package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics defines observability hooks for postback processing.
type Metrics interface {
	ObserveDeliverySuccess(duration time.Duration)
	ObserveDeliveryFailure(reason string, duration time.Duration)
	IncRetryScheduled()
	IncDead()
}

// NoopMetrics can be used when metrics are not needed.
type NoopMetrics struct{}

func (NoopMetrics) ObserveDeliverySuccess(duration time.Duration)             {}
func (NoopMetrics) ObserveDeliveryFailure(reason string, duration time.Duration) {}
func (NoopMetrics) IncRetryScheduled()                                        {}
func (NoopMetrics) IncDead()                                                  {}

// OTelMetrics is an OpenTelemetry-backed implementation.
type OTelMetrics struct {
	successDuration metric.Float64Histogram
	failureDuration metric.Float64Histogram
	retriesTotal    metric.Int64Counter
	deadTotal       metric.Int64Counter
}

// NewOTelMetrics creates metrics using the provided OpenTelemetry meter.
func NewOTelMetrics(meter metric.Meter) (*OTelMetrics, error) {
	if meter == nil {
		meter = otel.Meter("github.com/shelik/postback")
	}

	successDuration, err := meter.Float64Histogram(
		"postback.delivery.success.duration",
		metric.WithDescription("Duration of successful postback deliveries in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	failureDuration, err := meter.Float64Histogram(
		"postback.delivery.failure.duration",
		metric.WithDescription("Duration of failed postback deliveries in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	retriesTotal, err := meter.Int64Counter(
		"postback.retries.scheduled",
		metric.WithDescription("Total number of scheduled retries"),
	)
	if err != nil {
		return nil, err
	}

	deadTotal, err := meter.Int64Counter(
		"postback.dead",
		metric.WithDescription("Total number of postbacks marked as dead"),
	)
	if err != nil {
		return nil, err
	}

	return &OTelMetrics{
		successDuration: successDuration,
		failureDuration: failureDuration,
		retriesTotal:    retriesTotal,
		deadTotal:       deadTotal,
	}, nil
}

func (m *OTelMetrics) ObserveDeliverySuccess(duration time.Duration) {
	if m == nil {
		return
	}
	m.successDuration.Record(context.Background(), duration.Seconds())
}

func (m *OTelMetrics) ObserveDeliveryFailure(reason string, duration time.Duration) {
	if m == nil {
		return
	}
	if reason == "" {
		reason = "unknown"
	}
	opts := metric.WithAttributes(attribute.String("error.type", reason))
	m.failureDuration.Record(context.Background(), duration.Seconds(), opts)
}

func (m *OTelMetrics) IncRetryScheduled() {
	if m == nil {
		return
	}
	m.retriesTotal.Add(context.Background(), 1)
}

func (m *OTelMetrics) IncDead() {
	if m == nil {
		return
	}
	m.deadTotal.Add(context.Background(), 1)
}
