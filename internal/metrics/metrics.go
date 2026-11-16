package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	EventsIngested    *prometheus.CounterVec
	DeliveriesTotal   *prometheus.CounterVec
	DeliveryDuration  *prometheus.HistogramVec
	QueueLength       *prometheus.GaugeVec
	DLQTotal          *prometheus.CounterVec
	ActiveWorkers     prometheus.Gauge
	RateLimitedTotal  *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	return &Metrics{
		EventsIngested: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dispatcher_events_ingested_total",
				Help: "Total number of webhook events ingested via gRPC",
			},
			[]string{"tenant_id", "event_type", "status"},
		),
		DeliveriesTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dispatcher_deliveries_total",
				Help: "Total number of webhook HTTP delivery attempts",
			},
			[]string{"tenant_id", "event_type", "status_code", "result"},
		),
		DeliveryDuration: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "dispatcher_delivery_duration_seconds",
				Help:    "Execution duration of webhook HTTP delivery in seconds",
				Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
			},
			[]string{"tenant_id", "event_type"},
		),
		QueueLength: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "dispatcher_queue_length",
				Help: "Current size of internal queues",
			},
			[]string{"queue_name"},
		),
		DLQTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dispatcher_dlq_total",
				Help: "Total count of dead letter queue operations",
			},
			[]string{"tenant_id", "event_type", "action"},
		),
		ActiveWorkers: factory.NewGauge(
			prometheus.GaugeOpts{
				Name: "dispatcher_active_workers",
				Help: "Number of currently active worker goroutines",
			},
		),
		RateLimitedTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dispatcher_rate_limited_total",
				Help: "Total number of deliveries paused/delayed due to endpoint rate limits",
			},
			[]string{"endpoint_id"},
		),
	}
}
