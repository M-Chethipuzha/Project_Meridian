// Package metrics provides Prometheus instrumentation for Meridian Stream services.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "meridian"

var (
	EventsPublished = promauto.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Name: "events_published_total", Help: "Total events published."}, []string{"service", "topic"})
	EventsConsumed  = promauto.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Name: "events_consumed_total", Help: "Total events consumed."}, []string{"service", "topic"})
	EventsFailed    = promauto.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Name: "events_failed_total", Help: "Total events failed."}, []string{"service", "topic", "type"})
	PublishDuration = promauto.NewHistogram(prometheus.HistogramOpts{Namespace: namespace, Name: "publish_duration_seconds", Help: "Publish latency.", Buckets: prometheus.DefBuckets})
	ConsumeDuration = promauto.NewHistogram(prometheus.HistogramOpts{Namespace: namespace, Name: "consume_duration_seconds", Help: "Consume latency.", Buckets: prometheus.DefBuckets})
	ConsumerLag     = promauto.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: "consumer_lag_messages", Help: "Consumer lag in messages."})
	BatchWriteDuration = promauto.NewHistogram(prometheus.HistogramOpts{Namespace: namespace, Name: "batch_write_duration_seconds", Help: "Batch flush latency.", Buckets: prometheus.DefBuckets})
	BatchSize       = promauto.NewHistogram(prometheus.HistogramOpts{Namespace: namespace, Name: "batch_size_events", Help: "Events per batch.", Buckets: prometheus.LinearBuckets(10, 100, 20)})
	Up              = promauto.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: "up", Help: "Service up indicator."})
)
