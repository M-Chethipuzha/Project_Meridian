// Package metrics provides Prometheus instrumention shared across
// Meridian Stream services.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "meridian"

// shared metric labels
const (
	labelService = "service"
	labelTopic   = "topic"
	labelType    = "type" // error type classification
)

var (
	// EventsPublished counts successfully published events per service/topic.
	EventsPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "events_published_total",
		Help:      "Total number of events successfully published.",
	}, []string{labelService, labelTopic})

	// EventsConsumed counts successfully consumed and committed events.
	EventsConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "events_consumed_total",
		Help:      "Total number of events successfully consumed and committed.",
	}, []string{labelService, labelTopic})

	// EventsFailed counts events that failed at any stage of the pipeline.
	EventsFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "events_failed_total",
		Help:      "Total number of events that failed processing.",
	}, []string{labelService, labelTopic, labelType})

	// PublishDuration tracks the time taken to encode and publish a single event.
	PublishDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "publish_duration_seconds",
		Help:      "Time spent encoding and publishing a single event.",
		Buckets:   prometheus.DefBuckets,
	})

	// ConsumeDuration tracks the time to decode, write, and commit a single event.
	ConsumeDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "consume_duration_seconds",
		Help:      "Time spent decoding, writing sink, and committing a single event.",
		Buckets:   prometheus.DefBuckets,
	})

	// ConsumerLag reports the consumer group lag in number of messages.
	ConsumerLag = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "consumer_lag_messages",
		Help:      "Current consumer group lag in number of messages (producer offset - consumer offset).",
	})

	// BatchWriteDuration tracks the time to flush a batch to the Parquet sink.
	BatchWriteDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "batch_write_duration_seconds",
		Help:      "Time spent flushing a batch of events to the Parquet sink.",
		Buckets:   prometheus.DefBuckets,
	})

	// BatchSize tracks the number of events per flushed batch.
	BatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "batch_size_events",
		Help:      "Number of events per flushed batch.",
		Buckets:   prometheus.LinearBuckets(10, 100, 20),
	})

	// Up is a standard process health metric.
	Up = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "up",
		Help:      "1 if the service is running, 0 otherwise.",
	})
)
