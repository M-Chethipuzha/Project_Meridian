// Command consumer reads Avro-encoded ChangeEvents from a Redpanda topic,
// decodes them via the Schema Registry, and writes them as time-partitioned
// Parquet files to MinIO. Supports dead-letter queue, backpressure, graceful
// shutdown, and startup health checks.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mathew/meridian-stream/internal/almanac/codec"
	"github.com/mathew/meridian-stream/internal/almanac/kafka"
	"github.com/mathew/meridian-stream/internal/almanac/metrics"
	"github.com/mathew/meridian-stream/internal/almanac/schema"
	"github.com/mathew/meridian-stream/internal/almanac/sink"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("consumer starting")
	metrics.Up.Set(1)

	brokers := getEnv("KAFKA_BROKERS", "localhost:19092")
	topic := getEnv("KAFKA_TOPIC", "recentchanges")
	groupID := getEnv("KAFKA_GROUP", "meridian-consumer")
	schemaRegistryURL := getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
	minioEndpoint := getEnv("MINIO_ENDPOINT", "localhost:9000")
	minioBucket := getEnv("MINIO_BUCKET", "events")
	minioAccess := getEnv("MINIO_ACCESS_KEY", "minioadmin")
	minioSecret := getEnv("MINIO_SECRET_KEY", "minioadmin")
	metricsAddr := getEnv("METRICS_ADDR", ":8082")
	lagInterval := getEnv("LAG_INTERVAL", "30s")
	dlqTopic := getEnv("DLQ_TOPIC", topic+"-dlq")
	maxRetries := getEnvInt("MAX_RETRIES", 3)
	backpressureLimit := getEnvInt("BACKPRESSURE_LIMIT", 10000)
	startupInterval := getEnvDuration("STARTUP_RETRY_INTERVAL", 5*time.Second)
	startupMaxRetries := getEnvInt("STARTUP_MAX_RETRIES", 6)

	metricsSrv := metrics.ServeMetrics(metricsAddr)
	log.Printf("metrics endpoint: http://0.0.0.0%s/metrics", metricsAddr)

	// Startup health checks.
	hc := kafka.NewHealthChecker(
		[]string{brokers},
		schemaRegistryURL,
		minioEndpoint, minioAccess, minioSecret, minioBucket, false,
	)
	startupCtx, startupCancel := context.WithTimeout(context.Background(), time.Duration(startupMaxRetries)*startupInterval)
	if err := hc.WaitForReady(startupCtx, startupMaxRetries, startupInterval); err != nil {
		startupCancel()
		log.Fatalf("startup health check failed: %v", err)
	}
	startupCancel()
	log.Println("startup health checks passed")

	consumer := kafka.NewConsumer([]string{brokers}, groupID, topic)
	defer consumer.Close()

	sc := schema.NewClient(schemaRegistryURL)
	cc := codec.NewCodec(sc, "")

	ps, err := sink.NewParquetSink(sink.ParquetSinkConfig{
		Endpoint:  minioEndpoint,
		Bucket:    minioBucket,
		AccessKey: minioAccess,
		SecretKey: minioSecret,
		UseSSL:    false,
		Region:    "us-east-1",
	})
	if err != nil {
		log.Fatalf("parquet sink: %v", err)
	}
	defer ps.Close()

	batcher := sink.NewBatcher(sink.BatchConfig{
		MaxRows:       1000,
		MaxBytes:      10 * 1024 * 1024,
		FlushInterval: 30 * time.Second,
		MaxPending:    backpressureLimit,
	}, ps)
	defer batcher.Close()

	dlq := kafka.NewDLQWriter([]string{brokers}, dlqTopic)
	defer dlq.Close()

	retryState := kafka.NewRetryState()

	var consumed atomic.Int64
	var dlqd atomic.Int64
	var errCount atomic.Int64
	var lastLog time.Time
	startTime := time.Now()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Printf("shutting down (consumed=%d dlq=%d errors=%d)", consumed.Load(), dlqd.Load(), errCount.Load())
		metrics.Up.Set(0)
		metricsSrv.Close()
		cancel()
	}()

	// Background lag polling.
	lagDur, err := time.ParseDuration(lagInterval)
	if err != nil {
		lagDur = 30 * time.Second
	}
	go pollLag(ctx, consumer, lagDur)

	fmt.Printf("consumer: subscribing to %s/%s as group %s (dlq=%s, backpressure=%d)\n",
		brokers, topic, groupID, dlqTopic, backpressureLimit)

	for {
		select {
		case <-ctx.Done():
			log.Println("consumer loop exiting")
			return
		default:
		}

		consumeStart := time.Now()

		msg, err := consumer.Read(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			errCount.Add(1)
			metrics.EventsFailed.WithLabelValues("consumer", topic, "read_error").Inc()
			log.Printf("read error: %v", err)
			continue
		}

		evt, err := cc.Decode(msg.Value)
		if err != nil {
			errCount.Add(1)
			metrics.EventsFailed.WithLabelValues("consumer", topic, "decode_error").Inc()
			log.Printf("decode error: %v", err)
			retryCount := retryState.Next(msg)
			if kafka.ShouldDLQ(retryCount, maxRetries) {
				if dlqErr := dlq.WriteFailed(msg, err); dlqErr != nil {
					log.Printf("dlq write error: %v", dlqErr)
				} else {
					dlqd.Add(1)
				}
				if commitErr := consumer.Commit(ctx, msg); commitErr != nil {
					log.Printf("dlq commit error: %v", commitErr)
				}
				retryState.Reset(msg)
			}
			continue
		}

		if err := batcher.Write(evt); err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			errCount.Add(1)
			metrics.EventsFailed.WithLabelValues("consumer", topic, "sink_error").Inc()
			log.Printf("sink error: %v", err)
			continue
		}

		if err := consumer.Commit(ctx, msg); err != nil {
			log.Printf("commit error: %v", err)
		}

		retryState.Reset(msg)
		metrics.ConsumeDuration.Observe(time.Since(consumeStart).Seconds())
		metrics.EventsConsumed.WithLabelValues("consumer", topic).Inc()

		c := consumed.Add(1)
		if time.Since(lastLog) > 10*time.Second {
			elapsed := time.Since(startTime).Seconds()
			rate := float64(c) / elapsed
			log.Printf("consumed %d events (%.0f/s, errors=%d, dlq=%d)", c, rate, errCount.Load(), dlqd.Load())
			lastLog = time.Now()
		}
	}
}

func pollLag(ctx context.Context, consumer *kafka.Consumer, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lag, err := consumer.Lag(ctx)
			if err != nil {
				log.Printf("lag poll error: %v", err)
				continue
			}
			metrics.ConsumerLag.Set(float64(lag))
		}
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
