// Command consumer reads Avro-encoded ChangeEvents from Redpanda with metrics.
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
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	metrics.Up.Set(1)

	brokers := getEnv("KAFKA_BROKERS", "localhost:19092")
	topic := getEnv("KAFKA_TOPIC", "recentchanges")
	groupID := getEnv("KAFKA_GROUP", "meridian-consumer")
	schemaRegistryURL := getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
	metricsAddr := getEnv("METRICS_ADDR", ":8082")

	metricsSrv := metrics.ServeMetrics(metricsAddr)

	consumer := kafka.NewConsumer([]string{brokers}, groupID, topic)
	defer consumer.Close()

	sc := schema.NewClient(schemaRegistryURL)
	cc := codec.NewCodec(sc, "")

	var consumed, errCount atomic.Int64
	startTime := time.Now()
	lastLog := time.Now()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigCh; log.Printf("shutting down (consumed=%d errors=%d)", consumed.Load(), errCount.Load()); metrics.Up.Set(0); metricsSrv.Close(); cancel() }()

	fmt.Printf("consumer: subscribing to %s/%s as group %s\n", brokers, topic, groupID)

	for {
		msg, err := consumer.Read(ctx)
		if err != nil { log.Printf("read error: %v", err); continue }
		_, err = cc.Decode(msg.Value)
		if err != nil { errCount.Add(1); continue }
		if err := consumer.Commit(ctx, msg); err != nil { log.Printf("commit error: %v", err) }
		metrics.EventsConsumed.WithLabelValues("consumer", topic).Inc()
		c := consumed.Add(1)
		if time.Since(lastLog) > 10*time.Second {
			log.Printf("consumed %d events (%.0f/s)", c, float64(c)/time.Since(startTime).Seconds())
			lastLog = time.Now()
		}
	}
}

func getEnv(key, fallback string) string { if v := os.Getenv(key); v != "" { return v }; return fallback }
