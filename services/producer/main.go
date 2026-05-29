// Command producer connects to Wikimedia EventStreams SSE, serializes via Avro, publishes to Redpanda.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mathew/meridian-stream/internal/almanac"
	"github.com/mathew/meridian-stream/internal/almanac/codec"
	"github.com/mathew/meridian-stream/internal/almanac/kafka"
	"github.com/mathew/meridian-stream/internal/almanac/metrics"
	"github.com/mathew/meridian-stream/internal/almanac/schema"
	"github.com/mathew/meridian-stream/internal/almanac/sse"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	metrics.Up.Set(1)

	brokers := getEnv("KAFKA_BROKERS", "localhost:19092")
	topic := getEnv("KAFKA_TOPIC", "recentchanges")
	schemaRegistryURL := getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
	metricsAddr := getEnv("METRICS_ADDR", ":8081")

	metricsSrv := metrics.ServeMetrics(metricsAddr)

	prod := kafka.NewProducer([]string{brokers}, topic)
	defer prod.Close()

	sc := schema.NewClient(schemaRegistryURL)
	cc := codec.NewCodec(sc, avroSchema)
	if err := cc.Register(topic + "-value"); err != nil { log.Fatalf("schema: %v", err) }

	var published, errCount atomic.Int64
	startTime := time.Now()

	onError := func(err error) {
		errCount.Add(1)
		metrics.EventsFailed.WithLabelValues("producer", topic, "sse_error").Inc()
		log.Printf("sse error: %v", err)
	}
	onEvent := func(evt almanac.ChangeEvent) {
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		data, err := cc.Encode(&evt)
		if err != nil { errCount.Add(1); return }
		if err := prod.Publish(ctx, []byte(evt.Key()), data); err != nil { errCount.Add(1); return }
		metrics.PublishDuration.Observe(time.Since(start).Seconds())
		metrics.EventsPublished.WithLabelValues("producer", topic).Inc()
		p := published.Add(1)
		if p%100 == 0 { log.Printf("published %d events (%.0f/s, errors=%d)", p, float64(p)/time.Since(startTime).Seconds(), errCount.Load()) }
	}

	reader := sse.NewReader(onEvent, onError)
	reader.Start()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("shutting down (published=%d errors=%d)", published.Load(), errCount.Load())
	reader.Stop(); metrics.Up.Set(0); metricsSrv.Close()
}

var avroSchema = `{"type":"record","name":"ChangeEvent","namespace":"meridian","fields":[{"name":"id","type":"long"},{"name":"type","type":"string"},{"name":"namespace","type":"int"},{"name":"title","type":"string"},{"name":"title_url","type":"string"},{"name":"comment","type":"string"},{"name":"timestamp","type":"long"},{"name":"user","type":"string"},{"name":"bot","type":"boolean"},{"name":"server_url","type":"string"},{"name":"server_name","type":"string"},{"name":"server_script_url","type":"string"},{"name":"wiki","type":"string"},{"name":"parsed_timestamp","type":"long"}]}`

func getEnv(key, fallback string) string { if v := os.Getenv(key); v != "" { return v }; return fallback }
