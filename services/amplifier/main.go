// Command amplifier generates high-volume synthetic ChangeEvents for load
// testing the pipeline. Takes a seed event pattern and amplifies it by
// varying key fields (ID, timestamp, title).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
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
)

func main() {
	var (
		rate        = flag.Int("rate", 1000, "Events per second to generate")
		duration    = flag.Duration("duration", 30*time.Second, "How long to run")
		seedJSON    = flag.String("seed", "", "Seed event JSON file path (optional)")
		concurrency = flag.Int("concurrency", 10, "Number of concurrent producers")
	)
	flag.Parse()

	brokers := getEnv("KAFKA_BROKERS", "localhost:19092")
	topic := getEnv("KAFKA_TOPIC", "recentchanges")
	schemaRegistryURL := getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
	metricsAddr := getEnv("METRICS_ADDR", ":8084")

	metricsSrv := metrics.ServeMetrics(metricsAddr)
	defer metricsSrv.Close()
	metrics.Up.Set(1)

	sc := schema.NewClient(schemaRegistryURL)
	cc := codec.NewCodec(sc, avroSchema)
	if err := cc.Register(topic + "-value"); err != nil {
		log.Fatalf("schema registration: %v", err)
	}

	// Load or create seed event.
	var baseEvent almanac.ChangeEvent
	if *seedJSON != "" {
		data, err := os.ReadFile(*seedJSON)
		if err != nil {
			log.Fatalf("read seed file: %v", err)
		}
		if err := json.Unmarshal(data, &baseEvent); err != nil {
			log.Fatalf("parse seed event: %v", err)
		}
	} else {
		baseEvent = almanac.ChangeEvent{
			Type:            "edit",
			Namespace:       0,
			Title:           "LoadTest_Page",
			TitleURL:        "LoadTest_Page",
			Comment:         "amplifier load test",
			User:            "loadtester",
			Bot:             false,
			ServerURL:       "https://example.org",
			ServerName:      "Example Wiki",
			ServerScriptURL: "https://example.org/w",
			Wiki:            "testwiki",
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down")
		cancel()
	}()

	// Create concurrent producers.
	producers := make([]*kafka.Producer, *concurrency)
	for i := 0; i < *concurrency; i++ {
		producers[i] = kafka.NewProducer([]string{brokers}, topic)
		defer producers[i].Close()
	}

	interval := time.Second / time.Duration(*rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var published atomic.Int64
	var errors atomic.Int64
	startTime := time.Now()
	timeout := time.After(*duration)

	log.Printf("amplifier: %d events/s for %s (%d producers)", *rate, *duration, *concurrency)

	var idSeq atomic.Int64
	idSeq.Store(1000000)

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-timeout:
			break loop
		case <-ticker.C:
			pid := int(published.Load()) % *concurrency
			id := idSeq.Add(1)

			evt := baseEvent
			evt.ID = id
			evt.Timestamp = time.Now().Unix()
			evt.ParsedTimestamp = time.Now()
			evt.Title = fmt.Sprintf("LoadTest_Page_%d", id%1000)
			evt.TitleURL = evt.Title
			evt.User = fmt.Sprintf("loadtester_%d", rand.Intn(100))

			go func(producerIdx int, e almanac.ChangeEvent) {
				pubStart := time.Now()
				data, err := cc.Encode(&e)
				if err != nil {
					errors.Add(1)
					metrics.EventsFailed.WithLabelValues("amplifier", topic, "encode_error").Inc()
					return
				}
				if err := producers[producerIdx].Publish(ctx, []byte(e.Key()), data); err != nil {
					errors.Add(1)
					metrics.EventsFailed.WithLabelValues("amplifier", topic, "publish_error").Inc()
					return
				}
				metrics.PublishDuration.Observe(time.Since(pubStart).Seconds())
				metrics.EventsPublished.WithLabelValues("amplifier", topic).Inc()

				p := published.Add(1)
				if p%1000 == 0 {
					elapsed := time.Since(startTime).Seconds()
					rate := float64(p) / elapsed
					log.Printf("amplifier: published %d events (%.0f/s, errors=%d)", p, rate, errors.Load())
				}
			}(pid, evt)
		}
	}

	elapsed := time.Since(startTime).Seconds()
	count := published.Load()
	errCount := errors.Load()
	var actualRate float64
	if elapsed > 0 {
		actualRate = float64(count) / elapsed
	}
	fmt.Printf("\namplifier complete: %d events in %.0fs (%.0f/s target, %.0f/s actual, %d errors)\n",
		count, elapsed, float64(*rate), actualRate, errCount)
}

var avroSchema = `{
	"type": "record",
	"name": "ChangeEvent",
	"namespace": "meridian",
	"fields": [
		{"name": "id", "type": "long"},
		{"name": "type", "type": "string"},
		{"name": "namespace", "type": "int"},
		{"name": "title", "type": "string"},
		{"name": "title_url", "type": "string"},
		{"name": "comment", "type": "string"},
		{"name": "timestamp", "type": "long"},
		{"name": "user", "type": "string"},
		{"name": "bot", "type": "boolean"},
		{"name": "server_url", "type": "string"},
		{"name": "server_name", "type": "string"},
		{"name": "server_script_url", "type": "string"},
		{"name": "wiki", "type": "string"},
		{"name": "parsed_timestamp", "type": "long"}
	]
}`

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
