// Command amplifier generates high-volume synthetic ChangeEvents for load testing.
package main

import (
	"context"
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
	"github.com/mathew/meridian-stream/internal/almanac/kafka"
	"github.com/mathew/meridian-stream/internal/almanac/metrics"
)

func main() {
	var (
		rate        = flag.Int("rate", 1000, "Events/s")
		duration    = flag.Duration("duration", 30*time.Second, "Run duration")
		concurrency = flag.Int("concurrency", 10, "Concurrent producers")
	)
	flag.Parse()

	brokers := getEnv("KAFKA_BROKERS", "localhost:19092")
	topic := getEnv("KAFKA_TOPIC", "recentchanges")
	metricsAddr := getEnv("METRICS_ADDR", ":8084")

	metricsSrv := metrics.ServeMetrics(metricsAddr)
	defer metricsSrv.Close()
	metrics.Up.Set(1)

	producers := make([]*kafka.Producer, *concurrency)
	for i := 0; i < *concurrency; i++ { producers[i] = kafka.NewProducer([]string{brokers}, topic); defer producers[i].Close() }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigCh; cancel() }()

	var published, errors atomic.Int64
	startTime := time.Now()
	interval := time.Second / time.Duration(*rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var idSeq atomic.Int64
	idSeq.Store(1000000)

	log.Printf("amplifier: %d events/s for %s (%d producers)", *rate, *duration, *concurrency)

	for {
		select {
		case <-ctx.Done(): break
		case <-time.After(*duration): break
		case <-ticker.C:
			id := idSeq.Add(1)
			evt := almanac.ChangeEvent{ID: id, Type: "edit", Title: fmt.Sprintf("LoadTest_Page_%d", id%1000), TitleURL: fmt.Sprintf("LoadTest_Page_%d", id%1000), User: fmt.Sprintf("loadtester_%d", rand.Intn(100)), Timestamp: time.Now().Unix(), ParsedTimestamp: time.Now(), Wiki: "testwiki"}
			pid := int(published.Load()) % *concurrency
			go func(pidx int, e almanac.ChangeEvent) {
				start := time.Now()
				if err := producers[pidx].Publish(ctx, []byte(e.Key()), []byte(fmt.Sprintf(`{"id":%d}`, e.ID))); err != nil { errors.Add(1); return }
				metrics.PublishDuration.Observe(time.Since(start).Seconds())
				metrics.EventsPublished.WithLabelValues("amplifier", topic).Inc()
				p := published.Add(1)
				if p%1000 == 0 { log.Printf("amplifier: %d events (%.0f/s, errors=%d)", p, float64(p)/time.Since(startTime).Seconds(), errors.Load()) }
			}(pid, evt)
		}
	}
	elapsed := time.Since(startTime).Seconds()
	fmt.Printf("\namplifier: %d events in %.0fs (%.0f/s, errors=%d)\n", published.Load(), elapsed, float64(published.Load())/elapsed, errors.Load())
}

func getEnv(k, f string) string { if v := os.Getenv(k); v != "" { return v }; return f }
