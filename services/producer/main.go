// Command producer connects to Wikimedia EventStreams SSE and publishes to Redpanda.
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
	"github.com/mathew/meridian-stream/internal/almanac/kafka"
	"github.com/mathew/meridian-stream/internal/almanac/sse"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	brokers := getEnv("KAFKA_BROKERS", "localhost:19092")
	topic := getEnv("KAFKA_TOPIC", "recentchanges")

	prod := kafka.NewProducer([]string{brokers}, topic)
	defer prod.Close()

	var published atomic.Int64
	startTime := time.Now()

	onError := func(err error) { log.Printf("sse error: %v", err) }
	onEvent := func(evt almanac.ChangeEvent) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := prod.Publish(ctx, []byte(evt.Key()), []byte("")); err != nil {
			log.Printf("publish error: %v", err)
			return
		}
		p := published.Add(1)
		if p%100 == 0 {
			log.Printf("published %d events (%.0f/s)", p, float64(p)/time.Since(startTime).Seconds())
		}
	}

	reader := sse.NewReader(onEvent, onError)
	reader.Start()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("shutting down (published=%d)", published.Load())
	reader.Stop()
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" { return v }
	return fallback
}
