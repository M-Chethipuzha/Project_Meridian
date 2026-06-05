// Command dlq-replay reads DLQ messages and re-publishes them to original topics.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "Print without publishing")
	replay := flag.Bool("replay", false, "Re-publish to original topic")
	flag.Parse()

	brokers := getEnv("KAFKA_BROKERS", "localhost:19092")
	dlqTopic := getEnv("DLQ_TOPIC", "recentchanges-dlq")
	groupID := getEnv("KAFKA_GROUP", "meridian-dlq-replay")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigCh; cancel() }()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokers}, GroupID: groupID, Topic: dlqTopic, MinBytes: 1, MaxBytes: 10e6, StartOffset: kafka.LastOffset,
	})
	defer reader.Close()

	var writer *kafka.Writer
	if *replay && !*dryRun {
		writer = &kafka.Writer{Addr: kafka.TCP(brokers), BatchTimeout: 10 * time.Millisecond, Async: true}
		defer writer.Close()
	}

	var count, replayCount int
	fmt.Printf("dlq-replay: reading %s (replay=%v, dry-run=%v)\n", dlqTopic, *replay, *dryRun)
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil { log.Printf("read error: %v", err); continue }
		count++
		fmt.Printf("--- DLQ %d --- original: %s, error: %s\n", count, getHeader(msg.Headers, "x-original-topic"), getHeader(msg.Headers, "x-error-message"))
		if *replay && *dryRun { fmt.Printf("  [dry-run] would replay to %s\n", getHeader(msg.Headers, "x-original-topic")) }
		if *replay && !*dryRun {
			origTopic := getHeader(msg.Headers, "x-original-topic")
			if origTopic != "" { writer.WriteMessages(ctx, kafka.Message{Topic: origTopic, Key: msg.Key, Value: msg.Value}); replayCount++ }
		}
	}
}

func getEnv(k, f string) string { if v := os.Getenv(k); v != "" { return v }; return f }
func getHeader(h []kafka.Header, k string) string { for _, v := range h { if v.Key == k { return string(v.Value) } }; return "" }
