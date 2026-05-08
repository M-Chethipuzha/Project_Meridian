// Command dlq-replay reads messages from a dead-letter queue topic and can
// re-publish them back to the original topic. Useful for recovery after
// resolving the root cause of failures.
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
	dryRun := flag.Bool("dry-run", false, "Print what would be replayed without publishing")
	replay := flag.Bool("replay", false, "Re-publish DLQ messages back to the original topic")
	flag.Parse()

	brokers := getEnv("KAFKA_BROKERS", "localhost:19092")
	dlqTopic := getEnv("DLQ_TOPIC", "recentchanges-dlq")
	groupID := getEnv("KAFKA_GROUP", "meridian-dlq-replay")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down")
		cancel()
	}()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{brokers},
		GroupID:     groupID,
		Topic:       dlqTopic,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.LastOffset,
	})
	defer reader.Close()

	var writer *kafka.Writer
	if *replay && !*dryRun {
		writer = &kafka.Writer{
			Addr:         kafka.TCP(brokers),
			BatchTimeout: 10 * time.Millisecond,
			Async:        true,
		}
		defer writer.Close()
	}

	var count int
	var replayCount int

	fmt.Printf("dlq-replay: reading from %s (group=%s, replay=%v, dry-run=%v)\n",
		dlqTopic, groupID, *replay, *dryRun)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				goto done
			default:
			}
			log.Printf("read error: %v", err)
			continue
		}

		count++

		originalTopic := getHeader(msg.Headers, "x-original-topic")
		errorType := getHeader(msg.Headers, "x-error-type")
		errorMsg := getHeader(msg.Headers, "x-error-message")

		fmt.Printf("--- DLQ Message %d ---\n", count)
		fmt.Printf("  original topic: %s\n", originalTopic)
		fmt.Printf("  error type:     %s\n", errorType)
		fmt.Printf("  error message:  %s\n", errorMsg)
		fmt.Printf("  key:            %s\n", string(msg.Key))
		fmt.Printf("  offset:         %d\n", msg.Offset)
		fmt.Println()

		if *replay && originalTopic != "" {
			replayCount++
			if *dryRun {
				fmt.Printf("  [dry-run] would replay to %s\n", originalTopic)
			} else {
				replayMsg := kafka.Message{
					Topic: originalTopic,
					Key:   msg.Key,
					Value: msg.Value,
				}
				if err := writer.WriteMessages(ctx, replayMsg); err != nil {
					log.Printf("replay error (msg %d): %v", count, err)
				} else {
					fmt.Printf("  replayed to %s\n", originalTopic)
				}
			}
		}
	}

done:
	fmt.Printf("dlq-replay: processed %d messages, replayed %d\n", count, replayCount)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getHeader(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
