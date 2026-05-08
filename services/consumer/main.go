// Command consumer reads events from Redpanda and writes to a sink.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mathew/meridian-stream/internal/almanac/kafka"
)

func main() {
	brokers := getEnv("KAFKA_BROKERS", "localhost:19092")
	topic := getEnv("KAFKA_TOPIC", "recentchanges")
	groupID := getEnv("KAFKA_GROUP", "meridian-consumer")

	consumer := kafka.NewConsumer([]string{brokers}, groupID, topic)
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigCh; cancel() }()

	fmt.Printf("consumer: subscribing to %s/%s as group %s\n", brokers, topic, groupID)

	for {
		msg, err := consumer.Read(ctx)
		if err != nil { log.Printf("read error: %v", err); continue }
		fmt.Printf("received: topic=%s partition=%d offset=%d key=%s\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Key))
		consumer.Commit(ctx, msg)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" { return v }
	return fallback
}
