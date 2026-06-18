package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// Consumer reads messages from a Redpanda topic as part of a consumer group.
type Consumer struct {
	reader *kafka.Reader
	topic  string
	group  string
}

// NewConsumer creates a new Consumer subscribed to the given topic as part of a group.
func NewConsumer(brokers []string, groupID, topic string) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     groupID,
		Topic:       topic,
		MinBytes:    1,
		MaxBytes:    10e6, // 10 MB
		StartOffset: kafka.LastOffset,
	})
	return &Consumer{reader: r, topic: topic, group: groupID}
}

// Read blocks until a message is received and returns the raw kafka.Message.
// The caller must call Commit on the message to mark it as processed (at-least-once).
func (c *Consumer) Read(ctx context.Context) (kafka.Message, error) {
	return c.reader.ReadMessage(ctx)
}

// Commit marks a message as processed, advancing the consumer group offset.
func (c *Consumer) Commit(ctx context.Context, msg kafka.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}

// Lag returns the consumer's approximate lag in messages as tracked by the
// kafka-go reader internals (latest broker offset - last committed offset).
func (c *Consumer) Lag(ctx context.Context) (int64, error) {
	stats := c.reader.Stats()
	return stats.Lag, nil
}

// Close shuts down the consumer.
func (c *Consumer) Close() error {
	return c.reader.Close()
}

// Topic returns the consumer's topic.
func (c *Consumer) Topic() string { return c.topic }

// Group returns the consumer's group ID.
func (c *Consumer) Group() string { return c.group }
