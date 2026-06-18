// Package kafka provides Redpanda (Kafka-API) producer and consumer wrappers.
package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer publishes messages to a Redpanda topic.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer creates a new Producer connected to the given brokers and topic.
func NewProducer(brokers []string, topic string) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchTimeout: 10 * time.Millisecond,
		Async:        false,
		RequiredAcks: kafka.RequireAll,
	}
	return &Producer{writer: w}
}

// Publish sends a message with the given key and value bytes to Redpanda.
func (p *Producer) Publish(ctx context.Context, key, value []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
	})
}

// Close shuts down the producer, flushing any buffered messages.
func (p *Producer) Close() error {
	return p.writer.Close()
}
