// Package kafka provides Redpanda (Kafka-API) producer and consumer wrappers.
package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

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

func (p *Producer) Publish(ctx context.Context, key, value []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{Key: key, Value: value})
}

func (p *Producer) Close() error { return p.writer.Close() }
