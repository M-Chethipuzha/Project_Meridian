package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
	topic  string
	group  string
}

func NewConsumer(brokers []string, groupID, topic string) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     groupID,
		Topic:       topic,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.LastOffset,
	})
	return &Consumer{reader: r, topic: topic, group: groupID}
}

func (c *Consumer) Read(ctx context.Context) (kafka.Message, error) {
	return c.reader.ReadMessage(ctx)
}

func (c *Consumer) Commit(ctx context.Context, msg kafka.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}

func (c *Consumer) Close() error { return c.reader.Close() }
func (c *Consumer) Topic() string { return c.topic }
func (c *Consumer) Group() string { return c.group }
