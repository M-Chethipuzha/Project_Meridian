package kafka

import (
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type ErrorType int
const (
	ErrorDecode ErrorType = iota
	ErrorSink
	ErrorUnknown
)

func (e ErrorType) String() string { return []string{"decode_error", "sink_error", "unknown"}[e] }

type DLQWriter struct {
	writer *kafka.Writer
	topic  string
}

func NewDLQWriter(brokers []string, topic string) *DLQWriter {
	return &DLQWriter{
		writer: &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, BatchTimeout: 10 * time.Millisecond, Async: true},
		topic:  topic,
	}
}

func (d *DLQWriter) WriteFailed(msg kafka.Message, cause error) error {
	dlqMsg := kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Headers: []kafka.Header{
			{Key: "x-original-topic", Value: []byte(msg.Topic)},
			{Key: "x-original-partition", Value: []byte(fmt.Sprintf("%d", msg.Partition))},
			{Key: "x-original-offset", Value: []byte(fmt.Sprintf("%d", msg.Offset))},
			{Key: "x-error-type", Value: []byte(classifyError(cause))},
			{Key: "x-error-message", Value: []byte(cause.Error())},
			{Key: "x-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	}
	return d.writer.WriteMessages(context.Background(), dlqMsg)
}

func (d *DLQWriter) Close() error { return d.writer.Close() }

func classifyError(err error) string {
	msg := err.Error()
	if strings.Contains(msg, "decode") || strings.Contains(msg, "unmarshal") { return "decode_error" }
	if strings.Contains(msg, "sink") || strings.Contains(msg, "write") { return "sink_error" }
	return "unknown"
}
