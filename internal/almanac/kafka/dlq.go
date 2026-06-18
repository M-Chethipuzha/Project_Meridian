package kafka

import (
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// DLQWriter publishes failed messages to a dead-letter queue topic.
type DLQWriter struct {
	writer *kafka.Writer
	topic  string
}

// NewDLQWriter creates a DLQWriter that publishes to the given topic.
func NewDLQWriter(brokers []string, topic string) *DLQWriter {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchTimeout: 10 * time.Millisecond,
	}
	return &DLQWriter{writer: w, topic: topic}
}

// WriteFailed publishes a failed message to the DLQ topic with error context
// encoded in the message headers.
func (d *DLQWriter) WriteFailed(msg kafka.Message, err error) error {
	headers := []kafka.Header{
		{Key: "x-error-type", Value: []byte(errorType(err))},
		{Key: "x-error-message", Value: []byte(err.Error())},
		{Key: "x-original-topic", Value: []byte(msg.Topic)},
		{Key: "x-retry-count", Value: []byte("0")},
		{Key: "x-original-timestamp", Value: []byte(time.Now().String())},
	}

	dlqMsg := kafka.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: append(headers, msg.Headers...),
	}

	return d.writer.WriteMessages(nil, dlqMsg)
}

// Close shuts down the underlying writer.
func (d *DLQWriter) Close() error {
	if d.writer != nil {
		return d.writer.Close()
	}
	return nil
}

func errorType(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "decode"), strings.Contains(msg, "unmarshal"), strings.Contains(msg, "parse"):
		return "decode_error"
	case strings.Contains(msg, "write"), strings.Contains(msg, "flush"), strings.Contains(msg, "sink"):
		return "sink_error"
	default:
		return "unknown"
	}
}

// ShouldDLQ returns true when retryCount >= maxRetries, indicating the message
// should be sent to the dead-letter queue.
func ShouldDLQ(retryCount, maxRetries int) bool {
	return retryCount >= maxRetries
}

// ExtractErrorFromHeaders reads the x-error-type header value from a set of
// Kafka message headers. Returns empty string if not found.
func ExtractErrorFromHeaders(headers []kafka.Header) string {
	for _, h := range headers {
		if h.Key == "x-error-type" {
			return string(h.Value)
		}
	}
	return ""
}

// ErrorType returns the error classification string for the given error.
// Exported alias for external use.
func ErrorType(err error) string {
	return errorType(err)
}


