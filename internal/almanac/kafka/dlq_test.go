package kafka

import (
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestShouldDLQ(t *testing.T) {
	tests := []struct {
		count      int
		maxRetries int
		want       bool
	}{
		{0, 3, false},
		{2, 3, false},
		{3, 3, true},
		{5, 3, true},
		{0, 5, false},
		{5, 5, true},
	}

	for _, tt := range tests {
		got := ShouldDLQ(tt.count, tt.maxRetries)
		if got != tt.want {
			t.Errorf("ShouldDLQ(%d, %d) = %v, want %v", tt.count, tt.maxRetries, got, tt.want)
		}
	}
}

func TestExtractErrorHeaders(t *testing.T) {
	headers := []kafka.Header{
		{Key: "x-error-type", Value: []byte("decode_error")},
		{Key: "x-error-message", Value: []byte("parse failed")},
		{Key: "x-original-topic", Value: []byte("recentchanges")},
	}

	got := ExtractErrorFromHeaders(headers)
	if got != "decode_error" {
		t.Errorf("expected decode_error, got %q", got)
	}
}

func TestExtractErrorHeadersMissing(t *testing.T) {
	headers := []kafka.Header{
		{Key: "x-original-topic", Value: []byte("test")},
	}

	got := ExtractErrorFromHeaders(headers)
	if got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestErrorType(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{errors.New("decode error: short message"), "decode_error"},
		{errors.New("unmarshal failed"), "decode_error"},
		{errors.New("parse schema"), "decode_error"},
		{errors.New("write to sink failed"), "sink_error"},
		{errors.New("flush error: connection reset"), "sink_error"},
		{errors.New("connection refused"), "unknown"},
	}

	for _, tt := range tests {
		got := ErrorType(tt.err)
		if got != tt.want {
			t.Errorf("ErrorType(%q) = %q, want %q", tt.err.Error(), got, tt.want)
		}
	}
}

func TestDLQRoutingScenario(t *testing.T) {
	rs := NewRetryState()
	msg := kafka.Message{Topic: "recentchanges", Partition: 0, Offset: 42}

	// Simulate retries.
	for i := 0; i < 3; i++ {
		rs.Next(msg)
	}

	if !ShouldDLQ(rs.Next(msg), 3) {
		t.Error("expected ShouldDLQ to return true after 3 retries")
	}

	rs.Reset(msg)

	if got := rs.Next(msg); got != 0 {
		t.Errorf("after reset retry should start at 0, got %d", got)
	}
}

func TestDLQWriterClose(t *testing.T) {
	w := NewDLQWriter([]string{"localhost:9092"}, "test-dlq")
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
