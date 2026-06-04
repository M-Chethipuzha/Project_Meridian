package kafka

import (
	"testing"
	"github.com/segmentio/kafka-go"
)

func TestRetryState(t *testing.T) {
	rs := NewRetryState()
	msg := kafka.Message{Topic: "test", Partition: 0, Offset: 1}
	if n := rs.Next(msg); n != 1 { t.Fatalf("expected 1, got %d", n) }
	if n := rs.Next(msg); n != 2 { t.Fatalf("expected 2, got %d", n) }
	rs.Reset(msg)
	if n := rs.Next(msg); n != 1 { t.Fatalf("expected 1 after reset, got %d", n) }
}

func TestShouldDLQ(t *testing.T) {
	if ShouldDLQ(2, 3) { t.Fatal("should not DLQ before max") }
	if !ShouldDLQ(3, 3) { t.Fatal("should DLQ at max") }
}
