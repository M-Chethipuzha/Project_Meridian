package kafka

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestRetryStateIncrement(t *testing.T) {
	rs := NewRetryState()
	msg := kafka.Message{Topic: "test", Partition: 0, Offset: 10}

	for i := 0; i < 3; i++ {
		got := rs.Next(msg)
		if got != i {
			t.Errorf("Next() attempt %d = %d, want %d", i, got, i)
		}
	}
}

func TestRetryStateIsolated(t *testing.T) {
	rs := NewRetryState()
	msg1 := kafka.Message{Topic: "t1", Partition: 0, Offset: 1}
	msg2 := kafka.Message{Topic: "t2", Partition: 0, Offset: 1}

	rs.Next(msg1)
	got := rs.Next(msg2)
	if got != 0 {
		t.Errorf("different messages should have independent state; got %d, want 0", got)
	}
}

func TestRetryStateReset(t *testing.T) {
	rs := NewRetryState()
	msg := kafka.Message{Topic: "test", Partition: 0, Offset: 10}

	rs.Next(msg)
	rs.Next(msg)

	if got := rs.Next(msg); got != 2 {
		t.Fatalf("before reset: Next() = %d, want 2", got)
	}

	rs.Reset(msg)

	if got := rs.Next(msg); got != 0 {
		t.Errorf("after reset: Next() = %d, want 0", got)
	}
}


