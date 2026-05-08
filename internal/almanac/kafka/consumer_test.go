package kafka

import (
	"context"
	"testing"
	"time"
)

func TestConsumerRequiresBroker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	consumer := NewConsumer([]string{"127.0.0.1:19092"}, "test-group", "test-topic")
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := consumer.Read(ctx)
	if err == nil {
		t.Errorf("expected error without broker, got nil")
	} else {
		t.Logf("expected connection error: %v", err)
	}
}
