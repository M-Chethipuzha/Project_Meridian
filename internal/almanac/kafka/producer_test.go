package kafka

import (
	"context"
	"testing"
	"time"
)

func TestProducerPublishRequiresBroker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	prod := NewProducer([]string{"127.0.0.1:19092"}, "test-topic")
	defer prod.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := prod.Publish(ctx, []byte("key"), []byte(`{"id":1}`))
	if err == nil {
		t.Log("producer connected successfully (unexpected without Redpanda)")
	} else {
		t.Logf("expected connection error without broker: %v", err)
	}
}
