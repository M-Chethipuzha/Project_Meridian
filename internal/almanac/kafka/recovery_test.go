package kafka

import (
	"context"
	"testing"
	"time"
)

func TestWaitForReadyContextCancel(t *testing.T) {
	hc := NewHealthChecker(
		[]string{"localhost:1"},
		"http://localhost:1",
		"localhost:1", "", "", "test", false,
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := hc.WaitForReady(ctx, 5, 10*time.Millisecond)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestWaitForReadyMaxRetries(t *testing.T) {
	hc := NewHealthChecker(
		[]string{"localhost:1"},
		"http://localhost:1",
		"localhost:1", "", "", "test", false,
	)

	start := time.Now()
	err := hc.WaitForReady(context.Background(), 2, 5*time.Millisecond)
	dur := time.Since(start)

	if err == nil {
		t.Fatal("expected error for unreachable services, got nil")
	}
	if dur > 2*time.Second {
		t.Errorf("WaitForReady took too long: %v", dur)
	}
}
