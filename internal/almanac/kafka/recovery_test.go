package kafka

import (
	"context"
	"testing"
	"time"
)

func TestHealthCheckerWaitForReadyTimeout(t *testing.T) {
	hc := NewHealthChecker([]string{"localhost:1"}, "http://localhost:1", "localhost:1", "", "", "test", false)
	err := hc.WaitForReady(context.Background(), 1, 100*time.Millisecond)
	if err == nil { t.Fatal("expected timeout error") }
}
