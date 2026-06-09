package benchmarks

import (
	"testing"
	"time"
	"github.com/mathew/meridian-stream/internal/almanac"
)

func BenchmarkChangeEventSerialization(b *testing.B) {
	evt := &almanac.ChangeEvent{ID: 1, Type: "edit", Title: "Benchmark", Wiki: "test"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ { _ = evt.Key() }
}

func BenchmarkTimeParse(b *testing.B) {
	for i := 0; i < b.N; i++ { time.Parse(time.RFC3339, "2026-06-08T10:00:00Z") }
}
