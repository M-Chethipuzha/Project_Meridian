package codec

import (
	"testing"
	"github.com/mathew/meridian-stream/internal/almanac"
)

func TestEncode(t *testing.T) {
	// Test requires a running Schema Registry; skip in short mode
	t.Skip("integration test: requires Schema Registry")
}

func TestCodecRoundTrip(t *testing.T) {
	t.Skip("integration test: requires Schema Registry")
}
