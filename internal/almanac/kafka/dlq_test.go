package kafka

import (
	"errors"
	"testing"
)

func TestClassifyError(t *testing.T) {
	if classifyError(errors.New("decode error")) != "decode_error" { t.Fatal("expected decode_error") }
	if classifyError(errors.New("sink write failed")) != "sink_error" { t.Fatal("expected sink_error") }
	if classifyError(errors.New("network timeout")) != "unknown" { t.Fatal("expected unknown") }
}
