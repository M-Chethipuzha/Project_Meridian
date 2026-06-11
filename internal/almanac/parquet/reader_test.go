package parquet

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadNonExistent(t *testing.T) {
	_, err := ReadFile("/tmp/nonexistent.parquet")
	if err == nil { t.Fatal("expected error") }
}
