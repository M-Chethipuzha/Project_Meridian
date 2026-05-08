package sse

import (
	"testing"
	"time"
	"log"
)

func TestNewReader(t *testing.T) {
	r := NewReader(nil, func(err error) { log.Print(err) })
	if r == nil { t.Fatal("expected reader") }
	r.Stop()
}

func TestReaderClose(t *testing.T) {
	r := NewReader(nil, nil)
	r.Start()
	time.Sleep(100 * time.Millisecond)
	r.Close()
}
