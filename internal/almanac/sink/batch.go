package sink

import (
	"fmt"
	"sync"
	"time"

	"github.com/mathew/meridian-stream/internal/almanac"
)

type BatchConfig struct {
	MaxRows       int
	MaxBytes      int
	FlushInterval time.Duration
	MaxPending    int
}

type Batcher struct {
	cfg    BatchConfig
	writer FileWriter
	rows   []parquetRow
	bytes  int
	mu     sync.Mutex
	pending chan struct{}
	done    chan struct{}
	timer   *time.Timer
}

func NewBatcher(cfg BatchConfig, writer FileWriter) *Batcher {
	b := &Batcher{
		cfg: cfg, writer: writer,
		pending: make(chan struct{}, cfg.MaxPending),
		done:    make(chan struct{}),
		timer:   time.NewTimer(cfg.FlushInterval),
	}
	go b.flushLoop()
	return b
}

func (b *Batcher) Write(evt *almanac.ChangeEvent) error {
	b.mu.Lock()
	if b.cfg.MaxPending > 0 {
		select {
		case b.pending <- struct{}{}:
		default:
			b.mu.Unlock()
			return fmt.Errorf("backpressure limit reached (%d pending)", b.cfg.MaxPending)
		}
	}
	b.rows = append(b.rows, toRow(evt))
	b.bytes += 256
	shouldFlush := len(b.rows) >= b.cfg.MaxRows || b.bytes >= b.cfg.MaxBytes
	b.mu.Unlock()
	if shouldFlush { return b.Flush() }
	return nil
}

func (b *Batcher) Flush() error {
	b.mu.Lock()
	rows := b.rows
	b.rows = nil
	b.bytes = 0
	b.mu.Unlock()
	if len(rows) == 0 { return nil }
	return b.writer.WriteFile(rows, time.Now())
}

func (b *Batcher) Close() error {
	b.timer.Stop()
	close(b.done)
	return b.Flush()
}

func (b *Batcher) flushLoop() {
	for {
		select {
		case <-b.done: return
		case <-b.timer.C: b.Flush(); b.timer.Reset(b.cfg.FlushInterval)
		}
	}
}
