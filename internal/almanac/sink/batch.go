package sink

import (
	"log"
	"sync"
	"time"

	"github.com/mathew/meridian-stream/internal/almanac"
)

// BatchConfig defines the rolling flush policy for the Batcher.
// MaxPending sets a limit on buffered-but-not-yet-flushed events (0 = unlimited).
type BatchConfig struct {
	MaxRows       int
	MaxBytes      int
	FlushInterval time.Duration
	MaxPending    int
}

// Batcher accumulates ChangeEvents and flushes them to a FileWriter
// when either MaxRows, MaxBytes, or FlushInterval is exceeded.
type Batcher struct {
	cfg       BatchConfig
	fw        FileWriter
	mu        sync.Mutex
	buf       []parquetRow
	byteLen   int
	flushCh   chan struct{}
	done      chan struct{}
	lastFlush time.Time
	pendingCh chan struct{}
}

// NewBatcher creates a new Batcher that flushes to the given FileWriter.
func NewBatcher(cfg BatchConfig, fw FileWriter) *Batcher {
	var pendingCh chan struct{}
	if cfg.MaxPending > 0 {
		pendingCh = make(chan struct{}, cfg.MaxPending)
	}
	b := &Batcher{
		cfg:       cfg,
		fw:        fw,
		buf:       make([]parquetRow, 0, cfg.MaxRows),
		flushCh:   make(chan struct{}, 1),
		done:      make(chan struct{}),
		lastFlush: time.Now(),
		pendingCh: pendingCh,
	}
	go b.flushLoop()
	return b
}

// Write adds an event to the buffer and triggers a flush if thresholds are exceeded.
// If MaxPending > 0, Write blocks when the pending buffer is full until Flush drains it.
func (b *Batcher) Write(evt *almanac.ChangeEvent) error {
	row := toRow(evt)

	b.mu.Lock()
	b.buf = append(b.buf, row)
	b.byteLen += estimateSize(&row)
	shouldFlush := len(b.buf) >= b.cfg.MaxRows || b.byteLen >= b.cfg.MaxBytes
	b.mu.Unlock()

	if b.cfg.MaxPending > 0 {
		b.pendingCh <- struct{}{}
	}

	if shouldFlush {
		select {
		case b.flushCh <- struct{}{}:
		default:
		}
	}
	return nil
}

// Flush writes the current buffer to the FileWriter and drains the pending
// channel by the number of flushed rows.
func (b *Batcher) Flush() error {
	b.mu.Lock()
	if len(b.buf) == 0 {
		b.mu.Unlock()
		return nil
	}
	batch := b.buf
	n := len(batch)
	t := time.Now()
	b.buf = make([]parquetRow, 0, b.cfg.MaxRows)
	b.byteLen = 0
	b.lastFlush = t
	b.mu.Unlock()

	if b.cfg.MaxPending > 0 {
		for i := 0; i < n; i++ {
			<-b.pendingCh
		}
	}

	if err := b.fw.WriteFile(batch, t); err != nil {
		log.Printf("batcher: flush error: %v", err)
		return err
	}
	return nil
}

func (b *Batcher) flushLoop() {
	ticker := time.NewTicker(b.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.done:
			return
		case <-ticker.C:
			b.mu.Lock()
			elapsed := time.Since(b.lastFlush)
			shouldFlush := len(b.buf) > 0 && elapsed >= b.cfg.FlushInterval
			b.mu.Unlock()
			if shouldFlush {
				_ = b.Flush()
			}
		case <-b.flushCh:
			_ = b.Flush()
		}
	}
}

// Close flushes remaining data and stops the background goroutine.
func (b *Batcher) Close() error {
	close(b.done)
	return b.Flush()
}

func estimateSize(r *parquetRow) int {
	return len(r.Type) + len(r.Title) + len(r.TitleURL) + len(r.Comment) +
		len(r.User) + len(r.ServerURL) + len(r.ServerName) + len(r.ServerScriptURL) +
		len(r.Wiki) + 64
}
