package sink

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/mathew/meridian-stream/internal/almanac"
)

// mockFileWriter records the number of flush calls and rows.
type mockFileWriter struct {
	flushes atomic.Int64
	rows    atomic.Int64
}

func (m *mockFileWriter) WriteFile(rows []parquetRow, ts time.Time) error {
	m.flushes.Add(1)
	m.rows.Add(int64(len(rows)))
	return nil
}

func makeEvent(id int64) *almanac.ChangeEvent {
	return &almanac.ChangeEvent{
		ID:              id,
		Type:            "edit",
		Namespace:       0,
		Title:           "Test",
		Timestamp:       time.Now().Unix(),
		User:            "tester",
		Bot:             false,
		Wiki:            "testwiki",
		ParsedTimestamp: time.Now(),
	}
}

func TestBatcherFlushesOnMaxRows(t *testing.T) {
	mw := &mockFileWriter{}
	b := NewBatcher(BatchConfig{
		MaxRows:       10,
		MaxBytes:      10 * 1024 * 1024,
		FlushInterval: time.Hour,
	}, mw)

	for i := 0; i < 35; i++ {
		if err := b.Write(makeEvent(int64(i))); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	// Flush remaining rows
	b.Close()

	r := mw.rows.Load()
	if r != 35 {
		t.Errorf("expected 35 rows flushed, got %d", r)
	}
	// At least one flush must have occurred (could be coalesced)
	f := mw.flushes.Load()
	if f < 1 {
		t.Errorf("expected at least 1 flush, got %d", f)
	}
}

func TestBatcherFlushesOnClose(t *testing.T) {
	mw := &mockFileWriter{}
	b := NewBatcher(BatchConfig{
		MaxRows:       100,
		MaxBytes:      10 * 1024 * 1024,
		FlushInterval: time.Hour,
	}, mw)

	for i := 0; i < 5; i++ {
		if err := b.Write(makeEvent(int64(i))); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	b.Close()

	// Should have flushed remaining 5 rows
	f := mw.flushes.Load()
	r := mw.rows.Load()
	if f < 1 {
		t.Errorf("expected at least 1 flush, got %d", f)
	}
	if r != 5 {
		t.Errorf("expected 5 rows flushed, got %d", r)
	}
}

func TestBatcherFlushesOnInterval(t *testing.T) {
	mw := &mockFileWriter{}
	b := NewBatcher(BatchConfig{
		MaxRows:       1000,
		MaxBytes:      10 * 1024 * 1024,
		FlushInterval: 50 * time.Millisecond,
	}, mw)
	defer b.Close()

	if err := b.Write(makeEvent(1)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	f := mw.flushes.Load()
	if f < 1 {
		t.Errorf("expected at least 1 flush, got %d", f)
	}
}

func TestBatcherBackpressureBlocks(t *testing.T) {
	mw := &mockFileWriter{}
	b := NewBatcher(BatchConfig{
		MaxRows:       100,
		MaxBytes:      10 * 1024 * 1024,
		FlushInterval: time.Hour,
		MaxPending:    2,
	}, mw)

	// Write 2 events (fills pendingCh).
	if err := b.Write(makeEvent(1)); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	if err := b.Write(makeEvent(2)); err != nil {
		t.Fatalf("Write 2: %v", err)
	}

	// 3rd write should block. Detect via goroutine + timeout.
	blocked := make(chan error, 1)
	go func() {
		blocked <- b.Write(makeEvent(3))
	}()

	select {
	case err := <-blocked:
		t.Fatalf("3rd write should have blocked, but returned: %v", err)
	case <-time.After(50 * time.Millisecond):
		// Good — it blocked.
	}

	// Flush should unblock the writer.
	b.Flush()

	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("3rd write after flush: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("3rd write did not unblock after flush")
	}

	b.Close()
}

func TestBatcherBackpressureDisabled(t *testing.T) {
	mw := &mockFileWriter{}
	b := NewBatcher(BatchConfig{
		MaxRows:       1000,
		MaxBytes:      10 * 1024 * 1024,
		FlushInterval: time.Hour,
		MaxPending:    0,
	}, mw)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			if err := b.Write(makeEvent(int64(i))); err != nil {
				t.Errorf("Write %d: %v", i, err)
			}
		}
		close(done)
	}()

	select {
	case <-done:
		// All writes completed without blocking.
	case <-time.After(200 * time.Millisecond):
		t.Fatal("writes blocked with MaxPending=0")
	}

	b.Close()
}

func TestBackpressureDoesNotDeadlock(t *testing.T) {
	mw := &mockFileWriter{}
	b := NewBatcher(BatchConfig{
		MaxRows:       5,
		MaxBytes:      10 * 1024 * 1024,
		FlushInterval: time.Hour,
		MaxPending:    5,
	}, mw)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 20; i++ {
			if err := b.Write(makeEvent(int64(i))); err != nil {
				t.Errorf("Write %d: %v", i, err)
			}
		}
		close(done)
	}()

	// Concurrent flush loop to drain.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			b.Close()
			return
		case <-ticker.C:
			b.Flush()
		case <-time.After(2 * time.Second):
			t.Fatal("deadlock detected: writes did not complete")
		}
	}
}

func TestBatcherMaxPendingFlushDrains(t *testing.T) {
	mw := &mockFileWriter{}
	b := NewBatcher(BatchConfig{
		MaxRows:       10,
		MaxBytes:      10 * 1024 * 1024,
		FlushInterval: time.Hour,
		MaxPending:    5,
	}, mw)

	for i := 0; i < 5; i++ {
		if err := b.Write(makeEvent(int64(i))); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	// Flush should drain all 5 from pendingCh.
	b.Flush()

	// After flush we should be able to write again without issue.
	if err := b.Write(makeEvent(99)); err != nil {
		t.Fatalf("Write after flush: %v", err)
	}

	b.Close()
	flushes := mw.flushes.Load()
	if flushes < 1 {
		t.Errorf("expected at least 1 flush, got %d", flushes)
	}
}

func TestToOneRowConversion(t *testing.T) {
	now := time.Now()
	evt := &almanac.ChangeEvent{
		ID:              1,
		Type:            "edit",
		Namespace:       0,
		Title:           "Test",
		TitleURL:        "https://example.com",
		Comment:         "comment",
		Timestamp:       now.Unix(),
		User:            "user",
		Bot:             false,
		ServerURL:       "https://example.com",
		ServerName:      "Example",
		ServerScriptURL: "https://example.com/w",
		Wiki:            "testwiki",
		ParsedTimestamp: now,
	}

	row := toRow(evt)

	if row.ID != evt.ID {
		t.Errorf("ID: got %d, want %d", row.ID, evt.ID)
	}
	if row.Type != evt.Type {
		t.Errorf("Type: got %q, want %q", row.Type, evt.Type)
	}
	if row.Title != evt.Title {
		t.Errorf("Title: got %q, want %q", row.Title, evt.Title)
	}
	if row.User != evt.User {
		t.Errorf("User: got %q, want %q", row.User, evt.User)
	}
	if row.Bot != evt.Bot {
		t.Errorf("Bot: got %v, want %v", row.Bot, evt.Bot)
	}
	if row.Wiki != evt.Wiki {
		t.Errorf("Wiki: got %q, want %q", row.Wiki, evt.Wiki)
	}
	if row.ParsedTimestamp != now.Unix() {
		t.Errorf("ParsedTimestamp: got %d, want %d", row.ParsedTimestamp, now.Unix())
	}
}
