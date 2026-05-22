package sink

import (
	"testing"
	"time"
	"github.com/mathew/meridian-stream/internal/almanac"
)

type mockWriter struct{ rows int; err error }
func (m *mockWriter) WriteFile(_ []parquetRow, _ time.Time) error { m.rows++; return m.err }

func TestBatcherFlushOnRowCount(t *testing.T) {
	mw := &mockWriter{}
	b := NewBatcher(BatchConfig{MaxRows: 10, FlushInterval: time.Hour}, mw)
	defer b.Close()
	for i := 0; i < 10; i++ {
		b.Write(&almanac.ChangeEvent{ID: int64(i)})
	}
	time.Sleep(50 * time.Millisecond)
	if mw.rows != 1 { t.Fatalf("expected 1 flush, got %d", mw.rows) }
}
