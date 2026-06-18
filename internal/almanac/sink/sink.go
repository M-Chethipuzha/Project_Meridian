// Package sink provides a write-ahead buffer and Parquet sink for
// batch-writing ChangeEvents to MinIO in time-partitioned Parquet files.
package sink

import "github.com/mathew/meridian-stream/internal/almanac"

// Sink is the interface for writing ChangeEvents to a sink destination.
type Sink interface {
	Write(evt *almanac.ChangeEvent) error
	Flush() error
	Close() error
}
