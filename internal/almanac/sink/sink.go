// Package sink provides interfaces for writing ChangeEvents to a sink destination.
package sink

import "github.com/mathew/meridian-stream/internal/almanac"

type Sink interface {
	Write(evt *almanac.ChangeEvent) error
	Flush() error
	Close() error
}
