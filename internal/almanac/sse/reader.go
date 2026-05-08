// Package sse provides a streaming reader for Wikimedia EventStreams SSE.
package sse

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"log"

	"github.com/mathew/meridian-stream/internal/almanac"
)

const DefaultEventSourceURL = "https://stream.wikimedia.org/v2/stream/recentchange"
const ReconnectDelay = 5 * time.Second

type EventHandler func(almanac.ChangeEvent)
type ErrorHandler func(error)

type Reader struct {
	url     string
	onEvent EventHandler
	onError ErrorHandler
	closeCh chan struct{}
}

func NewReader(onEvent EventHandler, onError ErrorHandler) *Reader {
	return &Reader{
		url:     DefaultEventSourceURL,
		onEvent: onEvent,
		onError: onError,
		closeCh: make(chan struct{}),
	}
}

func (r *Reader) Start() { go r.loop() }
func (r *Reader) Stop()  { close(r.closeCh) }
