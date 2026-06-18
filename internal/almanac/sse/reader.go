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

	"github.com/mathew/meridian-stream/internal/almanac"
)

const (
	// DefaultEventSourceURL is the Wikimedia RecentChanges SSE stream.
	DefaultEventSourceURL = "https://stream.wikimedia.org/v2/stream/recentchange"

	// ReconnectDelay is the wait time before reconnecting after a failure.
	ReconnectDelay = 5 * time.Second
)

// EventHandler is called for each parsed ChangeEvent.
type EventHandler func(almanac.ChangeEvent)

// ErrorHandler is called for non-fatal errors during streaming.
type ErrorHandler func(error)

// Reader connects to Wikimedia EventStreams and dispatches ChangeEvents.
type Reader struct {
	url     string
	onEvent EventHandler
	onError ErrorHandler
	closeCh chan struct{}
}

// NewReader creates a new SSE reader for the given event handler and error handler.
func NewReader(onEvent EventHandler, onError ErrorHandler) *Reader {
	return &Reader{
		url:     DefaultEventSourceURL,
		onEvent: onEvent,
		onError: onError,
		closeCh: make(chan struct{}),
	}
}

// Start begins reading the SSE stream in a background goroutine.
func (r *Reader) Start() {
	go r.loop()
}

// Stop signals the reader to shut down gracefully.
func (r *Reader) Stop() {
	close(r.closeCh)
}

// Close implements io.Closer.
func (r *Reader) Close() error {
	r.Stop()
	return nil
}

var _ io.Closer = (*Reader)(nil)

func (r *Reader) loop() {
	for {
		select {
		case <-r.closeCh:
			return
		default:
		}
		if err := r.connect(); err != nil {
			if r.onError != nil {
				r.onError(fmt.Errorf("sse connect: %w", err))
			}
			select {
			case <-r.closeCh:
				return
			case <-time.After(ReconnectDelay):
			}
		}
	}
}

func (r *Reader) connect() error {
	req, err := http.NewRequest(http.MethodGet, r.url, http.NoBody)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("User-Agent", "Meridian/0.2 (realtime-ingestion-pipeline; contact@meridian.dev)")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, resp.Status)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var eventData strings.Builder
	for scanner.Scan() {
		select {
		case <-r.closeCh:
			return nil
		default:
		}

		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "data: "):
			eventData.WriteString(line[6:])
		case line == "" && eventData.Len() > 0:
			r.dispatch(eventData.String())
			eventData.Reset()
		}
	}
	return scanner.Err()
}

func (r *Reader) dispatch(data string) {
	var raw struct {
		ID              int64  `json:"id"`
		Type            string `json:"type"`
		Namespace       int    `json:"namespace"`
		Title           string `json:"title"`
		TitleURL        string `json:"title_url"`
		Comment         string `json:"comment"`
		Timestamp       int64  `json:"timestamp"`
		User            string `json:"user"`
		Bot             bool   `json:"bot"`
		ServerURL       string `json:"server_url"`
		ServerName      string `json:"server_name"`
		ServerScriptURL string `json:"server_script_url"`
		Wiki            string `json:"wiki"`
	}
	if err := json.Unmarshal([]byte(data), &raw); err != nil {
		if r.onError != nil {
			r.onError(fmt.Errorf("sse unmarshal: %w", err))
		}
		return
	}
	evt := almanac.ChangeEvent{
		ID:              raw.ID,
		Type:            raw.Type,
		Namespace:       raw.Namespace,
		Title:           raw.Title,
		TitleURL:        raw.TitleURL,
		Comment:         raw.Comment,
		Timestamp:       raw.Timestamp,
		User:            raw.User,
		Bot:             raw.Bot,
		ServerURL:       raw.ServerURL,
		ServerName:      raw.ServerName,
		ServerScriptURL: raw.ServerScriptURL,
		Wiki:            raw.Wiki,
		ParsedTimestamp: time.Unix(raw.Timestamp, 0).UTC(),
	}
	r.onEvent(evt)
}
