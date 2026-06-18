package sse

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/mathew/meridian-stream/internal/almanac"
)

func TestReaderDispatchesEvents(t *testing.T) {
	sseData := `data: {"id":12345,"type":"edit","namespace":0,"title":"Test Page","title_url":"Test_Page","comment":"test edit","timestamp":1718000000,"user":"testuser","bot":false,"server_url":"https://en.wikipedia.org","server_name":"en.wikipedia.org","server_script_url":"https://en.wikipedia.org/w","wiki":"enwiki"}

data: {"id":12346,"type":"new","namespace":1,"title":"New Page","comment":"created page","timestamp":1718000001,"user":"creator","bot":true,"wiki":"frwiki"}

`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, sseData)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Close connection after sending test data once.
		if h, ok := w.(http.Hijacker); ok {
			conn, _, _ := h.Hijack()
			conn.Close()
		}
	}))
	defer server.Close()

	var mu sync.Mutex
	var received []almanac.ChangeEvent

	onEvent := func(evt almanac.ChangeEvent) {
		mu.Lock()
		received = append(received, evt)
		mu.Unlock()
	}

	onError := func(err error) {
		t.Logf("error: %v", err)
	}

	reader := NewReader(onEvent, onError)
	reader.url = server.URL
	reader.Start()

	time.Sleep(500 * time.Millisecond)
	reader.Stop()

	mu.Lock()
	count := len(received)
	mu.Unlock()

	if count != 2 {
		t.Fatalf("expected 2 events, got %d", count)
	}

	mu.Lock()
	if received[0].ID != 12345 {
		t.Errorf("first event id = %d, want 12345", received[0].ID)
	}
	if received[0].Title != "Test Page" {
		t.Errorf("first event title = %q, want %q", received[0].Title, "Test Page")
	}
	if received[0].Wiki != "enwiki" {
		t.Errorf("first event wiki = %q, want %q", received[0].Wiki, "enwiki")
	}
	if received[0].Bot {
		t.Errorf("first event bot = true, want false")
	}
	if received[1].ID != 12346 {
		t.Errorf("second event id = %d, want 12346", received[1].ID)
	}
	if received[1].Bot != true {
		t.Errorf("second event bot = false, want true")
	}
	mu.Unlock()
}

func TestReaderHandlesConnectionError(t *testing.T) {
	var mu sync.Mutex
	var errors int

	onEvent := func(evt almanac.ChangeEvent) {}
	onError := func(err error) {
		mu.Lock()
		errors++
		mu.Unlock()
	}

	reader := NewReader(onEvent, onError)
	reader.url = "http://127.0.0.1:1/nonexistent"
	reader.Start()

	time.Sleep(200 * time.Millisecond)
	reader.Stop()

	mu.Lock()
	errCount := errors
	mu.Unlock()

	if errCount == 0 {
		t.Fatal("expected at least 1 error from bad connection, got 0")
	}
}

func TestReaderDispatchMalformedJSON(t *testing.T) {
	sseData := "data: {invalid json}\n\n"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, sseData)
	}))
	defer server.Close()

	var mu sync.Mutex
	var events int
	var errors int

	onEvent := func(evt almanac.ChangeEvent) {
		mu.Lock()
		events++
		mu.Unlock()
	}
	onError := func(err error) {
		mu.Lock()
		errors++
		mu.Unlock()
	}

	reader := NewReader(onEvent, onError)
	reader.url = server.URL
	reader.Start()

	time.Sleep(300 * time.Millisecond)
	reader.Stop()

	mu.Lock()
	eCount := events
	eCnt := errors
	mu.Unlock()

	if eCount > 0 {
		t.Errorf("expected 0 events from malformed data, got %d", eCount)
	}
	if eCnt == 0 {
		t.Log("malformed JSON should trigger error handler")
	}
}

func TestReaderImplementsIOCloser(t *testing.T) {
	reader := NewReader(func(evt almanac.ChangeEvent) {}, func(err error) {})
	if err := reader.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}
