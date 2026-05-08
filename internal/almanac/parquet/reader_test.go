package parquet

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// writeTestParquet creates a temporary Parquet file with sample rows for testing.
func writeTestParquet(t *testing.T, rows []parquetRow) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.parquet")

	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		t.Fatalf("create temp parquet writer: %v", err)
	}

	pw, err := writer.NewParquetWriter(fw, new(parquetRow), 4)
	if err != nil {
		fw.Close()
		t.Fatalf("create parquet writer: %v", err)
	}
	pw.RowGroupSize = 128 * 1024
	pw.PageSize = 8 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for i := range rows {
		if err := pw.Write(rows[i]); err != nil {
			fw.Close()
			t.Fatalf("write row %d: %v", i, err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		fw.Close()
		t.Fatalf("write stop: %v", err)
	}
	fw.Close()
	return path
}

func TestReadFile_Empty(t *testing.T) {
	rows := writeTestParquet(t, []parquetRow{})
	events, err := ReadFile(rows)
	if err != nil {
		t.Fatalf("ReadFile(empty): %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}
}

func TestReadFile_RoundTrip(t *testing.T) {
	now := time.Now().UTC()
	expected := []parquetRow{
		{
			ID:              1,
			Type:            "edit",
			Namespace:       0,
			Title:           "Test_Page",
			TitleURL:        "Test_Page",
			Comment:         "test edit",
			Timestamp:       now.Unix(),
			User:            "tester",
			Bot:             false,
			ServerURL:       "https://example.org",
			ServerName:      "Example Wiki",
			ServerScriptURL: "https://example.org/w",
			Wiki:            "testwiki",
			ParsedTimestamp: now.Unix(),
		},
		{
			ID:              2,
			Type:            "new",
			Namespace:       1,
			Title:           "New_Page",
			TitleURL:        "New_Page",
			Comment:         "created page",
			Timestamp:       now.Unix(),
			User:            "creator",
			Bot:             true,
			ServerURL:       "https://example.org",
			ServerName:      "Example Wiki",
			ServerScriptURL: "https://example.org/w",
			Wiki:            "enwiki",
			ParsedTimestamp: now.Unix(),
		},
	}

	path := writeTestParquet(t, expected)
	events, err := ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(events) != len(expected) {
		t.Fatalf("expected %d events, got %d", len(expected), len(events))
	}

	for i, evt := range events {
		if evt.ID != expected[i].ID {
			t.Errorf("event %d: ID=%d, want %d", i, evt.ID, expected[i].ID)
		}
		if evt.Type != expected[i].Type {
			t.Errorf("event %d: Type=%q, want %q", i, evt.Type, expected[i].Type)
		}
		if evt.Namespace != int(expected[i].Namespace) {
			t.Errorf("event %d: Namespace=%d, want %d", i, evt.Namespace, expected[i].Namespace)
		}
		if evt.Title != expected[i].Title {
			t.Errorf("event %d: Title=%q, want %q", i, evt.Title, expected[i].Title)
		}
		if evt.User != expected[i].User {
			t.Errorf("event %d: User=%q, want %q", i, evt.User, expected[i].User)
		}
		if evt.Bot != expected[i].Bot {
			t.Errorf("event %d: Bot=%v, want %v", i, evt.Bot, expected[i].Bot)
		}
		if evt.Wiki != expected[i].Wiki {
			t.Errorf("event %d: Wiki=%q, want %q", i, evt.Wiki, expected[i].Wiki)
		}
		// Verify parsed timestamp conversion
		expectedParsed := time.Unix(expected[i].ParsedTimestamp, 0).UTC()
		if !evt.ParsedTimestamp.Equal(expectedParsed) {
			t.Errorf("event %d: ParsedTimestamp=%v, want %v", i, evt.ParsedTimestamp, expectedParsed)
		}
	}
}

func TestReadFile_NonExistent(t *testing.T) {
	_, err := ReadFile("/nonexistent/file.parquet")
	if err == nil {
		t.Fatal("expected error for non-existent file, got nil")
	}
}

func TestReadFile_LargeBatch(t *testing.T) {
	n := 100
	rows := make([]parquetRow, n)
	now := time.Now().Unix()
	for i := 0; i < n; i++ {
		rows[i] = parquetRow{
			ID:        int64(1000 + i),
			Type:      "edit",
			Title:     "Batch_Page",
			User:      "batch_tester",
			Timestamp: now,
			Wiki:      "testwiki",
		}
	}

	path := writeTestParquet(t, rows)
	events, err := ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%d rows): %v", n, err)
	}
	if len(events) != n {
		t.Fatalf("expected %d events, got %d", n, len(events))
	}
}

func TestToEvent(t *testing.T) {
	now := time.Now().Unix()
	r := &parquetRow{
		ID:              42,
		Type:            "log",
		Namespace:       2,
		Title:           "Log_Page",
		TitleURL:        "Log_Page",
		Comment:         "automated log",
		Timestamp:       now,
		User:            "logger",
		Bot:             false,
		ServerURL:       "https://log.example.org",
		ServerName:      "Log Wiki",
		ServerScriptURL: "https://log.example.org/w",
		Wiki:            "logwiki",
		ParsedTimestamp: now,
	}

	evt := toEvent(r)
	if evt.ID != 42 {
		t.Errorf("ID=42, got %d", evt.ID)
	}
	if evt.Type != "log" {
		t.Errorf("Type=log, got %q", evt.Type)
	}
	if evt.Namespace != 2 {
		t.Errorf("Namespace=2, got %d", evt.Namespace)
	}
	if evt.Wiki != "logwiki" {
		t.Errorf("Wiki=logwiki, got %q", evt.Wiki)
	}
	expectedTime := time.Unix(now, 0).UTC()
	if !evt.ParsedTimestamp.Equal(expectedTime) {
		t.Errorf("ParsedTimestamp=%v, want %v", evt.ParsedTimestamp, expectedTime)
	}
}
