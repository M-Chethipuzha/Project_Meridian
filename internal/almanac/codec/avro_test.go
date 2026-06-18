package codec

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mathew/meridian-stream/internal/almanac"
	"github.com/mathew/meridian-stream/internal/almanac/schema"
)

var testSchemaV2 = `{
	"type": "record",
	"name": "ChangeEvent",
	"namespace": "meridian",
	"fields": [
		{"name": "id", "type": "long"},
		{"name": "type", "type": "string"},
		{"name": "namespace", "type": "int"},
		{"name": "title", "type": "string"},
		{"name": "title_url", "type": "string"},
		{"name": "comment", "type": "string"},
		{"name": "timestamp", "type": "long"},
		{"name": "user", "type": "string"},
		{"name": "bot", "type": "boolean"},
		{"name": "server_url", "type": "string"},
		{"name": "server_name", "type": "string"},
		{"name": "server_script_url", "type": "string"},
		{"name": "wiki", "type": "string"},
		{"name": "parsed_timestamp", "type": "long"},
		{"name": "minor", "type": "int", "default": 0},
		{"name": "page_id", "type": ["null", "long"], "default": null}
	]
}`

var testSchema = `{
	"type": "record",
	"name": "ChangeEvent",
	"namespace": "meridian",
	"fields": [
		{"name": "id", "type": "long"},
		{"name": "type", "type": "string"},
		{"name": "namespace", "type": "int"},
		{"name": "title", "type": "string"},
		{"name": "title_url", "type": "string"},
		{"name": "comment", "type": "string"},
		{"name": "timestamp", "type": "long"},
		{"name": "user", "type": "string"},
		{"name": "bot", "type": "boolean"},
		{"name": "server_url", "type": "string"},
		{"name": "server_name", "type": "string"},
		{"name": "server_script_url", "type": "string"},
		{"name": "wiki", "type": "string"},
		{"name": "parsed_timestamp", "type": "long"}
	]
}`

func TestEncodeDecodeRoundTrip(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		switch r.URL.Path {
		case "/subjects/test-value/versions":
			w.Write([]byte(`{"id":1}`))
		case "/schemas/ids/1":
			w.Write([]byte(`{"schema":"` + escapeJSON(testSchema) + `"}`))
		}
	}))
	defer ts.Close()

	sc := schema.NewClient(ts.URL)
	cc := NewCodec(sc, testSchema)

	if err := cc.Register("test-value"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	original := almanac.ChangeEvent{
		ID:              123,
		Type:            "edit",
		Namespace:       0,
		Title:           "Test Article",
		TitleURL:        "https://en.wikipedia.org/wiki/Test_Article",
		Comment:         "test edit",
		Timestamp:       1700000000,
		User:            "tester",
		Bot:             false,
		ServerURL:       "https://en.wikipedia.org",
		ServerName:      "Wikipedia",
		ServerScriptURL: "https://en.wikipedia.org/w",
		Wiki:            "enwiki",
		ParsedTimestamp: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
	}

	data, err := cc.Encode(&original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	if len(data) < 5 {
		t.Fatalf("encoded data too short: %d bytes", len(data))
	}
	if data[0] != 0x00 {
		t.Errorf("bad magic byte: 0x%02x", data[0])
	}

	decoded, err := cc.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if decoded.ID != original.ID {
		t.Errorf("ID: got %d, want %d", decoded.ID, original.ID)
	}
	if decoded.Type != original.Type {
		t.Errorf("Type: got %q, want %q", decoded.Type, original.Type)
	}
	if decoded.Title != original.Title {
		t.Errorf("Title: got %q, want %q", decoded.Title, original.Title)
	}
	if decoded.User != original.User {
		t.Errorf("User: got %q, want %q", decoded.User, original.User)
	}
	if decoded.Bot != original.Bot {
		t.Errorf("Bot: got %v, want %v", decoded.Bot, original.Bot)
	}
	if decoded.Wiki != original.Wiki {
		t.Errorf("Wiki: got %q, want %q", decoded.Wiki, original.Wiki)
	}
	if !decoded.ParsedTimestamp.Equal(original.ParsedTimestamp) {
		t.Errorf("ParsedTimestamp: got %v, want %v", decoded.ParsedTimestamp, original.ParsedTimestamp)
	}
}

func TestDecodeShortMessage(t *testing.T) {
	cc := NewCodec(nil, "")
	_, err := cc.Decode([]byte{0x00, 0x01, 0x02})
	if err == nil {
		t.Fatal("expected error for short message")
	}
}

func TestDecodeBadMagicByte(t *testing.T) {
	cc := NewCodec(nil, "")
	_, err := cc.Decode([]byte{0xFF, 0x00, 0x00, 0x00, 0x01, 0x00})
	if err == nil {
		t.Fatal("expected error for bad magic byte")
	}
}

func TestSchemaEvolutionV1toV2(t *testing.T) {
	// Encode with v1 schema, decode with v2 schema — v2 defaults must fill.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		switch r.URL.Path {
		case "/subjects/test-value/versions":
			w.Write([]byte(`{"id":1}`))
		case "/schemas/ids/1":
			w.Write([]byte(`{"schema":"` + escapeJSON(testSchemaV2) + `"}`))
		}
	}))
	defer ts.Close()

	// Register with v1, decode returns v2 schema from registry.
	sc := schema.NewClient(ts.URL)
	ccV1 := NewCodec(sc, testSchema)
	if err := ccV1.Register("test-value"); err != nil {
		t.Fatalf("Register v1: %v", err)
	}

	original := almanac.ChangeEvent{
		ID: 456, Type: "edit", Namespace: 0, Title: "Test",
		Timestamp: 1700000000, User: "tester", Wiki: "enwiki",
		ParsedTimestamp: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
	}

	data, err := ccV1.Encode(&original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Decode with a v2 codec (uses v2 schema from registry).
	ccV2 := NewCodec(sc, "")
	decoded, err := ccV2.Decode(data)
	if err != nil {
		t.Fatalf("Decode with v2 schema: %v", err)
	}

	if decoded.ID != original.ID {
		t.Errorf("ID: got %d, want %d", decoded.ID, original.ID)
	}
}

func TestSchemaEvolutionV2toV1(t *testing.T) {
	// Encode with v2 schema, decode with v1 — extra fields are dropped.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		switch r.URL.Path {
		case "/subjects/test-value/versions":
			w.Write([]byte(`{"id":2}`))
		case "/schemas/ids/2":
			w.Write([]byte(`{"schema":"` + escapeJSON(testSchemaV2) + `"}`))
		}
	}))
	defer ts.Close()

	sc := schema.NewClient(ts.URL)
	ccV2 := NewCodec(sc, testSchemaV2)
	if err := ccV2.Register("test-value"); err != nil {
		t.Fatalf("Register v2: %v", err)
	}

	original := almanac.ChangeEvent{
		ID: 789, Type: "new", Namespace: 0, Title: "V2Test",
		Timestamp: 1700000001, User: "v2user", Wiki: "enwiki",
		ParsedTimestamp: time.Date(2024, 2, 15, 10, 30, 0, 0, time.UTC),
	}

	data, err := ccV2.Encode(&original)
	if err != nil {
		t.Fatalf("Encode v2: %v", err)
	}

	// Decode with v1 codec — extra fields ignored silently.
	ccV1 := NewCodec(sc, testSchema)
	decoded, err := ccV1.Decode(data)
	if err != nil {
		t.Fatalf("Decode with v1 schema: %v", err)
	}

	if decoded.ID != original.ID {
		t.Errorf("ID: got %d, want %d", decoded.ID, original.ID)
	}
}

func escapeJSON(s string) string {
	result := make([]byte, 0, len(s))
	for _, c := range []byte(s) {
		switch c {
		case '"':
			result = append(result, '\\', '"')
		case '\\':
			result = append(result, '\\', '\\')
		default:
			result = append(result, c)
		}
	}
	return string(result)
}
