package schema

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRegisterAndGetByID(t *testing.T) {
	schemaJSON := `{"type":"record","name":"Test","fields":[{"name":"x","type":"long"}]}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/subjects/test-value/versions":
			if r.Method != http.MethodPost {
				t.Errorf("expected POST, got %s", r.Method)
			}
			w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
			w.Write([]byte(`{"id":42}`))
		case "/schemas/ids/42":
			if r.Method != http.MethodGet {
				t.Errorf("expected GET, got %s", r.Method)
			}
			w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
			w.Write([]byte(`{"schema":"{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"}]}"}`))
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer ts.Close()

	client := NewClient(ts.URL)

	id, err := client.Register("test-value", schemaJSON)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if id != 42 {
		t.Fatalf("expected id 42, got %d", id)
	}

	got, err := client.GetByID(42)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got != schemaJSON {
		t.Fatalf("expected %q, got %q", schemaJSON, got)
	}
}

func TestGetByIDCaches(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// First call succeeds; subsequent calls 503 to prove cache
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		w.Write([]byte(`{"schema":"{\"type\":\"record\"}"}`))
	}))
	defer ts.Close()

	client := NewClient(ts.URL)

	// First call hits the server (populates cache)
	s1, err := client.GetByID(1)
	if err != nil {
		t.Fatalf("first GetByID: %v", err)
	}

	// Server shuts down; second call must hit cache
	ts.Close()
	s2, err := client.GetByID(1)
	if err != nil {
		t.Fatalf("second GetByID (should be cached): %v", err)
	}

	if s1 != s2 {
		t.Errorf("cached result mismatch: %q vs %q", s1, s2)
	}
}

func TestRegisterError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte(`{"error_code":40901,"message":"Schema already registered"}`))
	}))
	defer ts.Close()

	client := NewClient(ts.URL)
	_, err := client.Register("test-value", `{"type":"record"}`)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestSetCompatibility(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody map[string]string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if r.Body != nil {
			json.NewDecoder(r.Body).Decode(&gotBody)
		}
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		w.Write([]byte(`{"compatibility":"BACKWARD"}`))
	}))
	defer ts.Close()

	client := NewClient(ts.URL)
	if err := client.SetCompatibility("test-value", "BACKWARD"); err != nil {
		t.Fatalf("SetCompatibility: %v", err)
	}

	if gotMethod != http.MethodPut {
		t.Errorf("expected PUT, got %s", gotMethod)
	}
	if gotPath != "/config/test-value" {
		t.Errorf("expected /config/test-value, got %s", gotPath)
	}
	if gotBody["compatibility"] != "BACKWARD" {
		t.Errorf("expected compatibility=BACKWARD, got %v", gotBody)
	}
}

func TestSetCompatibilityError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte(`{"error_code":422,"message":"Invalid compatibility level"}`))
	}))
	defer ts.Close()

	client := NewClient(ts.URL)
	err := client.SetCompatibility("test-value", "INVALID")
	if err == nil {
		t.Fatal("expected error for invalid mode, got nil")
	}
}

func TestSetCompatibilityInvalidMode(t *testing.T) {
	client := NewClient("http://localhost:1")
	err := client.SetCompatibility("test-value", "NOT_A_MODE")
	if err == nil {
		t.Fatal("expected error for invalid mode, got nil")
	}
}

func TestGetByIDError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	client := NewClient(ts.URL)
	_, err := client.GetByID(999)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
