package schema

import "testing"

func TestNewClient(t *testing.T) {
	c := NewClient("http://localhost:8081")
	if c == nil { t.Fatal("expected client") }
}

func TestRegisterInvalid(t *testing.T) {
	c := NewClient("http://localhost:9999")
	_, err := c.Register("test", `{"type":"record","name":"X","fields":[]}`)
	if err == nil { t.Fatal("expected error") }
}
