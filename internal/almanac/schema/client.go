// Package schema provides a client for the Redpanda Schema Registry.
package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

// Client is a thread-safe HTTP client for the Redpanda Schema Registry.
// It caches schema lookups by ID to avoid redundant HTTP requests.
type Client struct {
	baseURL string
	hc      *http.Client
	cacheMu sync.RWMutex
	cache   map[int]string
}

// NewClient creates a new client for the Schema Registry at the given URL.
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		hc:      &http.Client{},
		cache:   make(map[int]string),
	}
}

type registerRequest struct {
	Schema string `json:"schema"`
}

type registerResponse struct {
	ID int `json:"id"`
}

// Register registers a schema under a subject and returns its schema ID.
func (c *Client) Register(subject string, schemaJSON string) (int, error) {
	body, _ := json.Marshal(registerRequest{Schema: schemaJSON})
	req, err := http.NewRequest(http.MethodPost, c.baseURL+"/subjects/"+subject+"/versions", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, fmt.Errorf("register schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("register schema: %s: %s", resp.Status, string(b))
	}

	var r registerResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return 0, fmt.Errorf("decode register response: %w", err)
	}

	c.cacheMu.Lock()
	c.cache[r.ID] = schemaJSON
	c.cacheMu.Unlock()
	return r.ID, nil
}

type schemaResponse struct {
	Schema string `json:"schema"`
}

// GetByID retrieves the schema string for the given ID, using the local cache when possible.
func (c *Client) GetByID(id int) (string, error) {
	c.cacheMu.RLock()
	s, ok := c.cache[id]
	c.cacheMu.RUnlock()
	if ok {
		return s, nil
	}

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/schemas/ids/%d", c.baseURL, id), nil)
	if err != nil {
		return "", err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return "", fmt.Errorf("get schema %d: %w", id, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("get schema %d: %s: %s", id, resp.Status, string(b))
	}

	var r schemaResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return "", fmt.Errorf("decode schema response: %w", err)
	}

	c.cacheMu.Lock()
	c.cache[id] = r.Schema
	c.cacheMu.Unlock()
	return r.Schema, nil
}

// ValidCompatibilityModes lists the accepted compatibility level strings.
var ValidCompatibilityModes = []string{
	"BACKWARD", "FORWARD", "FULL", "NONE",
	"BACKWARD_TRANSITIVE", "FORWARD_TRANSITIVE", "FULL_TRANSITIVE",
}

// SetCompatibility sets the compatibility level for a subject.
// Valid modes: BACKWARD, FORWARD, FULL, NONE, BACKWARD_TRANSITIVE,
// FORWARD_TRANSITIVE, FULL_TRANSITIVE.
func (c *Client) SetCompatibility(subject, mode string) error {
	mode = strings.ToUpper(mode)
	valid := false
	for _, m := range ValidCompatibilityModes {
		if mode == m {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("invalid compatibility mode: %q", mode)
	}

	body, _ := json.Marshal(map[string]string{"compatibility": mode})
	req, err := http.NewRequest(http.MethodPut, c.baseURL+"/config/"+subject, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("set compatibility: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("set compatibility: %s: %s", resp.Status, string(b))
	}
	return nil
}
