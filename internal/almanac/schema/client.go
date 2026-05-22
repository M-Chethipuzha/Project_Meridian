// Package schema provides a client for the Redpanda Schema Registry.
package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
)

type Client struct {
	baseURL string
	hc      *http.Client
	cacheMu sync.RWMutex
	cache   map[int]string
}

func NewClient(baseURL string) *Client {
	return &Client{baseURL: baseURL, hc: &http.Client{}, cache: make(map[int]string)}
}

func (c *Client) Register(subject string, schemaJSON string) (int, error) {
	body, _ := json.Marshal(map[string]string{"schema": schemaJSON})
	req, _ := http.NewRequest(http.MethodPost, c.baseURL+"/subjects/"+subject+"/versions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	resp, err := c.hc.Do(req)
	if err != nil { return 0, fmt.Errorf("register schema: %w", err) }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { b, _ := io.ReadAll(resp.Body); return 0, fmt.Errorf("register schema: %s: %s", resp.Status, string(b)) }
	var r struct{ ID int `json:"id"` }
	json.NewDecoder(resp.Body).Decode(&r)
	c.cacheMu.Lock(); c.cache[r.ID] = schemaJSON; c.cacheMu.Unlock()
	return r.ID, nil
}

func (c *Client) GetByID(id int) (string, error) {
	c.cacheMu.RLock(); s, ok := c.cache[id]; c.cacheMu.RUnlock()
	if ok { return s, nil }
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/schemas/ids/%d", c.baseURL, id), nil)
	resp, err := c.hc.Do(req)
	if err != nil { return "", fmt.Errorf("get schema %d: %w", id, err) }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { return "", fmt.Errorf("get schema %d: %s", id, resp.Status) }
	var r struct{ Schema string `json:"schema"` }
	json.NewDecoder(resp.Body).Decode(&r)
	c.cacheMu.Lock(); c.cache[id] = r.Schema; c.cacheMu.Unlock()
	return r.Schema, nil
}
