// Package almanac defines shared data models for the Meridian Stream pipeline.
package almanac

import "time"

// ChangeEvent represents a single Wikimedia RecentChange event.
// Fields map to the Wikimedia EventStreams SSE JSON schema.
type ChangeEvent struct {
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

	// ParsedTimestamp is the deserialized timestamp.
	ParsedTimestamp time.Time `json:"-"`
}

// Key returns a deterministic key for partitioning and deduplication.
func (e *ChangeEvent) Key() string {
	return e.Wiki + "/" + e.Title
}
