// Package codec provides Avro serialization with the Confluent wire format.
package codec

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/mathew/meridian-stream/internal/almanac"
	"github.com/mathew/meridian-stream/internal/almanac/schema"
	"github.com/hamba/avro/v2"
)

var magicByte byte = 0x00

type Codec struct {
	client *schema.Client
	schema string
	id     int
}

func NewCodec(client *schema.Client, schemaJSON string) *Codec {
	return &Codec{client: client, schema: schemaJSON}
}

func (c *Codec) Register(subject string) error {
	id, err := c.client.Register(subject, c.schema)
	if err != nil { return fmt.Errorf("register schema: %w", err) }
	c.id = id
	return nil
}

type avroEvent struct {
	ID              int64  `avro:"id"`
	Type            string `avro:"type"`
	Namespace       int32  `avro:"namespace"`
	Title           string `avro:"title"`
	TitleURL        string `avro:"title_url"`
	Comment         string `avro:"comment"`
	Timestamp       int64  `avro:"timestamp"`
	User            string `avro:"user"`
	Bot             bool   `avro:"bot"`
	ServerURL       string `avro:"server_url"`
	ServerName      string `avro:"server_name"`
	ServerScriptURL string `avro:"server_script_url"`
	Wiki            string `avro:"wiki"`
	ParsedTimestamp int64  `avro:"parsed_timestamp"`
	Minor           int    `avro:"minor"`
	PageID          *int64 `avro:"page_id"`
}

func (c *Codec) Encode(evt *almanac.ChangeEvent) ([]byte, error) {
	parsed, _ := avro.Parse(c.schema)
	data, err := avro.Marshal(parsed, toAvro(evt))
	if err != nil { return nil, fmt.Errorf("avro marshal: %w", err) }
	var hdr [5]byte
	hdr[0] = magicByte
	binary.BigEndian.PutUint32(hdr[1:], uint32(c.id))
	msg := make([]byte, 5+len(data))
	copy(msg[:5], hdr[:])
	copy(msg[5:], data)
	return msg, nil
}

func (c *Codec) Decode(data []byte) (*almanac.ChangeEvent, error) {
	if len(data) < 5 { return nil, fmt.Errorf("short message: %d bytes", len(data)) }
	if data[0] != magicByte { return nil, fmt.Errorf("bad magic byte: 0x%02x", data[0]) }
	id := int(binary.BigEndian.Uint32(data[1:5]))
	schemaStr, err := c.client.GetByID(id)
	if err != nil { return nil, fmt.Errorf("get schema %d: %w", id, err) }
	parsed, _ := avro.Parse(schemaStr)
	a := &avroEvent{}
	if err := avro.Unmarshal(parsed, data[5:], a); err != nil { return nil, fmt.Errorf("avro unmarshal: %w", err) }
	return fromAvro(a), nil
}

func toAvro(evt *almanac.ChangeEvent) *avroEvent { return &avroEvent{ID: evt.ID, Type: evt.Type, Namespace: int32(evt.Namespace), Title: evt.Title, TitleURL: evt.TitleURL, Comment: evt.Comment, Timestamp: evt.Timestamp, User: evt.User, Bot: evt.Bot, ServerURL: evt.ServerURL, ServerName: evt.ServerName, ServerScriptURL: evt.ServerScriptURL, Wiki: evt.Wiki, ParsedTimestamp: evt.ParsedTimestamp.Unix()} }
func fromAvro(a *avroEvent) *almanac.ChangeEvent { return &almanac.ChangeEvent{ID: a.ID, Type: a.Type, Namespace: int(a.Namespace), Title: a.Title, TitleURL: a.TitleURL, Comment: a.Comment, Timestamp: a.Timestamp, User: a.User, Bot: a.Bot, ServerURL: a.ServerURL, ServerName: a.ServerName, ServerScriptURL: a.ServerScriptURL, Wiki: a.Wiki, ParsedTimestamp: time.Unix(a.ParsedTimestamp, 0).UTC()} }
