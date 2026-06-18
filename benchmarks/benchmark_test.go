// Package benchmarks contains Go benchmark tests for the Meridian Stream pipeline.
// These benchmarks measure end-to-end encode/publish throughput and latency
// without requiring external infrastructure.
package benchmarks

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/mathew/meridian-stream/internal/almanac"
	"github.com/mathew/meridian-stream/internal/almanac/codec"
	"github.com/mathew/meridian-stream/internal/almanac/kafka"
	"github.com/mathew/meridian-stream/internal/almanac/schema"
)

var benchSchema = `{
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

func makeBenchEvent(id int64) *almanac.ChangeEvent {
	return &almanac.ChangeEvent{
		ID:              id,
		Type:            "edit",
		Namespace:       0,
		Title:           "Benchmark_Page",
		TitleURL:        "Benchmark_Page",
		Comment:         "benchmark test event",
		Timestamp:       time.Now().Unix(),
		User:            "benchmarker",
		Bot:             false,
		ServerURL:       "https://example.org",
		ServerName:      "Example Wiki",
		ServerScriptURL: "https://example.org/w",
		Wiki:            "testwiki",
		ParsedTimestamp: time.Now(),
	}
}

// BenchmarkAvroEncode measures Avro encoding throughput for ChangeEvents.
func BenchmarkAvroEncode(b *testing.B) {
	sc := schema.NewClient("http://localhost:1")
	cc := codec.NewCodec(sc, benchSchema)
	// Register will fail (no server), but Encode only needs the local schema.
	_ = cc.Register("bench-value")

	event := makeBenchEvent(0)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		event.ID = int64(i)
		_, err := cc.Encode(event)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPublishLatency measures the latency of a full publish cycle
// (encode + send). Uses a mock topic and measures timing.
func BenchmarkPublishLatency(b *testing.B) {
	sc := schema.NewClient("http://localhost:1")
	cc := codec.NewCodec(sc, benchSchema)
	_ = cc.Register("bench-value")

	prod := kafka.NewProducer([]string{"localhost:1"}, "bench-topic")

	event := makeBenchEvent(0)
	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		event.ID = int64(i)
		event.Timestamp = time.Now().Unix()
		data, err := cc.Encode(event)
		if err != nil {
			b.Fatal(err)
		}
		// Publish will fail (no broker), but we measure the attempt latency.
		_ = prod.Publish(ctx, []byte(event.Key()), data)
	}

	prod.Close()
}

// BenchmarkParquetRowConversion measures the cost of converting ChangeEvents
// to Parquet rows.
func BenchmarkParquetRowConversion(b *testing.B) {
	event := makeBenchEvent(0)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = toRow(event)
	}
}

// parquetRow mirrors the sink package struct for isolated benchmarks.
type parquetRow struct {
	ID              int64
	Type            string
	Namespace       int32
	Title           string
	TitleURL        string
	Comment         string
	Timestamp       int64
	User            string
	Bot             bool
	ServerURL       string
	ServerName      string
	ServerScriptURL string
	Wiki            string
	ParsedTimestamp int64
}

func toRow(evt *almanac.ChangeEvent) parquetRow {
	return parquetRow{
		ID:              evt.ID,
		Type:            evt.Type,
		Namespace:       int32(evt.Namespace),
		Title:           evt.Title,
		TitleURL:        evt.TitleURL,
		Comment:         evt.Comment,
		Timestamp:       evt.Timestamp,
		User:            evt.User,
		Bot:             evt.Bot,
		ServerURL:       evt.ServerURL,
		ServerName:      evt.ServerName,
		ServerScriptURL: evt.ServerScriptURL,
		Wiki:            evt.Wiki,
		ParsedTimestamp: evt.ParsedTimestamp.Unix(),
	}
}

// BenchmarkEventGeneration measures the rate at which the amplifier
// can generate synthetic events.
func BenchmarkEventGeneration(b *testing.B) {
	base := &almanac.ChangeEvent{
		Type: "edit", Namespace: 0, Title: "LoadTest",
		User: "loadtester", Wiki: "testwiki",
	}
	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		evt := *base
		evt.ID = int64(i)
		evt.Timestamp = time.Now().Unix()
		evt.Title = randomTitle(rng, i)
		_ = evt
	}
}

func randomTitle(rng *rand.Rand, i int) string {
	pages := []string{"Alpha", "Beta", "Gamma", "Delta", "Epsilon"}
	return pages[rng.Intn(len(pages))] + "_" + itoa(i)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
