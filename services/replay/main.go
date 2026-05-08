// Command replay reads stored Parquet files from MinIO and re-publishes the
// events back to Redpanda. Supports replay by date range with configurable
// speed throttling.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mathew/meridian-stream/internal/almanac/codec"
	"github.com/mathew/meridian-stream/internal/almanac/kafka"
	"github.com/mathew/meridian-stream/internal/almanac/metrics"
	mparquet "github.com/mathew/meridian-stream/internal/almanac/parquet"
	"github.com/mathew/meridian-stream/internal/almanac/schema"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	var (
		dateFrom    = flag.String("from", "", "Start date (YYYY-MM-DD, inclusive)")
		dateTo      = flag.String("to", "", "End date (YYYY-MM-DD, inclusive)")
		rateLimit   = flag.Int("rate", 0, "Max events/sec (0 = unlimited)")
		dryRun      = flag.Bool("dry-run", false, "Print events without publishing")
		prefix      = flag.String("prefix", "dt=", "MinIO object prefix filter")
	)
	flag.Parse()

	brokers := getEnv("KAFKA_BROKERS", "localhost:19092")
	topic := getEnv("KAFKA_TOPIC", "recentchanges")
	schemaRegistryURL := getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
	minioEndpoint := getEnv("MINIO_ENDPOINT", "localhost:9000")
	minioBucket := getEnv("MINIO_BUCKET", "events")
	minioAccess := getEnv("MINIO_ACCESS_KEY", "minioadmin")
	minioSecret := getEnv("MINIO_SECRET_KEY", "minioadmin")
	metricsAddr := getEnv("METRICS_ADDR", ":8083")

	metricsSrv := metrics.ServeMetrics(metricsAddr)
	defer metricsSrv.Close()
	metrics.Up.Set(1)

	mc, err := minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioAccess, minioSecret, ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalf("minio client: %v", err)
	}

	sc := schema.NewClient(schemaRegistryURL)
	cc := codec.NewCodec(sc, avroSchema)
	if err := cc.Register(topic + "-value"); err != nil {
		log.Fatalf("schema registration: %v", err)
	}

	prod := kafka.NewProducer([]string{brokers}, topic)
	defer prod.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down")
		cancel()
	}()

	// Build prefix filters from date range.
	prefixes := buildPrefixes(*prefix, *dateFrom, *dateTo)
	if len(prefixes) == 0 {
		// Replay all objects under the given prefix.
		prefixes = []string{*prefix + "/"}
	}

	var totalEvents atomic.Int64
	var totalFiles atomic.Int64
	startTime := time.Now()

	for _, pf := range prefixes {
		if ctx.Err() != nil {
			break
		}
		log.Printf("scanning prefix %q", pf)

		keys, err := mparquet.ListObjects(ctx, mc, minioBucket, pf)
		if err != nil {
			log.Printf("list objects %s: %v", pf, err)
			continue
		}

		for _, key := range keys {
			if ctx.Err() != nil {
				break
			}
			if !strings.HasSuffix(key, ".parquet") {
				continue
			}
			if err := replayFile(ctx, mc, minioBucket, key, cc, prod, *dryRun, *rateLimit, &totalEvents); err != nil {
				log.Printf("replay %s: %v", key, err)
				continue
			}
			totalFiles.Add(1)
			log.Printf("replayed %s (%d events)", key, totalEvents.Load())
		}
	}

	elapsed := time.Since(startTime).Seconds()
	count := totalEvents.Load()
	var rate float64
	if elapsed > 0 {
		rate = float64(count) / elapsed
	}
	fmt.Printf("\nreplay complete: %d files, %d events in %.0fs (%.0f/s)\n",
		totalFiles.Load(), count, elapsed, rate)
}

func replayFile(
	ctx context.Context,
	mc *minio.Client, bucket, key string,
	cc *codec.Codec,
	prod *kafka.Producer,
	dryRun bool,
	rateLimit int,
	totalEvents *atomic.Int64,
) error {
	path, err := mparquet.DownloadFile(ctx, mc, bucket, key)
	if err != nil {
		return err
	}
	defer os.Remove(path)

	events, err := mparquet.ReadFile(path)
	if err != nil {
		return err
	}

	var ticker *time.Ticker
	if rateLimit > 0 {
		interval := time.Second / time.Duration(rateLimit)
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
	}

	for _, evt := range events {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if ticker != nil {
			<-ticker.C
		}

		if dryRun {
			totalEvents.Add(1)
			continue
		}

		data, err := cc.Encode(evt)
		if err != nil {
			log.Printf("encode error (event %d): %v", evt.ID, err)
			continue
		}
		if err := prod.Publish(ctx, []byte(evt.Key()), data); err != nil {
			return fmt.Errorf("publish event %d: %w", evt.ID, err)
		}
		totalEvents.Add(1)
	}

	return nil
}

func buildPrefixes(basePrefix, from, to string) []string {
	if from == "" && to == "" {
		return nil
	}
	start, err := time.Parse("2006-01-02", from)
	if err != nil {
		log.Fatalf("invalid --from date %q: %v", from, err)
	}
	end, err := time.Parse("2006-01-02", to)
	if err != nil {
		log.Fatalf("invalid --to date %q: %v", to, err)
	}
	if end.Before(start) {
		log.Fatalf("--to must be after --from")
	}

	var prefixes []string
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		prefixes = append(prefixes, fmt.Sprintf("%sdt=%s/", basePrefix, d.Format("2006-01-02")))
	}
	return prefixes
}

var avroSchema = `{
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

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
