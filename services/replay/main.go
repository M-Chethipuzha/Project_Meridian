// Command replay reads stored Parquet files from MinIO and re-publishes to Redpanda.
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
	dateFrom := flag.String("from", "", "Start date (YYYY-MM-DD)")
	dateTo := flag.String("to", "", "End date (YYYY-MM-DD)")
	rateLimit := flag.Int("rate", 0, "Max events/s (0=unlimited)")
	dryRun := flag.Bool("dry-run", false, "Print without publishing")
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

	mc, _ := minio.New(minioEndpoint, &minio.Options{Creds: credentials.NewStaticV4(minioAccess, minioSecret, ""), Secure: false})
	sc := schema.NewClient(schemaRegistryURL)
	cc := codec.NewCodec(sc, avroSchema)
	cc.Register(topic + "-value")

	prod := kafka.NewProducer([]string{brokers}, topic)
	defer prod.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigCh; cancel() }()

	prefixes := buildPrefixes("dt=", *dateFrom, *dateTo)
	if len(prefixes) == 0 { prefixes = []string{"dt=/"} }

	var totalEvents atomic.Int64
	startTime := time.Now()
	for _, pf := range prefixes {
		keys, err := mparquet.ListObjects(ctx, mc, minioBucket, pf)
		if err != nil { continue }
		for _, key := range keys {
			if !strings.HasSuffix(key, ".parquet") { continue }
			replayFile(ctx, mc, minioBucket, key, cc, prod, *dryRun, *rateLimit, &totalEvents)
		}
	}
	fmt.Printf("\nreplay: %d events in %.0fs\n", totalEvents.Load(), time.Since(startTime).Seconds())
}

func replayFile(ctx context.Context, mc *minio.Client, bucket, key string, cc *codec.Codec, prod *kafka.Producer, dryRun bool, rateLimit int, total *atomic.Int64) {
	path, _ := mparquet.DownloadFile(ctx, mc, bucket, key)
	defer os.Remove(path)
	events, _ := mparquet.ReadFile(path)
	for _, evt := range events {
		if !dryRun { data, _ := cc.Encode(evt); prod.Publish(ctx, []byte(evt.Key()), data) }
		total.Add(1)
	}
}

func buildPrefixes(base, from, to string) []string { return nil }

var avroSchema = `{"type":"record","name":"ChangeEvent","namespace":"meridian","fields":[{"name":"id","type":"long"},{"name":"type","type":"string"},{"name":"namespace","type":"int"},{"name":"title","type":"string"},{"name":"title_url","type":"string"},{"name":"comment","type":"string"},{"name":"timestamp","type":"long"},{"name":"user","type":"string"},{"name":"bot","type":"boolean"},{"name":"server_url","type":"string"},{"name":"server_name","type":"string"},{"name":"server_script_url","type":"string"},{"name":"wiki","type":"string"},{"name":"parsed_timestamp","type":"long"}]}`

func getEnv(k, f string) string { if v := os.Getenv(k); v != "" { return v }; return f }
