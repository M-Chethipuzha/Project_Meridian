package sink

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/mathew/meridian-stream/internal/almanac"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// ParquetSinkConfig holds configuration for the Parquet sink.
type ParquetSinkConfig struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	UseSSL    bool
	Region    string
	LocalDir  string
}

// parquetRow mirrors ChangeEvent fields as Parquet-compatible Go types.
type parquetRow struct {
	ID              int64  `parquet:"name=id, type=INT64"`
	Type            string `parquet:"name=type, type=BYTE_ARRAY, convertedtype=UTF8"`
	Namespace       int32  `parquet:"name=namespace, type=INT32"`
	Title           string `parquet:"name=title, type=BYTE_ARRAY, convertedtype=UTF8"`
	TitleURL        string `parquet:"name=title_url, type=BYTE_ARRAY, convertedtype=UTF8"`
	Comment         string `parquet:"name=comment, type=BYTE_ARRAY, convertedtype=UTF8"`
	Timestamp       int64  `parquet:"name=timestamp, type=INT64"`
	User            string `parquet:"name=user, type=BYTE_ARRAY, convertedtype=UTF8"`
	Bot             bool   `parquet:"name=bot, type=BOOLEAN"`
	ServerURL       string `parquet:"name=server_url, type=BYTE_ARRAY, convertedtype=UTF8"`
	ServerName      string `parquet:"name=server_name, type=BYTE_ARRAY, convertedtype=UTF8"`
	ServerScriptURL string `parquet:"name=server_script_url, type=BYTE_ARRAY, convertedtype=UTF8"`
	Wiki            string `parquet:"name=wiki, type=BYTE_ARRAY, convertedtype=UTF8"`
	ParsedTimestamp int64  `parquet:"name=parsed_timestamp, type=INT64"`
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

// FileWriter writes batches of parquetRows to Parquet files in MinIO.
type FileWriter interface {
	WriteFile(rows []parquetRow, ts time.Time) error
}

// ParquetSink writes batched ChangeEvents as Parquet files partitioned by
// dt=YYYY-MM-DD/hour=HH/ and uploads them to MinIO.
type ParquetSink struct {
	mc          *minio.Client
	bucket      string
	localDir    string
	compression parquet.CompressionCodec
}

var _ FileWriter = (*ParquetSink)(nil)

// NewParquetSink creates a new ParquetSink connected to the configured MinIO endpoint.
func NewParquetSink(cfg ParquetSinkConfig) (*ParquetSink, error) {
	mc, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("create minio client: %w", err)
	}

	ctx := context.Background()
	exists, err := mc.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("bucket %q does not exist; create it first", cfg.Bucket)
	}

	localDir := cfg.LocalDir
	if localDir == "" {
		localDir = filepath.Join(os.TempDir(), "meridian-parquet")
	}
	if err := os.MkdirAll(localDir, 0755); err != nil {
		return nil, fmt.Errorf("create local dir: %w", err)
	}

	return &ParquetSink{
		mc:          mc,
		bucket:      cfg.Bucket,
		localDir:    localDir,
		compression: parquet.CompressionCodec_SNAPPY,
	}, nil
}

// WriteFile writes a batch of rows to a time-partitioned Parquet file and uploads it to MinIO.
func (s *ParquetSink) WriteFile(rows []parquetRow, ts time.Time) error {
	if len(rows) == 0 {
		return nil
	}

	dt := ts.Format("2006-01-02")
	hour := ts.Format("15")
	now := time.Now().UnixNano()
	name := fmt.Sprintf("%s_%d.parquet", ts.Format("150405"), now)
	key := fmt.Sprintf("dt=%s/hour=%s/%s", dt, hour, name)

	fpath := filepath.Join(s.localDir, name)
	fw, err := local.NewLocalFileWriter(fpath)
	if err != nil {
		return fmt.Errorf("create local file: %w", err)
	}

	pw, err := writer.NewParquetWriter(fw, new(parquetRow), 4)
	if err != nil {
		fw.Close()
		return fmt.Errorf("create parquet writer: %w", err)
	}
	pw.CompressionType = s.compression
	pw.RowGroupSize = 128 * 1024 * 1024
	pw.PageSize = 8 * 1024

	for i := range rows {
		if err := pw.Write(rows[i]); err != nil {
			fw.Close()
			return fmt.Errorf("write row %d: %w", i, err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		fw.Close()
		return fmt.Errorf("stop parquet writer: %w", err)
	}
	fw.Close()

	_, err = s.mc.FPutObject(context.Background(), s.bucket, key, fpath, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		os.Remove(fpath)
		return fmt.Errorf("upload to minio: %w", err)
	}

	os.Remove(fpath)
	log.Printf("parquet: wrote %d rows -> s3://%s/%s", len(rows), s.bucket, key)
	return nil
}

// Close cleans up the local temp directory.
func (s *ParquetSink) Close() error {
	return os.RemoveAll(s.localDir)
}
