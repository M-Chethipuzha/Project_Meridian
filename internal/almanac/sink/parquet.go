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
	"github.com/xitongsys/parquet-go/writer"
)

type ParquetSinkConfig struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	UseSSL    bool
	Region    string
}

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
	return parquetRow{ID: evt.ID, Type: evt.Type, Namespace: int32(evt.Namespace), Title: evt.Title, TitleURL: evt.TitleURL, Comment: evt.Comment, Timestamp: evt.Timestamp, User: evt.User, Bot: evt.Bot, ServerURL: evt.ServerURL, ServerName: evt.ServerName, ServerScriptURL: evt.ServerScriptURL, Wiki: evt.Wiki, ParsedTimestamp: evt.ParsedTimestamp.Unix()}
}

type ParquetSink struct {
	mc     *minio.Client
	bucket string
	localDir string
}

func NewParquetSink(cfg ParquetSinkConfig) (*ParquetSink, error) {
	mc, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil { return nil, fmt.Errorf("minio client: %w", err) }
	localDir := filepath.Join(os.TempDir(), "meridian-parquet")
	os.MkdirAll(localDir, 0755)
	return &ParquetSink{mc: mc, bucket: cfg.Bucket, localDir: localDir}, nil
}

func (s *ParquetSink) WriteFile(rows []parquetRow, ts time.Time) error {
	if len(rows) == 0 { return nil }
	hour := ts.Format("15")
	key := fmt.Sprintf("dt=%s/hour=%s/events-%d.parquet", ts.Format("2006-01-02"), hour, ts.UnixNano())
	fpath := filepath.Join(s.localDir, filepath.Base(key))
	fw, err := local.NewLocalFileWriter(fpath)
	if err != nil { return fmt.Errorf("create local file: %w", err) }
	pw, err := writer.NewParquetWriter(fw, new(parquetRow), 1)
	if err != nil { fw.Close(); return fmt.Errorf("create parquet writer: %w", err) }
	for i := range rows {
		if err := pw.Write(rows[i]); err != nil { fw.Close(); return fmt.Errorf("write row %d: %w", i, err) }
	}
	if err := pw.WriteStop(); err != nil { fw.Close(); return err }
	fw.Close()
	_, err = s.mc.FPutObject(context.Background(), s.bucket, key, fpath, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil { return fmt.Errorf("upload to minio: %w", err) }
	os.Remove(fpath)
	log.Printf("parquet: wrote %d rows -> s3://%s/%s", len(rows), s.bucket, key)
	return nil
}

func (s *ParquetSink) Close() error { return os.RemoveAll(s.localDir) }
