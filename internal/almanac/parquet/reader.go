// Package parquet provides reading utilities for ChangeEvent Parquet files
// stored in MinIO. Used by the replay system and benchmark tooling.
package parquet

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/mathew/meridian-stream/internal/almanac"
	"github.com/minio/minio-go/v7"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

// parquetRow mirrors the write-side struct for reading back Parquet rows.
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

func toEvent(r *parquetRow) *almanac.ChangeEvent {
	return &almanac.ChangeEvent{
		ID:              r.ID,
		Type:            r.Type,
		Namespace:       int(r.Namespace),
		Title:           r.Title,
		TitleURL:        r.TitleURL,
		Comment:         r.Comment,
		Timestamp:       r.Timestamp,
		User:            r.User,
		Bot:             r.Bot,
		ServerURL:       r.ServerURL,
		ServerName:      r.ServerName,
		ServerScriptURL: r.ServerScriptURL,
		Wiki:            r.Wiki,
		ParsedTimestamp: time.Unix(r.ParsedTimestamp, 0).UTC(),
	}
}

// ReadFile reads a local Parquet file and returns all ChangeEvents.
func ReadFile(path string) ([]*almanac.ChangeEvent, error) {
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(parquetRow), 4)
	if err != nil {
		return nil, fmt.Errorf("create parquet reader: %w", err)
	}
	defer pr.ReadStop()

	numRows := int(pr.GetNumRows())
	if numRows == 0 {
		return nil, nil
	}

	rows := make([]parquetRow, numRows)
	if err := pr.Read(&rows); err != nil {
		// If we read partial results, handle EOF gracefully.
		if err != io.EOF || len(rows) == 0 {
			return nil, fmt.Errorf("read parquet rows: %w", err)
		}
	}

	events := make([]*almanac.ChangeEvent, 0, len(rows))
	for i := range rows {
		events = append(events, toEvent(&rows[i]))
	}
	return events, nil
}

// ListObjects lists Parquet file keys in the given MinIO bucket prefix.
// The prefix should follow the dt=YYYY-MM-DD/hour=HH/ pattern.
func ListObjects(ctx context.Context, mc *minio.Client, bucket, prefix string) ([]string, error) {
	var keys []string
	for obj := range mc.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("list objects: %w", obj.Err)
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

// DownloadFile downloads a MinIO object to a local temp file and returns the path.
// The caller should remove the file after use.
func DownloadFile(ctx context.Context, mc *minio.Client, bucket, key string) (string, error) {
	tmpDir := filepath.Join(os.TempDir(), "meridian-replay")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return "", fmt.Errorf("create temp dir: %w", err)
	}

	localPath := filepath.Join(tmpDir, filepath.Base(key))
	if err := mc.FGetObject(ctx, bucket, key, localPath, minio.GetObjectOptions{}); err != nil {
		return "", fmt.Errorf("download %s: %w", key, err)
	}
	return localPath, nil
}
