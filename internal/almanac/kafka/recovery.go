package kafka

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type HealthChecker struct {
	brokers        []string
	schemaRegistry string
	minioEndpoint  string
	minioAccess    string
	minioSecret    string
	minioBucket    string
	minioUseSSL    bool
}

func NewHealthChecker(brokers []string, schemaRegistry, minioEndpoint, minioAccess, minioSecret, minioBucket string, minioUseSSL bool) *HealthChecker {
	return &HealthChecker{brokers: brokers, schemaRegistry: schemaRegistry, minioEndpoint: minioEndpoint, minioAccess: minioAccess, minioSecret: minioSecret, minioBucket: minioBucket, minioUseSSL: minioUseSSL}
}

func (h *HealthChecker) WaitForReady(ctx context.Context, maxRetries int, interval time.Duration) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		select {
		case <-ctx.Done(): return ctx.Err()
		default:
		}
		if err := h.checkAll(); err != nil { lastErr = err; time.Sleep(interval); continue }
		return nil
	}
	return fmt.Errorf("health check failed after %d retries: %w", maxRetries, lastErr)
}

func (h *HealthChecker) checkAll() error {
	// Check Schema Registry
	resp, err := http.Get(h.schemaRegistry + "/subjects")
	if err != nil { return fmt.Errorf("schema registry: %w", err) }
	resp.Body.Close()

	// Check MinIO
	mc, err := minio.New(h.minioEndpoint, &minio.Options{Creds: credentials.NewStaticV4(h.minioAccess, h.minioSecret, ""), Secure: h.minioUseSSL})
	if err != nil { return fmt.Errorf("minio client: %w", err) }
	if _, err := mc.ListBuckets(context.Background()); err != nil { return fmt.Errorf("minio: %w", err) }
	return nil
}
