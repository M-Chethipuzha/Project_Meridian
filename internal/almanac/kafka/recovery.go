package kafka

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/segmentio/kafka-go"
)

// HealthChecker performs startup health checks against pipeline dependencies.
type HealthChecker struct {
	RedpandaBrokers   []string
	SchemaRegistryURL string
	MinIOEndpoint     string
	MinIOAccessKey    string
	MinIOSecretKey    string
	MinIOBucket       string
	MinioUseSSL       bool
}

// NewHealthChecker creates a HealthChecker for the given dependency endpoints.
func NewHealthChecker(brokers []string, srURL, minioEndpoint, minioAccessKey, minioSecretKey, minioBucket string, useSSL bool) *HealthChecker {
	return &HealthChecker{
		RedpandaBrokers:   brokers,
		SchemaRegistryURL: srURL,
		MinIOEndpoint:     minioEndpoint,
		MinIOAccessKey:    minioAccessKey,
		MinIOSecretKey:    minioSecretKey,
		MinIOBucket:       minioBucket,
		MinioUseSSL:       useSSL,
	}
}

// CheckAll checks all dependencies and returns a combined error if any fail.
func (hc *HealthChecker) CheckAll(ctx context.Context) error {
	var errs []error

	if err := hc.checkRedpanda(ctx); err != nil {
		errs = append(errs, fmt.Errorf("redpanda: %w", err))
	}
	if err := hc.checkSchemaRegistry(ctx); err != nil {
		errs = append(errs, fmt.Errorf("schema registry: %w", err))
	}
	if err := hc.checkMinIO(ctx); err != nil {
		errs = append(errs, fmt.Errorf("minio: %w", err))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (hc *HealthChecker) checkRedpanda(ctx context.Context) error {
	conn, err := kafka.DialContext(ctx, "tcp", hc.RedpandaBrokers[0])
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

func (hc *HealthChecker) checkSchemaRegistry(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, hc.SchemaRegistryURL, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (hc *HealthChecker) checkMinIO(ctx context.Context) error {
	client, err := minio.New(hc.MinIOEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(hc.MinIOAccessKey, hc.MinIOSecretKey, ""),
		Secure: hc.MinioUseSSL,
	})
	if err != nil {
		return err
	}

	exists, err := client.BucketExists(ctx, hc.MinIOBucket)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("bucket %q does not exist", hc.MinIOBucket)
	}
	return nil
}

// WaitForReady polls CheckAll until it succeeds, maxAttempts is reached, or the
// context is cancelled. Returns nil on first success.
func (hc *HealthChecker) WaitForReady(ctx context.Context, maxAttempts int, interval time.Duration) error {
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := hc.CheckAll(ctx); err == nil {
			return nil
		} else if attempt < maxAttempts {
			time.Sleep(interval)
		} else {
			return fmt.Errorf("health check failed after %d attempts: %w", maxAttempts, err)
		}
	}
	return nil
}
