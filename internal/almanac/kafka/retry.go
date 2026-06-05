package kafka

import (
	"fmt"
	"sync"

	"github.com/segmentio/kafka-go"
)

type RetryState struct {
	mu    sync.Mutex
	count map[string]int
}

func NewRetryState() *RetryState { return &RetryState{count: make(map[string]int)} }

func retryKey(msg kafka.Message) string { return fmt.Sprintf("%s/%d/%d", msg.Topic, msg.Partition, msg.Offset) }

func (r *RetryState) Next(msg kafka.Message) int {
	r.mu.Lock(); defer r.mu.Unlock()
	key := retryKey(msg)
	r.count[key]++
	return r.count[key]
}

func (r *RetryState) Reset(msg kafka.Message) {
	r.mu.Lock(); defer r.mu.Unlock()
	delete(r.count, retryKey(msg))
}

func ShouldDLQ(retryCount, maxRetries int) bool { return retryCount >= maxRetries }
