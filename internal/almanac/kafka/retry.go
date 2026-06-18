package kafka

import (
	"fmt"
	"sync"

	"github.com/segmentio/kafka-go"
)

// RetryState tracks per-message retry counts using a topic/partition/offset key.
type RetryState struct {
	mu   sync.Mutex
	data map[string]int
}

// NewRetryState creates an empty RetryState.
func NewRetryState() *RetryState {
	return &RetryState{data: make(map[string]int)}
}

func retryKey(msg kafka.Message) string {
	return fmt.Sprintf("%s/%d/%d", msg.Topic, msg.Partition, msg.Offset)
}

// Next returns the current retry count for the message and increments it.
func (rs *RetryState) Next(msg kafka.Message) int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	key := retryKey(msg)
	count := rs.data[key]
	rs.data[key] = count + 1
	return count
}

// Reset clears the retry state for a successfully processed message.
func (rs *RetryState) Reset(msg kafka.Message) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	delete(rs.data, retryKey(msg))
}


