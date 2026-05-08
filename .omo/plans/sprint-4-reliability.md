# sprint-4-reliability — Work Plan

## TL;DR (For humans)

**What you'll get:** The pipeline stops losing messages. Failed events go to a dead-letter queue instead of being silently dropped. The consumer survives crashes cleanly, drains in-flight work before shutting down, and checks that Redpanda/MinIO are healthy on startup. If the batch writer to MinIO falls behind, it pushes back on the consumer to slow down instead of letting memory grow unbounded. The Avro schema gains a version 2 with an optional field while staying backwards-compatible.

**Why this approach:** Every component tackles a specific failure mode we've seen in the current code. DLQ recovers messages instead of dropping them. Backpressure at the batcher prevents OOM. Startup checks fail fast instead of silently corrupting data. Schema evolution proves the system can handle schema changes without downtime. These four together make the pipeline production-ready.

**What it will NOT do:** No metrics dashboards (Sprint 3), no load-testing framework (Sprint 5), no Kubernetes (Sprint 6). No circular-buffer DLQ — Kafka topics are sufficient. No custom rebalance logic — kafka-go handles consumer groups.

**Effort:** Large
**Risk:** Medium — DLQ and backpressure touch the hot path; graceful shutdown must not deadlock
**Decisions to sanity-check:** DLQ retry count (3), backpressure limit (10000 events), schema compatibility mode (BACKWARD)

Your next move: Approve, then execute `$start-work` to begin implementation.

---

> TL;DR (machine): Large effort, Medium risk — DLQ + Recovery + Backpressure + Schema Evolution for v0.5.0

## Scope
### Must have
1. DLQ: DLQ producer with header-encoded error context, consumer error routing with retry (3x) → DLQ, standalone replay utility
2. Recovery: Graceful shutdown drain (signal→flush batcher→close consumer), startup health checks (Redpanda, Schema Registry, MinIO) with retry backoff (5s interval, 6 attempts)
3. Backpressure: Batcher.MaxPending (default 10000) with blocking Write(), consumer backpressure propagation
4. Schema Evolution: Schema v2 with optional "minor" field (int, default 0), Schema Registry BACKWARD compatibility mode config, v1→v2 decode test
5. Required QA tests (per AGENTS.md): Consumer crash recovery, Broker restart recovery, Schema compatibility, DLQ routing, Backpressure handling

### Must NOT have (guardrails, anti-slop, scope boundaries)
- No Prometheus/Grafana/metrics changes (Sprint 3)
- No lag monitoring or alerting
- No replay system (Sprint 5)
- No benchmark suite or k6 (Sprint 5)
- No Kubernetes, Flink, Feast (Sprint 6)
- No file-based DLQ — use Kafka topics
- No custom rebalance handler — kafka-go manages this
- No changes to the SSE reader or producer main loop
- No changes to the Parquet sink write path (only add blocking to batcher)
- Do NOT modify `internal/almanac/kafka/producer.go` beyond adding the DLQ topic writer
- Do NOT add new external dependencies — use only `segmentio/kafka-go` headers, context, stdlib

## Verification strategy
> Zero human intervention — all verification is agent-executed.
- Test decision: Tests-after for most items; TDD for DLQ routing test
- Framework: `go test -short -count=1 ./...`
- Evidence directory: `.omo/evidence/sprint-4/`

## Execution strategy
### Parallel execution waves
- **Wave 1**: DLQ core (dlq.go + retry helper + tests) — independent
- **Wave 2**: Backpressure (batcher MaxPending + blocking Write + tests) — independent
- **Wave 3**: Schema Evolution (v2 schema, compatibility config, evolution test) — independent
- **Wave 4**: Recovery (graceful shutdown, health checks) + wire DLQ + backpressure into consumer main.go — depends on Waves 1-3
- **Wave 5**: QA integration tests + DLQ replay utility + CHANGELOG — depends on Wave 4

### Dependency matrix
| Todo | Depends on | Blocks | Can parallelize with |
| --- | --- | --- | --- |
| 1. DLQ producer + header encoding | — | 4, 5, 6 | 2, 3 |
| 2. Consumer error routing + retry → DLQ | 1 | 4, 5 | 3 |
| 3. Batcher MaxPending backpressure | — | 4 | 1, 2 |
| 4. Schema v2 + compatibility config | — | 5 | 1, 2, 3 |
| 5. Graceful shutdown + health checks | — | 6 | 1, 2, 3, 4 |
| 6. Wire DLQ + backpressure into consumer main.go | 1, 2, 3 | 7 | 5 |
| 7. DLQ replay utility | 1 | 8 | 6 |
| 8. QA integration tests | 1, 2, 3, 4, 5, 6, 7 | 9 | — |
| 9. CHANGELOG + final verification | 8 | — | — |

## Todos

- [ ] 1. DLQ: Create internal/almanac/kafka/dlq.go — DLQ producer with error header encoding
  Main files: `internal/almanac/kafka/dlq.go`, `internal/almanac/kafka/dlq_test.go`
  
  **What to do:**
  - Create `type DLQWriter struct` wrapping a `*kafka.Writer` pointed at the DLQ topic
  - Constructor: `NewDLQWriter(brokers []string, topic string) *DLQWriter` — defaults to `{mainTopic}-dlq`
  - Method: `WriteFailed(ctx context.Context, originalMsg kafka.Message, err error) error`
    - Encodes original key/value as-is
    - Adds kafka.Message.Headers: `x-error-type`, `x-error-message`, `x-original-topic`, `x-retry-count` (always "0" for first DLQ write), `x-original-timestamp`
    - Uses `kafka.Writer` with `RequiredAcks: kafka.RequireAll`, `Async: false`, `BatchTimeout: 10ms`
  - Create `type RetryPolicy struct { MaxRetries int }` with default 3
  - Helper: `ShouldDLQ(retryCount int, policy RetryPolicy) bool` — returns true when `retryCount >= MaxRetries`
  - Method: `Close() error` to close the underlying writer
  - Must NOT import models.go or almanac package — operates on raw Kafka messages
  - Must NOT add new external dependencies
  
  **What to NOT do:**
  - Do NOT modify existing producer.go or consumer.go in this task
  
  **Test file:** `internal/almanac/kafka/dlq_test.go`
  - `TestDLQWriterClose` — verify Close succeeds on nil writer (safe)
  - `TestDLQMessageHeaders` — construct a DLQ message manually, verify headers contain expected key/value pairs
  - `TestShouldDLQ` — test retry count thresholds: 0 → false, 2 → false, 3 → true, 5 → true
  - `TestNewDLQWriterDefaults` — verify topic name convention
  
  **Acceptance criteria:** `go test -short -count=1 ./internal/almanac/kafka/...` passes
  **Commit:** Y | `feat(dlq): add DLQ writer with error header encoding`

- [ ] 2. DLQ: Add retry helper + route logic in consumer error path
  Main files: `internal/almanac/kafka/retry.go`, `internal/almanac/kafka/retry_test.go`
  
  **What to do:**
  - Create `internal/almanac/kafka/retry.go`
  - `type RetryState struct` — tracks `retryCount int` per message (identified by key hash or offset)
  - Method `Next(msg kafka.Message) int` — returns current count, increments
  - Method `Reset(msg kafka.Message)` — clears retry state for a successfully processed message
  - Internal map `map[string]int` keyed by `topic/partition/offset` with mutex
  - Create `func ExtractErrorFromHeaders(headers []kafka.Header) string` — reads `x-error-type` header from DLQ messages (for replay tool)
  - Must NOT depend on any external package beyond stdlib + kafka-go
  
  **What to NOT do:**
  - Do NOT wire into consumer main.go yet (that's a later task)
  
  **Test file:** `internal/almanac/kafka/retry_test.go`
  - `TestRetryStateIncrement` — call Next 3 times, expect 0, 1, 2
  - `TestRetryStateReset` — increment then reset, verify Next returns 0
  - `TestExtractErrorHeaders` — construct headers with x-error-type, verify extraction
  - `TestExtractErrorHeadersMissing` — headers without x-error-type return empty string
  
  **Acceptance criteria:** `go test -short -count=1 ./internal/almanac/kafka/...` passes
  **Commit:** Y | `feat(dlq): add retry state tracking and header extraction helpers`

- [ ] 3. Backpressure: Add MaxPending to batcher with blocking Write
  Main files: `internal/almanac/sink/batch.go`, `internal/almanac/sink/batch_test.go`
  
  **What to do:**
  - Add `MaxPending int` field to `BatchConfig` (default 0 = unlimited / no backpressure)
  - Add `pendingCh chan struct{}` to `Batcher` struct — buffered channel with capacity `MaxPending`
  - In `Write()`: after appending, try to push to `pendingCh`:
    - If `MaxPending == 0`, skip (backpressure disabled)
    - `select { case b.pendingCh <- struct{}{}: default: }` — if channel is full (buffer at capacity), block until space opens
  - In `Flush()`: drain `pendingCh` by the number of rows flushed
    - `for i := 0; i < len(batch); i++ { <-b.pendingCh }` (non-blocking, channel has exactly this many items)
  - Must NOT change the Sink interface or FileWriter interface
  - Must NOT add new dependencies
  
  **What to NOT do:**
  - Do NOT wire backpressure into consumer main.go yet
  - Do NOT modify ParquetSink or Sink interface
  
  **Test updates in `batch_test.go`:**
  - `TestBatcherBackpressureBlocks` — create batcher with MaxPending=2, write 3 events in a goroutine, measure that 3rd write blocks (use `select` with timeout). Then flush, verify 3rd write unblocks.
  - `TestBatcherBackpressureDisabled` — MaxPending=0, write 1000 events rapidly, verify all succeed without blocking (no deadlock)
  - `TestBatcherMaxPendingFlushDrains` — write 5 events with MaxPending=5, flush, verify pendingCh is drained (len=0)
  
  **Acceptance criteria:** `go test -short -count=1 ./internal/almanac/sink/...` passes
  **Commit:** Y | `feat(sink): add MaxPending backpressure to batcher with blocking Write`

- [ ] 4. Schema Evolution: Create v2 schema + compatibility config + evolution test
  Main files: `schemas/change_event_v2.avsc`, `internal/almanac/schema/client.go`, `internal/almanac/schema/client_test.go`, `internal/almanac/codec/avro.go`, `internal/almanac/codec/avro_test.go`
  
  **What to do:**
  - Create `schemas/change_event_v2.avsc`:
    - Copy of v1 schema
    - Add field `{"name": "minor", "type": "int", "default": 0, "doc": "Schema version minor revision indicator"}`
    - Add field `{"name": "page_id", "type": ["null", "long"], "default": null, "doc": "Wikimedia page ID (nullable, added in v2)"}`
  - Update `internal/almanac/codec/avro.go`:
    - Add `Minor int `avro:"minor"`` and `PageID *int64 `avro:"page_id"`` to `avroEvent` struct
    - Update `toAvro()`: `Minor: 0, PageID: nil` (defaults)
    - Update `fromAvro()`: add `Minor: a.Minor, PageID: a.PageID` fields (existing ChangeEvent struct stays unchanged — these are transport-only metadata; only add to ChangeEvent if desired, otherwise skip)
    - Actually: do NOT change `almanac.ChangeEvent` struct. The extra fields are transport metadata only.
    - Handle nullable long: `*int64` for `page_id`
    - Update avroEvent struct fields list to match v2 schema
  - Add `SetCompatibility(subject, mode string) error` method to `schema.Client`:
    - PUT `/config/{subject}` with body `{"compatibility": "BACKWARD"}`
    - Acceptable modes: BACKWARD, FORWARD, FULL, NONE, BACKWARD_TRANSITIVE, FORWARD_TRANSITIVE, FULL_TRANSITIVE
    - Return error on non-OK response
  - The producer must still register the v1 schema (the active one); v2 is for evolution testing.
  
  **Test updates:**
  - `codec/avro_test.go`: `TestSchemaEvolutionV1toV2` — encode with v1 schema, decode with v2 schema, verify new fields get default values (minor=0, page_id=nil)
  - `codec/avro_test.go`: `TestSchemaEvolutionV2toV1` — encode with v2 schema setting minor=5, decode with v1 schema, verify no error (field dropped)
  - `schema/client_test.go`: `TestSetCompatibility` — mock PUT /config/{subject}, verify mode sent correctly
  - `schema/client_test.go`: `TestSetCompatibilityError` — mock returns 422, verify error propagated
  
  **What to NOT do:**
  - Do NOT switch the producer to v2 schema. Producer stays on v1.
  - Do NOT change `almanac.ChangeEvent` struct or models.go
  
  **Acceptance criteria:** `go test -short -count=1 ./...` passes
  **Commit:** Y | `feat(schema): add v2 Avro schema, compatibility config, evolution tests`

- [ ] 5. Recovery: Graceful shutdown + startup health checks
  Main files: `internal/almanac/kafka/recovery.go`, `internal/almanac/kafka/recovery_test.go`
  
  **What to do:**
  - Create `internal/almanac/kafka/recovery.go`:
  - `type HealthChecker struct` with `RedpandaBrokers []string`, `SchemaRegistryURL string`, `MinIOEndpoint string`
  - `func NewHealthChecker(brokers []string, srURL, minioEndpoint string) *HealthChecker`
  - `func (hc *HealthChecker) CheckAll(ctx context.Context) error`:
    - Check Redpanda: create a kafka dialer, connect to first broker, close. If fails, wrap error.
    - Check Schema Registry: HEAD request to baseURL. If fails, wrap error.
    - Check MinIO: create minio client, call BucketExists. If fails, wrap error.
    - Return combined error if any check fails.
  - `func (hc *HealthChecker) WaitForReady(ctx context.Context, maxAttempts int, interval time.Duration) error`:
    - Loop up to maxAttempts times, calling CheckAll. On success return nil. On failure, wait interval and retry.
    - If ctx cancelled, return ctx.Err().
    - Default: maxAttempts=6, interval=5s (30s total)
  
  **What to NOT do:**
  - Do NOT wire into consumer main.go yet (that's in the next task)
  - Do NOT modify any existing files
  
  **Test file:** `internal/almanac/kafka/recovery_test.go`
  - `TestHealthCheckerRedpandaFail` — HealthChecker pointing at bad address, CheckAll returns error (use short-mode skip for real checks)
  - Actually for unit tests without real infra, test the retry logic:
  - `TestWaitForReadyRetries` — create HealthChecker with mock check that fails 2 times then succeeds, verify WaitForReady succeeds on attempt 3
  - `TestWaitForReadyMaxRetries` — check always fails, verify error after maxAttempts
  - `TestWaitForReadyContextCancel` — cancel context, verify ctx.Err() returned immediately
  
  **Acceptance criteria:** `go test -short -count=1 ./internal/almanac/kafka/...` passes
  **Commit:** Y | `feat(recovery): add startup health checks with retry backoff`

- [ ] 6. Wire DLQ + backpressure + health checks into consumer main.go
  Main files: `services/consumer/main.go`
  
  **What to do:**
  - Update consumer main.go:
    1. Add env vars: `DLQ_TOPIC` (default `{topic}-dlq`), `MAX_RETRIES` (default `3`), `BACKPRESSURE_LIMIT` (default `10000`), `STARTUP_RETRY_INTERVAL` (default `5s`), `STARTUP_MAX_RETRIES` (default `6`)
    2. Create `HealthChecker` on startup, call `WaitForReady` with the configured retries. If all attempts fail, log.Fatalf.
    3. Create `Batcher` with `MaxPending: BACKPRESSURE_LIMIT`
    4. Create `DLQWriter` pointing at DLQ topic
    5. Create `RetryState` tracker
    6. In the consumer loop:
       - On decode error: increment retry state, if `ShouldDLQ` → write to DLQ (with error type "decode_error"), commit original msg, log, continue. Otherwise log+continue (will retry on re-delivery after restart/commit skip... actually we need to NOT commit on retry, only commit on success or DLQ).
       - On sink error (batcher.Write fails): increment retry state, if ShouldDLQ → write raw msg to DLQ ("sink_error"), commit original msg, log, continue. Otherwise log and continue (batcher Write failure is non-fatal, the event stays buffered).
       - Actually: on sink error, don't commit the message. The event stays in the buffer (batcher.Write might have partially added it or not). On re-delivery after restart, it reprocesses.
       - Wait, let me reconsider. batcher.Write adds to buffer, then if buffer exceeds thresholds, sends signal. If batcher.Write itself fails (which it can't currently — it always returns nil), the data is already in the buffer. So on restart, the committed offset is what matters.
       - Better approach: only commit after successful batcher.Write. If batcher.Write fails, don't commit. On re-delivery, message is re-decoded and re-buffered.
       - So the flow is: Read → decode → retry/DLQ if fail. If decode OK → batcher.Write → if fail, don't commit → message is redelivered on restart.
       - But with backpressure, batcher.Write might block. If it fails (e.g., context canceled on shutdown), we don't want to DLQ that. So check for context cancellation.
    7. On shutdown (signal):
       - Stop reading: cancel consumer context
       - Wait for in-flight batcher.Write to complete (context cancel propagates)
       - Close batcher (flushes remaining events)
       - Close consumer
  - Restructure the main loop to use a helper `processMessage(ctx, msg) error` for testability
  
  **What to NOT do:**
  - Do NOT modify the SSE reader or producer
  - Do NOT add metrics/logging beyond what already exists
  - Do NOT change the avro schema being used (producer still uses v1)
  
  **Acceptance criteria:** `go build ./...` and `go vet ./...` pass clean
  **Commit:** Y | `feat(consumer): wire DLQ, backpressure, and health checks into consumer service`

- [ ] 7. DLQ replay utility
  Main files: `services/dlq-replay/main.go`
  
  **What to do:**
  - Create `services/dlq-replay/main.go`:
    - Reads from DLQ topic as a standalone consumer group (`meridian-dlq-replay`)
    - Prints each DLQ message with its headers (error type, original topic, original timestamp)
    - Supports `--replay` flag: re-publishes DLQ messages back to the original topic (from x-original-topic header)
    - Supports `--dry-run` flag: prints what would be replayed without actually publishing
    - Configurable via env vars: `KAFKA_BROKERS`, `DLQ_TOPIC`, `KAFKA_GROUP`
    - Logs: message count, errors, replay status
    - Signal handling for graceful shutdown
  - No test file needed for this main.go (integration-only surface)
  
  **What to NOT do:**
  - Do NOT add --batch or --filter flags (keep simple)
  - Do NOT make it modify the DLQ topic (no delete support)
  
  **Acceptance criteria:** `go build ./services/dlq-replay/...` passes
  **Commit:** Y | `feat(dlq): add DLQ replay utility command`

- [ ] 8. QA integration tests
  Main files: `internal/almanac/kafka/recovery_test.go`, `internal/almanac/schema/client_test.go`, `internal/almanac/kafka/dlq_test.go`, `internal/almanac/sink/batch_test.go`
  
  **What to do:**
  Add integration-level tests for required QA scenarios (per AGENTS.md Required Tests):
  
  1. **Consumer crash recovery test** (`recovery_test.go`):
     - Integration test tagged with `//go:build integration` (skipped in short mode)
     - Requires Redpanda: start test consumer, publish messages, kill consumer (simulate crash), restart consumer, verify it resumes from last committed offset
     - For short mode: add a unit-level test `TestRecoveryOffsetTracking` that validates the consumer group re-join behavior is configured correctly (StartOffset=LastOffset, manual commit)
  
  2. **Broker restart recovery test** (`recovery_test.go`):
     - Integration test tagged with `//go:build integration`
     - Publish messages, kill broker, restart broker, verify consumer reconnects and continues
     - For short mode: test that kafka-go's ReaderConfig automatically retries on connection loss (implicit — document in test comment)
  
  3. **Schema compatibility test** (`codec/avro_test.go`):
     - Already covered by task 4: `TestSchemaEvolutionV1toV2` and `TestSchemaEvolutionV2toV1`
     - Add explicit `TestSchemaBackwardCompatibility` — register v1 schema, encode with v1, decode with v2 schema (fetched from registry), verify defaults filled
  
  4. **DLQ routing test** (`dlq_test.go`):
     - `TestDLQRoutingScenario` — simulate consumer error path: create RetryState, call ShouldDLQ after 3 retries, verify returns true
     - `TestDLQHeaderCarriesOriginalOffset` — verify x-original-topic and error-type headers are set correctly
  
  5. **Backpressure handling test** (`batch_test.go`):
     - Already covered by task 3 tests. Add `TestBackpressureDoesNotDeadlock` — rapid write+flush cycle with MaxPending, verify no goroutine leak (GOMAXPROCS=1)
  
  **What to NOT do:**
  - Do NOT require Docker for short-mode tests — all short-mode tests must pass without external infra
  - Do NOT modify build tags on existing tests (keep them short-safe)
  
  **Acceptance criteria:** `go test -short -count=1 ./...` passes with no races
  **Commit:** Y | `test(qa): add integration test stubs for crash recovery, DLQ routing, and backpressure`

- [ ] 9. CHANGELOG + final build verification
  Main files: `CHANGELOG.md`
  
  **What to do:**
  - Add v0.5.0 section to CHANGELOG.md documenting all changes
  - Follow the same format as v0.3.0
  - Run `go build ./...`, `go vet ./...`, `go test -short -count=1 -race ./...` — all must pass
  - Update AGENTS.md `Current Sprint` field from "Sprint 0" to "Sprint 4" and `Current Target` from "v0.1.0" to "v0.5.0"
  
  **Acceptance criteria:** All three commands pass cleanly
  **Commit:** Y | `chore(release): add v0.5.0 changelog and update sprint status`

## Final verification wave
> Runs in parallel after ALL todos. ALL must pass.
- [ ] F1. Plan compliance audit — verify every todo's acceptance criteria is met
- [ ] F2. Build + Vet — `go build ./...` and `go vet ./...` both clean
- [ ] F3. Test suite — `go test -short -count=1 -race ./...` all pass
- [ ] F4. Scope fidelity — confirm no Sprint 3/5/6 features leaked in

## Commit strategy
- Each todo commits independently with conventional commit format
- Squash not required — each commit is a coherent unit
- Final commit updates CHANGELOG and AGENTS.md only
- Branch: `sprint/4-reliability`

## Success criteria
1. All DLQ-produced messages include error context headers
2. Consumer startup fails fast within 30s if infra is down
3. Batcher blocks at 10000 pending events, preventing OOM
4. Schema v2 decodes cleanly from v1-encoded data
5. Graceful shutdown flushes all buffered events before exit
6. All QA required tests (AGENTS.md lines 411-421) have at least stubs
7. `go build ./...` clean, `go vet ./...` clean, all tests pass
