# Production Infrastructure: Design Spec

Date: 2026-09-25 (revision 2, 2026-09-26)
Status: approved in brainstorming; revision 2 applies an independent principal-architect review (2 Critical, 14 Important, 15 Minor findings); awaiting written review
Builds on: `docs/superpowers/specs/2026-09-23-abandoned-cart-recovery-design.md` and the merged in-memory implementation

## 1. Purpose

Evolve the in-memory case-study pipeline into separately deployable roles running on real Kafka, Redis, and DynamoDB, and measure it under load, while keeping the in-memory mode as the zero-Docker path for reviewers.

Success means:

1. `./gradlew test` and `./gradlew run` behave as today with only a JDK: unit tests, the fake-clock verifier, and the demo.
2. `docker compose up --build` with only Docker runs the full stack with multiple replicas per role.
3. Contract tests prove the in-memory and infra adapters behave identically, and infra end-to-end tests prove wiring and failure semantics on real containers.
4. A load test reports achieved throughput, per-stage lag, send latency per lane, and zero duplicate or post-purchase sends at the sink, and names the local bottleneck honestly.
5. Every invariant in section 3 has a named mechanism and a test.

## 2. Decisions

| Decision | Choice | Reason |
|---|---|---|
| Scope | Separately deployable roles on real infra, plus a load test | Shows the design running, not only described. |
| Framework | Plain Java 21 with official clients (`kafka-clients`, Lettuce, AWS SDK v2, Jackson) | Core classes already depend only on ports; the scored mechanics stay visible; smallest diff. |
| Packaging | One application and one image, role selected with `--role` | One build, one image, least code. |
| Worker to dispatcher | Kafka intent topics, split into a fast lane (early offsets) and a slow lane (the 24 hour offset) | Decouples scheduling from sending; the lanes keep a next-day burst of 24 hour reminders from starving the higher-value early reminders (DESIGN §9). |
| Dedupe point | A fenced lease in the dispatcher's ledger row, taken for every send attempt including retries | Dedupe where the side effect happens; the fencing token makes a stale holder harmless. |
| Lateness | One decision point: the scheduler stamps `sendBy` into the intent, the dispatcher enforces it | Retries and replays need the check anyway; a stamped deadline cannot drift with replica config. |
| Cancellation freshness | Scheduler and dispatcher are gated on the detector's event-time watermark and pause, never send, while it lags | A consistent cart read cannot see a purchase still sitting in Kafka; the watermark bounds that gap. |
| Durability rule | Redis holds only rebuildable indexes and the watermark; DynamoDB and Kafka are the sources of truth | Losing Redis loses time, not work. |
| Retry scheduling | The ledger's sparse retry index, polled directly; no Redis retry queue | Retry rows are rare; one fewer structure and no rebuild path. |
| Cart writes | Field-scoped conditional `UpdateItem`, no read-modify-write in the detector | Removes the lost-update race on `sequenceStarts` and the detector's read and conflict loop. |
| Timer store | Redis, not DynamoDB-embedded timers | Cheaper per upsert at 5k to 12.5k per second; its dual write with the cart write is covered by monotonic upserts and the reconciler. |
| Partition key | `cartId` for every topic | State, version, timers, and ledger are per cart; guest login changes the shopper key, not the cart. A future per-shopper cap uses a per-shopper counter item, not re-keying. |
| Load test | Measure on local containers and report the bottleneck honestly | A laptop cannot sustain 12.5k per second through DynamoDB Local; the design does not change to beat an emulator. |
| Concurrency | Per-batch processing on virtual threads, grouped by `cartId` in the consumers | Serial per-partition processing caps at about 64 × 1/15 ms ≈ 4.3k per second, below 12.5k (Little's law); grouping keeps per-cart order. |

## 3. Invariants

| Invariant | Mechanism | Test |
|---|---|---|
| No send after purchase beyond a bounded window (`MAX_DETECTOR_LAG + CLOCK_SKEW +` one gateway round trip) | Watermark gate on scheduler and dispatcher (§5.4), consistent cart re-check immediately before send, token taken before the check | Verifier "lagging detector holds the reminder"; e2e 7; load test post-purchase = 0 at the sink |
| No send after `sendBy` except for the in-flight gateway call | `sendBy` stamped by the scheduler, checked after the token and claim | Verifier 11, 9b; dispatcher unit tests |
| At most one send per key, barring a lease holder paused past its lease mid-gateway-call (covered by the gateway idempotency key) | Conditional ledger claim with a fencing token for every attempt; every transition conditioned on the token | Ledger contract tests (takeover, stale token rejected); e2e 4, 5, 6; load test duplicates = 0 at the sink |
| No lost reminder on any single crash | Timer lease redelivery; redelivered `CHECK_ABANDON` on an abandoned cart re-arms `REMINDER 0`; every non-final ledger row is in the retry index with `nextAttemptAt = leaseUntil`; outcome and DLQ records produced before `finish`; reconciler | Contract tests for lease expiry; scheduler and dispatcher crash-point unit tests; e2e 6 |
| Redis loss loses no work | Watermark missing means lagging (pause); timers rebuilt by the reconciler immediately on a missing epoch sentinel; no work lives only in Redis | e2e 6; runbook drill |
| Per-cart state never regresses | Detector writes conditioned on `version < :v`; scheduler writes conditioned on `version = :v AND status = <expected>` | Cart store contract tests |
| Frequency cap holds | Only the scheduler writes `sequenceStarts`, conditioned on version and status | Verifier 14; contract test for concurrent detector write |
| Timer index never regresses | Upsert only if `(version, offset)` is greater; equal data is a no-op that keeps the lease; remove only if stored version ≤ given version | Timer store contract tests |
| A final ledger status stays final | All transitions conditioned on `status = SENDING AND leaseToken = :mine` | Ledger contract tests |
| Every `DEAD` row is replayable | DLQ record produced before `finish(DEAD)` | Dispatcher crash-point unit test |
| Holdout carts are never sent | Arm fixed on first write; policy checked on abandonment and on re-arm | Verifier 12 |
| All replicas agree on shard and partition counts | `init` persists them; roles refuse to start on a mismatch | Startup unit test |

## 4. Architecture

```
                 cart-events (keyed by cartId)
Cart Service ───────────────► [detector] ──► DynamoDB carts (conditional UpdateItem, version < :v)
 (loadgen stands in)               │    └──► Redis timers (monotonic upsert, conditional remove)
                                   └──► Redis watermarks (event time per partition, every second)
                                                    │
                      [scheduler] ◄── claimDue(upTo = watermark − skew), ack if unchanged
                           │ consistent reload + version/status compare, stamp sendBy
                           ▼
            reminder-intents-fast / reminder-intents-slow (keyed by cartId)
                           │
                      [dispatcher] ── paused while watermark lags, breaker open, or guardrail paused
                           │  token → fenced claim → sendBy? → consistent cart re-check → send
                           │  outcomes (and DLQ on death) produced before the ledger finish
                           │  transient → ledger RETRYING → retry loop polls the ledger retry index
                           └── reminder-dlq ◄── [replay] reopens ledger rows
                      [reconciler] ── on interval and on Redis loss: open carts vs Redis, rebuild missing timers
                      [loadgen]    ── produces events at a rate, reads sink-sends and outcomes, writes a report
```

### Roles

| Role | Job | Scales by |
|---|---|---|
| `init` | Create topics, tables, indexes, and the meta item; idempotent | One-off |
| `detector` | Consume `cart-events`, apply conditional updates, maintain timers, publish the watermark | Partitions and concurrency |
| `scheduler` | Claim due timers from all shards up to the watermark, run `ReminderScheduler`, publish intents, ack | Instances; claims are atomic |
| `dispatcher` | Consume both intent lanes, run `Dispatcher.handle`, run the retry loop | Partitions and concurrency |
| `reconciler` | Rebuild missing timers | One instance; a second is harmless |
| `replay` | Read `reminder-dlq`, reopen ledger rows | One-off |
| `loadgen` | Generate workload, observe sends and outcomes, write the report | One-off |

`--mode=inmemory` is the default and runs the existing fake-clock demo. Infra roles use `SystemClock`.

## 5. Data model

Conventions: epoch-millisecond timestamps, JSON payloads (Jackson, `FAIL_ON_UNKNOWN_PROPERTIES` off, every payload carries `schemaVersion`). `S` shards and `P` partitions are configurable (64 production, 8 local) and persisted by `init`. `shard(cartId) = floorMod(cartId.hashCode(), S)`.

### 5.1 Kafka

| Topic | Key | Value | Retention |
|---|---|---|---|
| `cart-events` | cartId | `{schemaVersion, type, cartId, shopperKey, firstName?, version, occurredAt, items[]}` | 7 days |
| `cart-events-dlq` | cartId | original bytes; headers `error`, `source-offset` | 30 days |
| `reminder-intents-fast` | cartId | `{schemaVersion, key, cartId, version, offsetIndex, scheduledFor, sendBy}` for offsets below `FAST_OFFSETS` (default 2) | 7 days |
| `reminder-intents-slow` | cartId | same, for the remaining offsets | 7 days |
| `reminder-dlq` | cartId | intent plus `{reason, failedAt}`; reason `poison` for undeserializable intents | 30 days |
| `reminder-outcomes` | cartId | `{schemaVersion, key?, cartId, arm, kind, at, attempts}`, kind one of `ABANDONED` (per-arm dataset for the control-group metric), `SENT`, `SKIPPED_LATE`, `CANCELLED`, `DEAD` | 7 days |
| `sink-sends` | cartId | one record per `send()` call at the recording sink: `{key, cartId, at, firstName, itemCount}` | 7 days |

Intents carry no items or names; the dispatcher builds the message from its consistent cart read, which is the send-time personalization refresh DESIGN §10 describes.

Broker: `auto.create.topics.enable=false`; replication factor and `min.insync.replicas` from config (1/1 local, 3/2 production); single-node KRaft also sets `offsets.topic.replication.factor=1` and `transaction.state.log.replication.factor=1`. Producers: `acks=all`, `enable.idempotence=true`. Consumers: manual commits after processing, commit on partition revoke and on shutdown.

### 5.2 Redis (rebuildable only, AOF on)

| Key | Type | Content |
|---|---|---|
| `timers:{s}` | sorted set | member cartId, score due or lease-expiry time |
| `timerdata:{s}` | hash | cartId → `kind\|version\|offsetIndex\|dueAt` |
| `watermarks` | hash | partition → `eventTime\|updatedAt` |
| `epoch` | string | sentinel written by `init` and the reconciler; missing means Redis lost data |

The `{s}` hash tag keeps each shard's keys in one Cluster slot, so Lua scripts stay cluster-safe (64 tags spread slightly unevenly over nodes; harmless at this size). Scripts use Redis `TIME`, never the client clock:

- `upsert(timer)`: writes only if the new `(version, offsetIndex)` is greater than the stored one, `CHECK_ABANDON` counting as −1; equal data is a no-op that keeps any lease score.
- `claim(upTo, n, leaseMs)`: takes up to `n` members with score ≤ `min(upTo, TIME)` and sets their score to `TIME + lease`, returning their data. A crashed claimer's timers become due again at lease expiry.
- `ack(cartId, data)`: removes only if the stored data still equals the claimed data.
- `remove(cartId, version)`: removes only if the stored version is ≤ `version`, so a stale detector cannot delete a newer cart cycle's timer.

A late upsert for an older version after a purchase removal can resurrect a timer; it fires, fails the version compare, and is dropped. Documented, not tombstoned.

### 5.3 DynamoDB

**`recovery-meta`**, one item: `shards`, `partitions`, `paused` (the guardrail pause switch, read by dispatchers every 5 s).

**`carts`**, partition key `cartId`:

- `shopperKey, firstName, status, version, lastActivityAt, items (at most 50), arm, sequenceStarts (pruned to the frequency window)`
- `openShard` (`"s#<n>"`) and `openUntil` = `lastActivityAt + lastOffset + lastLatenessBound`, rounded up to the next hour. Present while the cart has a next step; removed on `CLOSED`, when the cart is abandoned but ineligible (holdout or capped), and when the last offset is published.
- Sparse GSI `open-by-shard`: partition `openShard`, sort `openUntil`, projection `INCLUDE (status, version, lastActivityAt, arm, sequenceStarts)`, about 150 bytes per entry.
- `ttl` = `lastActivityAt + 30 days`, longer than the frequency window and Kafka retention, so a redelivered old event cannot recreate an expired cart as new.
- Detector edit or resume: `UpdateItem SET status=ACTIVE, version, lastActivityAt, items (edit only), firstName, shopperKey and arm if not set, openShard, openUntil, ttl`, condition `attribute_not_exists(cartId) OR version < :v`, `ReturnValues ALL_NEW`. Purchase or clear: `SET status=CLOSED, version REMOVE openShard, openUntil`, same condition. A failed condition is a stale or duplicate event.
- Scheduler abandonment: `SET status=ABANDONED, sequenceStarts = :pruned+new` (plus `REMOVE openShard` when ineligible), condition `version = :v AND status = ACTIVE`. End of sequence: `REMOVE openShard`, condition `version = :v AND status = ABANDONED`.
- Base-table reads by scheduler and dispatcher use `ConsistentRead=true`; GSI reads are eventually consistent, which is safe because every consumer of a GSI result reloads or conditions its write.

**`send-ledger`**, partition key `cartId`, sort key `sk = "<version, 20 digits>#<offset, 2 digits>"`, rows about 200 bytes:

- `status`: `SENDING` (with `leaseToken`, `leaseUntil`), `RETRYING`, final `SENT`, `SKIPPED_LATE`, `CANCELLED`, `DEAD`
- `sendBy`, `attempts`, `nextAttemptAt` (for `SENDING` equal to `leaseUntil`), `reason`, `ttl` of 30 days. No intent payload: the key gives cart, version, and offset, and the message is built from the cart at send time.
- `retryShard` present for every non-final status; sparse GSI `retrying-by-shard`: partition `retryShard`, sort `nextAttemptAt`, `KEYS_ONLY`.
- Claim (one conditional write): create as `SENDING` if absent; or take over if `RETRYING AND nextAttemptAt <= :now`, or `SENDING AND leaseUntil <= :now`. Every claim sets a fresh `leaseToken`. Every later transition is conditioned on `status = SENDING AND leaseToken = :mine`; a failed condition means the lease was lost, counted as `dispatch.lease_lost`, and the attempt stops.
- Highest offset for a version: query `begins_with(sk, "<version>#")`, descending, limit 1.

### 5.4 Watermark

- Each detector, after committing a batch, writes for each of its partitions: `eventTime = now` if the partition was caught up at that poll (consumer lag 0), otherwise the `occurredAt` of the last committed record; plus `updatedAt = now`.
- The global watermark `W` is the minimum over all `P` partitions. A missing field, or one with `updatedAt` older than 5 s (a dead detector), counts as `W = 0`, meaning lagging.
- Scheduler: `claimDue(upTo = W − CLOCK_SKEW)`, default skew 5 s, covering producer clock skew and small in-partition disorder.
- Dispatcher: pauses both intent consumers and the retry loop while `now − W > MAX_DETECTOR_LAG` (default 5 s), and resumes when caught up. Pausing adds latency; reminders that miss `sendBy` while paused are skipped, which is the design's drop-when-in-doubt rule.
- In memory, the detector is synchronous so `W = now`, unless the `Pipeline` detector-lag option buffers events.

### 5.5 Write and read cost at 5k events per second (12.5k spike, 945 reminders per second, about 2.4k per second in the next-day burst)

| Store | Load | Figure |
|---|---|---|
| `carts` writes | 1 UpdateItem per event at 1 to 3 WCU (item size with up to 50 items), plus 2 per abandonment | about 10k WCU/s baseline, about 25k spike; GSI moves at most hourly per cart |
| `carts` reads | Consistent GetItem per timer fire and per dispatch; the detector no longer reads | about 2.5k to 5k RCU/s |
| `send-ledger` | claim + GSI insert + finish + GSI delete = 4 WCU per reminder | about 3.8k WCU/s baseline, about 9.6k in the next-day burst |
| Redis | 1 script per event plus claims and acks | about 7k to 16k ops/s across the cluster; about 5 to 6 GB for 32M timers, about 90 MB per shard |
| Reconciler | `open-by-shard` holds in-flight carts only (about 1.8M active + 30M mid-sequence); INCLUDE projection is about 4.8 GB, about 0.6M eventually consistent RCU per sweep | about 650 RCU/s at a 15 minute interval; one sweep of 64 parallel shard streams takes well under a minute |

On-demand capacity absorbs about 2× the previous peak instantly; the 2.5× spike needs warm throughput or provisioned capacity set in advance.

## 6. Ports and core changes

Core classes stay shared by both modes. Every port change lands in both the in-memory and the infra adapter.

### 6.1 Ports

| Port | Contract | In-memory | Infra |
|---|---|---|---|
| `Clock` | `now()` | `FakeClock` | `SystemClock` |
| `CartStateStore` | `get(cartId)` (consistent), `applyEvent(event, arm) → Optional<CartRecord>` (empty when stale), `markAbandoned(record, starts, eligible) → boolean`, `endSequence(cartId, version) → boolean`, `openCarts(shard, now) → Stream<CartRecord>` | map | DynamoDB `carts` + `open-by-shard` |
| `TimerStore` | `upsert(timer) → boolean`, `remove(cartId, version)`, `claimDue(upTo, limit)`, `ack(timer)`, `existing(shard, cartIds) → Map` | map + priority queue with real leases on the fake clock | Redis scripts |
| `Watermark` | `publish(partition, eventTime)`, `current() → Instant` | the clock, or the buffered detector's position | Redis `watermarks` |
| `IntentPublisher` | `publish(intent)`, blocking until acknowledged, lane chosen by offset | queue drained by `Pipeline` | Kafka producer |
| `SendLedger` | `claim(key, sendBy, now) → Claimed(token, attempts, sendBy) \| NotClaimed(reason)`, `markRetry(key, token, attempts, nextAt) → boolean`, `finish(key, token, outcome, reason) → boolean`, `dueRetries(now, limit) → keys`, `reopen(key, now) → boolean`, `highestOffsetIndex(cartId, version)` | map + retry priority queue with real leases and tokens | DynamoDB `send-ledger` + `retrying-by-shard` |
| `NotificationSink` | `send(message) → SendResult` | `RecordingNotificationSink` | recording sink producing to `sink-sends`, optional injected transient failure rate |
| `OutcomeRecorder` | `record(outcome)` | list | Kafka `reminder-outcomes` |
| `DeadLetterQueue` | `add(letter)`; replay reads its own consumer | list with `drain()` | Kafka `reminder-dlq` |
| `ArmAssigner` | unchanged | | |

The `Outbox` port, `InMemoryOutbox`, the Redis retry queue, and `rebuildRetryIndex` do not exist.

### 6.2 Core classes

- **`AbandonmentDetector`**: one conditional `applyEvent`; on success, `ACTIVE` upserts `CHECK_ABANDON`, `CLOSED` calls `remove(cartId, version)`; on a stale result counts `events.ignored`. No read, no conflict loop.
- **`ReminderScheduler`** (per claimed timer; the caller acks after processing and leaves the timer un-acked on an exception):
  - Consistent reload; version mismatch → `timers.stale`.
  - `CHECK_ABANDON` on `ACTIVE`: compute pruned `sequenceStarts` + this start and eligibility; `markAbandoned` (a failed condition means a newer event won: drop); record an `ABANDONED` outcome with the arm; eligible → upsert `REMINDER 0`, otherwise the same write removed `openShard`.
  - `CHECK_ABANDON` on `ABANDONED` at the same version (a redelivery after a crash between the two writes): if eligible, upsert `REMINDER 0`; the monotonic upsert makes this idempotent.
  - `REMINDER i` on `ABANDONED`: publish `{key, cartId, version, i, scheduledFor, sendBy = scheduledFor + bound[i]}` to the lane for `i`; if `i + 1` exists, upsert it, otherwise `endSequence`.
  - Any other status → `timers.wrong_status`.
- **`Dispatcher.handle(intent)`**: take a token from the lane's bucket → `claim(key, sendBy, now)`; `NotClaimed` → count by reason (`dispatch.duplicate`), stop → if `now > sendBy`: record `SKIPPED_LATE`, `finish` → consistent cart read; not `ABANDONED` at the intent's version: record `CANCELLED`, `finish` → build the message (first name, items) and `send`:
  - `SENT`: record `SENT`, `finish(SENT)`.
  - Transient with attempts remaining: `markRetry` at `now + random(0, RETRY_BASE × 2^(attempts−1))` (full jitter).
  - Transient exhausted or permanent: produce the DLQ record, record `DEAD`, `finish(DEAD)`.
  - Every outcome and DLQ record is produced before its `finish`; a crash in between replays the attempt, and duplicate outcome or DLQ records are harmless (consumers dedupe by key; `reopen` only moves `DEAD`).
  - A `false` from `markRetry` or `finish` counts `dispatch.lease_lost` and stops.
- **Retry loop** (every `RETRY_POLL`, default 1 s, while not paused): `dueRetries(now, limit)` per shard from the KEYS_ONLY index → `claim` each (the conditional write settles races between dispatchers) → the same steps from the `sendBy` check on, in the lane of the key's offset.
- **Circuit breaker**: over the last 100 send attempts, a transient-failure ratio above 50% opens the breaker for 30 s: consumers and the retry loop pause, then one probe send decides. The guardrail switch `recovery-meta.paused` pauses the same way. Both reuse Kafka `pause`/`resume`.
- **Replay**: for each `reminder-dlq` record except reason `poison`, `ledger.reopen(key)` moves `DEAD` to `RETRYING`, due now, attempts reset. Replaying twice is harmless. A replay past `sendBy` is skipped as late.
- **`Reconciler`**: per shard in parallel on virtual threads, stream `openCarts(shard, now)`, check `existing(shard, cartIds)` in pipelined batches, and only for carts whose timer is missing or older than the record: `ACTIVE` → upsert `CHECK_ABANDON`; eligible `ABANDONED` → upsert the first offset after the ledger's highest that is still within its lateness bound. Runs every `RECONCILE_INTERVAL` (default 15 min) and immediately when the Redis `epoch` sentinel is missing, then rewrites it. Logs sweep duration; a sweep longer than the smallest lateness bound is reported on `/ready`. No lock: rebuilding is idempotent.
- **Model**: `CartEvent` and `CartRecord` gain an optional `firstName`; `CartRecord` prunes `sequenceStarts` and caps items at 50. `Metrics` becomes thread-safe (`ConcurrentHashMap` + `LongAdder`).
- **`Pipeline` (in-memory)**: drains intents into `dispatcher.handle` after each timer fire; `advanceTo` also stops at due retry times; options for a detector lag (buffered events, watermark behind) and a dispatch delay, so the verifier can show the gaps the watermark closes.

Metric changes: `reminders.scheduled` → `reminders.published`, `reminders.duplicate_timer` → `dispatch.duplicate`, `reminders.skipped_late` → `dispatch.skipped_late`; new `dispatch.lease_lost`, `dispatch.breaker_open`, `watermark.lag_ms`.

### 6.3 Concurrency and failure handling

- Consumer roles: one poll thread per consumer; each batch grouped by `cartId`; groups run concurrently on virtual threads, events within a group in order; commit after the whole batch completes.
- Scheduler: claimed timers run concurrently on virtual threads (one timer per cart, so no grouping); `publish` blocks until the broker acks; ack after processing.
- Dispatcher: small `max.poll.records`; an empty bucket, the watermark gate, the breaker, or the guardrail switch `pause` the partitions while polling continues, so blocking never exceeds `max.poll.interval.ms`. The fast lane has its own bucket (`FAST_SEND_RATE`) so slow-lane backlog never consumes it. Rates are per replica; the global rate is replicas × rate.
- Failure classification: deserialization and validation errors (bad JSON, unknown event type, an offset index the config no longer has) go to the topic's DLQ at once and the offset is committed; I/O and throttling errors retry the failed groups in process with backoff, then seek back to the last committed offset with exponential backoff capped at 30 s.
- Thread safety: core classes are stateless beyond ports; AWS SDK clients, the Kafka producer, and Lettuce are thread-safe; the Kafka consumer is used only by its poll thread; `Metrics`, buckets, and the breaker are thread-safe.
- Client sizing: in-flight work per role is bounded by `MAX_IN_FLIGHT` (default 256); the DynamoDB HTTP client's `maxConnections` matches it (the SDK default of 50 would cap a JVM near 5k events per second). Integration tests run with `-Djdk.tracePinnedThreads=full` and fail on pinning reports from our code paths.
- Lease rule: `LEASE ≥ 3 × GATEWAY_TIMEOUT`, validated at startup. Retry spacing: startup logs the effective attempt count per offset given its lateness bound (with defaults, about 3 for the 5 minute bounds).

## 7. Runtime and operations

### 7.1 Configuration

Environment variables, validated at startup (invalid config exits with a clear message); every role logs a hash of its effective config; roles refuse to start if `SHARDS` or `PARTITIONS` differ from `recovery-meta`.

- Recovery: `WINDOW`, `OFFSETS`, `LATENESS_BOUNDS` (ISO-8601 durations), `FREQUENCY_CAP`, `HOLDOUT_PERCENT`, `MAX_SEND_ATTEMPTS`, `RETRY_BASE`, `FAST_OFFSETS`
- Infra: `KAFKA_BOOTSTRAP`, `REDIS_URL`, `DYNAMO_ENDPOINT` (set only for DynamoDB Local), `SHARDS`, `PARTITIONS`, `REPLICATION_FACTOR`, `MIN_INSYNC_REPLICAS`
- Runtime: `LEASE`, `GATEWAY_TIMEOUT`, `MAX_SEND_RATE`, `FAST_SEND_RATE`, `SEND_FAILURE_RATE` (load testing only), `RECONCILE_INTERVAL`, `RETRY_POLL`, `MAX_IN_FLIGHT`, `MAX_DETECTOR_LAG`, `CLOCK_SKEW`

Recovery configuration changes apply to carts as their timers are next handled; `sendBy` already stamped on an intent is not recomputed.

### 7.2 Packaging

A multi-stage `Dockerfile`: a Gradle JDK 21 stage builds with a BuildKit cache mount, a slim JRE 21 stage runs it. `docker compose up --build` needs only Docker. Dependency versions are pinned at plan time from current releases.

### 7.3 `docker-compose.yml`

- `kafka`: Apache Kafka single-node KRaft with the replication settings in §5.1, healthcheck
- `redis`: Redis 7, `appendonly yes`
- `dynamodb`: DynamoDB Local, `-sharedDb` with a named volume by default, `-inMemory` under the `load` profile
- `init`: runs once; roles depend on it completing successfully
- `detector`, `scheduler`, `dispatcher`: 2 replicas each; `reconciler`: 1
- `loadgen`: `load` profile, on demand
- All roles share one `x-recovery-env` block, `JAVA_TOOL_OPTIONS=-Xmx256m -XX:+UseSerialGC`, `stop_grace_period: 40s`, no published ports; infrastructure ports bound to localhost
- A `demo.env` with short timings: window 30 s, offsets 30 s / 60 s / 120 s
- Documented minimum: about 6 GB of memory for Docker Desktop

### 7.4 Lifecycle and health

- SIGTERM: stop polling, finish the in-flight batch, commit, close clients, within 30 s.
- `/health` (JDK `com.sun.net.httpserver`): 200 while the role's loop threads are alive and have iterated within 3× their poll interval, including while backing off or paused; the compose healthcheck, so a dependency outage never causes restarts.
- `/ready`: dependency state, watermark lag, breaker and pause state, reconciler sweep duration, and a stuck-partition signal (committed offset unchanged for 5 minutes while lag > 0).
- `/metrics`: counters as text; also logged every 10 s.
- The scheduler idles 200 ms between empty claim rounds.

### 7.5 Runbook (README)

1. In-memory: `./gradlew test`, `./gradlew run`.
2. Infra: `docker compose up -d --build`, `docker compose logs -f`.
3. Visible demo: `demo.env`, then `docker compose --profile load run loadgen --rate=50 --duration=60s`.
4. Drills with expected outcomes: kill a scheduler mid-run (leased timers redelivered); `redis-cli FLUSHALL` (dispatchers pause on the missing watermark, the reconciler rebuilds timers at once); stop the detectors (scheduler and dispatchers pause; nothing is sent against stale state); stop dispatchers past the lateness bound (backlog skipped, not sent late); `SEND_FAILURE_RATE=0.3` (retries, DLQ, then `replay` recovers); set `recovery-meta.paused` (sending stops within 5 s).
5. Replay: `docker compose run --rm dispatcher --role=replay`.

## 8. Testing

### 8.1 Layers

| Layer | Command | Needs | Proves |
|---|---|---|---|
| Unit + fake-clock verifier | `./gradlew test` | JDK | Exact timing and all business rules; the source of truth for when |
| Contract tests | in-memory in `test`, infra in `integrationTest` | Docker for infra | Adapter parity: monotonic upsert and equal-data no-op, conditional remove, claim and lease expiry, ack-if-unchanged, ledger claim, takeover, and stale-token rejection, cart conditional updates including a concurrent detector write during abandonment |
| Infra end-to-end | `./gradlew integrationTest` | Docker | Wiring and failure semantics with roles as in-process threads |
| Load test | `docker compose --profile load run loadgen` | Docker | Throughput, lag, latency, and correctness under load |

`integrationTest` is a separate source set using `@Testcontainers(disabledWithoutDocker = true)`; `./gradlew check` runs both. Containers are shared; each test uses its own cart-id prefix and filters by it. Infra tests assert order, counts, and outcomes, never timestamps; timings are seconds-scale with lateness bounds at least 10× expected jitter; waiting uses an in-repo `await(condition, timeout)` helper. Crash points are proven deterministically in unit and contract tests (claim without ack, lease expiry, reclaim; outcome produced then crash before finish); killing real processes is a runbook drill.

### 8.2 New verifier scenarios

- A lagging detector holds the reminder: a purchase buffered behind a lagging detector means the due reminder is not sent while the watermark lags, and is cancelled once the detector catches up.
- A redelivered `CHECK_ABANDON` after a crash between the abandonment write and the reminder upsert still sends `REMINDER 0`.
- A dispatch delay longer than the lateness bound skips the reminder as late rather than sending it.

### 8.3 Infra end-to-end tests (at most 7, each under 30 s)

1. Happy path: one edit gives three sends in order with the expected keys.
2. Purchase mid-sequence: no sends after the purchase.
3. Duplicate and out-of-order events: exactly one send per key.
4. Two schedulers and two dispatchers on shared shards: no duplicate sends across 200 carts, counted at `sink-sends`.
5. `SEND_FAILURE_RATE=0.5`, 2 max attempts: some sends after a retry, some dead-lettered; `replay` then sends each dead key exactly once.
6. Redis `FLUSHALL` mid-sequence: dispatchers pause, the reconciler rebuilds, the remaining sends happen with no duplicates.
7. Detector stopped, purchase produced, reminder due: no send; detector restarted: the reminder is cancelled.

### 8.4 Existing test updates

Duplicate-timer handling moves to the dispatcher claim: scenario 6 and `ReminderSchedulerTest` duplicate cases assert `dispatch.duplicate`, with unchanged send counts. Lateness moves to the dispatcher: scenario 11 asserts `dispatch.skipped_late` and a ledger with three rows (one `SENT`, two `SKIPPED_LATE`). `DispatcherTest` is rewritten around `handle` and the retry loop, keeping its cases and adding fencing, breaker, and produce-before-finish crash points. `AbandonmentDetectorTest` drops the conflict-retry expectation; detector staleness is a failed conditional update. New unit tests: monotonic upsert, conditional remove, ledger state machine with fencing, watermark gate, failure classification.

### 8.5 Load test

- Workload: simulated shoppers, about 10 events per cart session, about 70% abandon, the rest purchase at a random point, some resume; `--rate` (default 5000) for `--duration`, compressed timings; stays under the frequency cap.
- Drain wait after producing: window + last offset + lateness bound + one reconcile interval.
- Sampled every 5 s: achieved produce rate, consumer lag per group, watermark lag, Redis timer backlog past due.
- Sends read from `sink-sends` (one record per `send()` call); accounting read from `reminder-outcomes`; both filtered to the run.
- Expected sends computed by loadgen from each cart's script, using the same `HashArmAssigner` for holdout.
- Report (console and `build/reports/load/<timestamp>.md`): achieved versus target rate; maximum and ending lag per group, naming the bottleneck stage; scheduled-to-sent latency p50/p95/p99 per lane, excluding a 30 s warm-up; expected, sent, skipped late, cancelled, dead-lettered; correctness at the sink: duplicate sends = 0, post-purchase sends = 0 (a send whose cart had a purchase at least 1 s before it), unexplained missing (expected − sent − skipped late − cancelled − dead) under 0.1%; machine cores and memory, noting loadgen shares the machine.

## 9. Documentation updates

- `DESIGN.md`: sections 4 to 7 for the fenced ledger claim, the single lateness deadline, the watermark gate, the Redis-rebuildable rule, monotonic timers, and field-scoped cart updates; section 9 for the fast and slow lanes; section 7 for the circuit breaker, jitter, and poison handling; section 3 for the guardrail pause switch and the `ABANDONED` per-arm outcome record; section 10 for first-name personalization built at send time; section 8 capacity from §5.5; section 11 gains the infra adapter column and how to run it; a note that per-shopper capping would use a counter item, not re-keying; the "Demonstrated by" citations extended to the infra tests.
- `DESIGN.md` §13 gains: how long does the gateway honour an idempotency key? The design depends on it in the lease-pause edge case.
- `README.md`: the runbook in §7.5.

## 10. Assumptions

- Clocks are NTP-synced with skew under `CLOCK_SKEW`; the Cart Service stamps `occurredAt` close to publish time, and events within a partition are ordered up to that skew.
- Docker Desktop has about 6 GB of memory for the full stack.
- The notification gateway honours the idempotency key for at least the longest lateness bound; it remains out of scope and is stood in for by the recording sink.

## 11. Out of scope

Real sends and gateway integration, provisioning real AWS (Terraform or CloudFormation), CI workflows, TLS and authentication, multi-region, schema registry (payloads stay JSON with `schemaVersion`), Kafka transactions and exactly-once semantics, timing-variant experiment arms (a single global `OFFSETS`; DESIGN §10 describes arms as future work), and the open business questions in `DESIGN.md` section 13.
