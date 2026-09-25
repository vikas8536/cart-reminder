# Production Infrastructure: Design Spec

Date: 2026-09-25
Status: approved in brainstorming (sections 1-5, each reviewed for anti-patterns and first principles), awaiting written review
Builds on: `docs/superpowers/specs/2026-09-23-abandoned-cart-recovery-design.md` and the merged in-memory implementation

## 1. Purpose

Evolve the in-memory case-study pipeline into separately deployable roles running on real Kafka, Redis, and DynamoDB, and measure it under load, while keeping the in-memory mode as the zero-Docker path for reviewers.

Success means:

1. `./gradlew test` and `./gradlew run` behave as today with only a JDK: unit tests, the fake-clock verifier, and the demo.
2. `docker compose up --build` with only Docker runs the full stack with multiple replicas per role.
3. Contract tests prove the in-memory and infra adapters behave identically, and infra end-to-end tests prove wiring and failure semantics on real containers.
4. A load test reports achieved throughput, per-stage lag, send latency, and zero duplicate or post-purchase sends, and names the local bottleneck honestly.
5. `DESIGN.md` claims about the production system are backed either by the fake-clock verifier (timing, rules) or the infra tests (wiring, failures).

## 2. Decisions

| Decision | Choice | Reason |
|---|---|---|
| Scope | Separately deployable roles on real infra, plus a load test | Shows the design running, not only described. |
| Framework | Plain Java 21 with official clients (`kafka-clients`, Lettuce, AWS SDK v2, Jackson) | Core classes already depend only on ports; the scored mechanics (offset commits, leases, conditional writes) stay visible; smallest diff and fastest tests. |
| Packaging | One application and one image, role selected with `--role` | One build, one image, least code. |
| Worker to dispatcher | Kafka `reminder-intents` topic | Decouples scheduling from sending, buffers bursts, and puts outbound flow on Kafka. |
| Dedupe point | The dispatcher's conditional ledger claim, adjacent to the send | First principles: dedupe where the side effect happens. Removes the scheduler's dual write. |
| Lateness | Checked in one place, the dispatcher | It must check anyway for retries and replays; one decision point. |
| Durability rule | Redis holds only rebuildable indexes; DynamoDB and Kafka are the sources of truth | Losing Redis loses no work, only time. |
| Timer store | Keep Redis (not DynamoDB-embedded timers) | Cheaper per upsert at 5k to 12.5k per second; the dual write is covered by monotonic upserts and the reconciler. |
| Partition key | `cartId` for every topic | State, version, timers, and ledger are per cart; always present and immutable; guest login changes the shopper key but not the cart. A future per-shopper cap uses a per-shopper counter item, not re-keying. |
| Load test | Measure on local containers and report the bottleneck honestly | A laptop cannot sustain 12.5k per second through DynamoDB Local; the design does not change to beat an emulator. |
| Concurrency | Per-batch processing on virtual threads, grouped by `cartId` | Per-partition serial processing cannot reach the target rate (Little's law); grouping keeps per-cart order. |

## 3. Architecture

```
                 cart-events (keyed by cartId)
Cart Service ───────────────► [detector] ──► DynamoDB carts (consistent read, conditional put on version)
 (loadgen stands in)                    └──► Redis timers (monotonic upsert by (version, offset))
                                                    │
                      [scheduler] ◄── claimDue with lease, ack when unchanged
                           │ consistent reload + version/status compare
                           ▼
                  reminder-intents (keyed by cartId)
                           │
                      [dispatcher] ── claim ledger row ── late? ── cart re-check ── rate limit ── send
                           │   SENT/SKIPPED_LATE/CANCELLED/DEAD ──► reminder-outcomes
                           │   transient ──► ledger RETRYING + Redis retries index ──► dispatcher retry loop
                           └── dead ──► reminder-dlq ◄── [replay] reopens ledger rows
                      [reconciler] ── every interval, under a Redis lock: open carts ──► missing timers,
                                      non-final ledger rows ──► retries index
                      [loadgen]    ── produces events at a rate, reads outcomes, writes a report
```

### Roles

| Role | Job | Scales by |
|---|---|---|
| `init` | Create topics, tables, and indexes; idempotent | One-off |
| `detector` | Consume `cart-events`, run `AbandonmentDetector`, commit after the batch | Partitions and concurrency |
| `scheduler` | Claim due timers from all shards, run `ReminderScheduler`, publish intents, ack | Instances; claims are atomic |
| `dispatcher` | Consume `reminder-intents`, run `Dispatcher.handle`, run the retry loop | Partitions and concurrency |
| `reconciler` | Rebuild missing timers and the retries index | One active instance under a lock |
| `replay` | Read `reminder-dlq`, reopen ledger rows | One-off |
| `loadgen` | Generate workload, observe outcomes, write the report | One-off |

`--mode=inmemory` is the default and runs the existing fake-clock demo. Infra roles use `SystemClock`.

## 4. Data model

Conventions: epoch-millisecond timestamps, JSON payloads (Jackson). `S` shards and `P` partitions are configurable (64 production, 8 local). `shard(cartId) = floorMod(cartId.hashCode(), S)`.

### 4.1 Kafka

| Topic | Key | Value | Retention |
|---|---|---|---|
| `cart-events` | cartId | `{type, cartId, shopperKey, version, occurredAt, items[]}` | 7 days |
| `cart-events-dlq` | cartId | original bytes; headers `error`, `source-offset` | 30 days |
| `reminder-intents` | cartId | `{key, cartId, shopperKey, version, offsetIndex, scheduledFor, items[]}` | 7 days |
| `reminder-dlq` | cartId | intent plus `{reason, failedAt}` | 30 days |
| `reminder-outcomes` | cartId | `{key, cartId, outcome, at, attempts}`, outcome one of `SENT`, `SKIPPED_LATE`, `CANCELLED`, `DEAD` | 7 days |

Broker settings: `auto.create.topics.enable=false`. Replication factor and `min.insync.replicas` from config (1/1 local, 3/2 production). Producers: `acks=all`, `enable.idempotence=true`. Consumers: manual commits after processing, commit on partition revoke and on shutdown.

### 4.2 Redis (rebuildable indexes only, AOF on)

| Key | Type | Content |
|---|---|---|
| `timers:{s}` | sorted set | member cartId, score due or lease-expiry time |
| `timerdata:{s}` | hash | cartId → `kind\|version\|offsetIndex\|dueAt` |
| `retries:{s}` | sorted set | member ledger key `cartId\|sk`, score next attempt or lease-expiry time |
| `reconciler:lock` | string | `SET NX PX` lock |

The `{s}` hash tag keeps each shard's keys in one Cluster slot, so Lua scripts stay cluster-safe. Scripts:

- `upsert(timer)`: writes only if `(version, offsetIndex)` of the new timer is greater than or equal to the stored one, with `CHECK_ABANDON` as offset −1.
- `claim(now, n, leaseMs)`: takes up to `n` members with score ≤ now and sets their score to `now + lease`, returning their data. A crashed claimer's timers become due again at lease expiry.
- `ack(cartId, data)`: removes only if the stored data still equals the claimed data, so a timer re-armed during processing survives.
- The same claim and remove pattern serves `retries:{s}`; an entry is removed only when its ledger row reaches a final status.

A late scheduler upsert for an older version after a purchase removal can resurrect a timer; it fires, fails the version compare, and is dropped. Documented, not tombstoned.

### 4.3 DynamoDB

**`carts`**, partition key `cartId`:

- `shopperKey, status, version, lastActivityAt, items (at most 50), arm, sequenceStarts (pruned to the frequency window)`
- `openShard` (`"s#<n>"`) and `openUntil` = `lastActivityAt + lastOffset + lastLatenessBound`, rounded up to the next hour; both present only while status is not `CLOSED`.
- Sparse GSI `open-by-shard`: partition `openShard`, sort `openUntil`. The reconciler queries `openShard = s AND openUntil >= now`.
- Writes condition on `attribute_not_exists(cartId) OR version = :expected`. Every read uses `ConsistentRead=true`.

**`send-ledger`**, partition key `cartId`, sort key `sk = "<version, 20 digits>#<offset, 2 digits>"`:

- `status`: `SENDING` (with `leaseUntil`), `RETRYING` (with `attempts`, `nextAttemptAt`), final `SENT`, `SKIPPED_LATE`, `CANCELLED`, `DEAD`
- `intent` JSON, `reason`, `ttl` of 30 days
- `retryShard` present only for non-final statuses; sparse GSI `retrying-by-shard`: partition `retryShard`, sort `nextAttemptAt`
- Highest offset for a version: query `begins_with(sk, "<version>#")`, descending, limit 1.

### 4.4 Write cost at 5k events per second

`carts`: about 1 write unit per event plus 1 per abandonment; the GSI moves at most once per cart per hour because `openUntil` is hour-rounded, keeping each GSI shard under about 200 writes per second at spike. Redis: 1 script call per event. `send-ledger`: about 2 to 3 writes per reminder, about 2k to 3k per second at peak. Redis memory for 30 million timers: about 5 to 6 GB, about 90 MB per shard.

## 5. Ports and core changes

Core classes stay shared by both modes. Every port change lands in both the in-memory and the infra adapter.

### 5.1 Ports

| Port | Contract | In-memory | Infra |
|---|---|---|---|
| `Clock` | `now()` | `FakeClock` | `SystemClock` |
| `CartStateStore` | `get(cartId)` (consistent), `put(record, expectedVersion)`, `openCarts(now)` | map | DynamoDB `carts` + `open-by-shard` |
| `TimerStore` | `upsert(timer) → boolean` (monotonic), `remove(cartId)`, `claimDue(now, limit)`, `ack(timer)` | map + priority queue with real lease semantics on the fake clock | Redis scripts |
| `IntentPublisher` | `publish(intent)`, blocking until acknowledged | queue drained by `Pipeline` | Kafka producer |
| `SendLedger` | `claim(intent, now) → CLAIMED \| DUPLICATE \| LEASED_ELSEWHERE`, `finish(key, outcome, reason)`, `markRetry(key, attempts, nextAt)`, `dueRetries(now, limit)`, `reopen(key, now)`, `highestOffsetIndex(cartId, version)`, `rebuildRetryIndex(now)` | map + retry priority queue with real leases | DynamoDB `send-ledger` + Redis `retries:{s}` behind one adapter |
| `NotificationSink` | `send(intent) → SendResult` | `RecordingNotificationSink` | recording sink with optional injected transient failure rate |
| `OutcomeRecorder` | `record(intent, outcome, at, attempts)` | list | Kafka `reminder-outcomes` |
| `DeadLetterQueue` | `add(letter)`; replay reads its own consumer | list with `drain()` | Kafka `reminder-dlq` |
| `ArmAssigner` | unchanged | | |

The `Outbox` port and `InMemoryOutbox` are removed.

### 5.2 Core classes

- **`AbandonmentDetector`**: on a conditional-put conflict, re-read and re-apply, up to 3 attempts, then throw so the offset is not committed and the event is redelivered.
- **`ReminderScheduler`**: consumes claimed timers. `CHECK_ABANDON`: consistent reload, version and status compare, conditional put to `ABANDONED` (a conflict means a newer event won; drop), policy check, upsert `REMINDER 0`. `REMINDER i`: consistent reload, version and status compare, `publish(intent)`, upsert `REMINDER i+1` if one exists. No ledger write and no lateness check. The caller acks after processing; an exception leaves the timer un-acked for lease redelivery.
- **`Dispatcher.handle(intent)`**: `claim` → `DUPLICATE` or `LEASED_ELSEWHERE`: count `dispatch.duplicate`, stop. Late (now after `scheduledFor + bound[offset]`): `finish(SKIPPED_LATE)`. Cart re-check with a consistent read: not wanted → `finish(CANCELLED)`. Rate-limit token, then `send`: `SENT` → `finish(SENT)`; transient → `markRetry` with exponential backoff while attempts remain, else `finish(DEAD)` and DLQ; permanent → `finish(DEAD)` and DLQ. Every `finish` also records the outcome.
- **`Dispatcher.retryDue()`**: for each row from `dueRetries`, the same steps from the lateness check on, carrying the attempt count.
- **Replay**: for each DLQ record, `ledger.reopen(key)` moves `DEAD` to `RETRYING`, due now, attempts reset. No Kafka re-publish; a crash mid-replay is recovered by `rebuildRetryIndex`. Replaying twice is harmless because `reopen` only moves `DEAD`.
- **`Reconciler`**: `openCarts(now)`; `ACTIVE` → upsert `CHECK_ABANDON`; eligible `ABANDONED` → upsert the first offset after the ledger's highest that is still within its lateness bound (a lower timer than one already stored is rejected by the monotonic upsert); then `ledger.rebuildRetryIndex(now)`, covering `RETRYING` rows and `SENDING` rows with expired leases.
- **Model**: `CartRecord` prunes `sequenceStarts` to the frequency window and caps items at 50. `Metrics` becomes thread-safe (`ConcurrentHashMap` + `LongAdder`).
- **Rate limiter**: a thread-safe token bucket in the dispatcher, disabled in memory.
- **`Pipeline` (in-memory)**: after each timer fire, drain the intent queue into `dispatcher.handle`; `advanceTo` also stops at due retry times.

Metric renames: `reminders.scheduled` → `reminders.published`, `reminders.duplicate_timer` → `dispatch.duplicate`, `reminders.skipped_late` → `dispatch.skipped_late`.

### 5.3 Concurrency

- Consumer roles: one poll thread per consumer; each batch is grouped by `cartId`; groups run concurrently on virtual threads, events within a group in order; commit after the whole batch completes.
- Scheduler: each claimed batch runs on virtual threads grouped by `cartId`; `publish` blocks until the broker acks; ack after processing.
- Dispatcher: small `max.poll.records`; when the token bucket is empty, `pause` the assigned partitions and keep polling, then `resume`, so blocking never exceeds `max.poll.interval.ms`.
- Batch failure (throttling, outage): seek back to the last committed offset with exponential backoff capped at 30 s; surfaced on `/ready`.
- Thread safety: core classes are stateless beyond ports; AWS SDK clients, the Kafka producer, and Lettuce are thread-safe; the Kafka consumer is used only by its poll thread; `Metrics` and the token bucket are thread-safe.
- Lease rule: `LEASE ≥ 3 × GATEWAY_TIMEOUT`, validated at startup.

## 6. Runtime and operations

### 6.1 Configuration

Environment variables, validated at startup (invalid config exits with a clear message); every role logs a hash of its effective config.

- Recovery: `WINDOW`, `OFFSETS`, `LATENESS_BOUNDS` (ISO-8601 durations), `FREQUENCY_CAP`, `HOLDOUT_PERCENT`, `MAX_SEND_ATTEMPTS`, `RETRY_BASE`
- Infra: `KAFKA_BOOTSTRAP`, `REDIS_URL`, `DYNAMO_ENDPOINT` (set only for DynamoDB Local), `SHARDS`, `PARTITIONS`, `REPLICATION_FACTOR`, `MIN_INSYNC_REPLICAS`
- Runtime: `LEASE`, `GATEWAY_TIMEOUT`, `MAX_SEND_RATE`, `SEND_FAILURE_RATE` (load testing only), `RECONCILE_INTERVAL`

Configuration changes apply to carts as their timers are next handled; they are not retroactive.

### 6.2 Packaging

A multi-stage `Dockerfile`: a Gradle JDK 21 stage builds with a BuildKit cache mount, a slim JRE 21 stage runs it. `docker compose up --build` needs only Docker. Dependency versions are pinned at plan time from current releases.

### 6.3 `docker-compose.yml`

- `kafka`: Apache Kafka single-node KRaft, healthcheck
- `redis`: Redis 7, `appendonly yes`
- `dynamodb`: DynamoDB Local, `-sharedDb`, named volume
- `init`: runs once; roles depend on it completing successfully
- `detector`, `scheduler`, `dispatcher`: 2 replicas each; `reconciler`: 1
- `loadgen`: `load` profile, on demand
- All roles share one `x-recovery-env` block, `JAVA_TOOL_OPTIONS=-Xmx256m -XX:+UseSerialGC`, `stop_grace_period: 40s`, no published ports; infrastructure ports bound to localhost
- A `demo.env` with short timings: window 30 s, offsets 30 s / 60 s / 120 s
- Documented minimum: about 6 GB of memory for Docker Desktop

### 6.4 Lifecycle and health

- SIGTERM: stop polling, finish the in-flight batch, commit, close clients, within 30 s.
- `/health` (JDK `com.sun.net.httpserver`): 200 while the role's loop threads are alive and have iterated within 3× their poll interval, including while backing off; this is the compose healthcheck, so a dependency outage never causes restarts.
- `/ready`: dependency state, for humans and the load report.
- `/metrics`: counters as text; also logged every 10 s.
- Scheduler idles 200 ms between empty claim rounds. Reconciler lock expiry mid-sweep is harmless because rebuilding is idempotent; the lock only saves work.

### 6.5 Runbook (README)

1. In-memory: `./gradlew test`, `./gradlew run`.
2. Infra: `docker compose up -d --build`, `docker compose logs -f`.
3. Visible demo: `demo.env`, then `docker compose --profile load run loadgen --rate=50 --duration=60s`.
4. Drills with expected outcomes: kill a scheduler mid-run (leased timers redelivered); `redis-cli FLUSHALL` (reconciler rebuilds timers and retries within one interval); stop dispatchers past the lateness bound (backlog skipped, not sent late); `SEND_FAILURE_RATE=0.3` (retries, DLQ, then `replay` recovers).
5. Replay: `docker compose run --rm dispatcher --role=replay`.

## 7. Testing

### 7.1 Layers

| Layer | Command | Needs | Proves |
|---|---|---|---|
| Unit + fake-clock verifier | `./gradlew test` | JDK | Exact timing and all business rules; the source of truth for when |
| Contract tests | in-memory in `test`, infra in `integrationTest` | Docker for infra | Adapter parity: monotonic upsert, claim, lease expiry and redelivery, ack-if-unchanged, ledger state machine and lease takeover, consistent reads, sparse-index queries |
| Infra end-to-end | `./gradlew integrationTest` | Docker | Wiring and failure semantics with roles as in-process threads |
| Load test | `docker compose --profile load run loadgen` | Docker | Throughput, lag, latency, and correctness under load |

`integrationTest` is a separate source set using `@Testcontainers(disabledWithoutDocker = true)`; `./gradlew check` runs both. Containers are shared across tests; each test uses its own cart-id prefix and filters outcomes by it. Infra tests assert order, counts, and outcomes, never timestamps; timings are seconds-scale with lateness bounds at least 10× expected jitter; waiting uses an in-repo `await(condition, timeout)` helper. Crash-while-leased is proven at the contract level (claim, no ack, lease expiry, reclaim); killing real processes is a runbook drill.

### 7.2 Infra end-to-end tests (at most 6, each under 30 s)

1. Happy path: one edit gives three sends in order with the expected keys.
2. Purchase mid-sequence: no sends after the purchase.
3. Duplicate and out-of-order events: exactly one send per key.
4. Two schedulers and two dispatchers on shared shards: no duplicate sends across 200 carts.
5. `SEND_FAILURE_RATE=0.5`, 2 max attempts: some sends after a retry, some dead-lettered; `replay` then sends each dead key exactly once.
6. Redis `FLUSHALL` mid-sequence: after one reconcile, the remaining sends happen with no duplicates.

### 7.3 Existing test updates

Duplicate-timer handling moves to the dispatcher claim: scenario 6 and `ReminderSchedulerTest` duplicate cases assert `dispatch.duplicate`, with unchanged send counts. Lateness moves to the dispatcher: scenario 11 asserts `dispatch.skipped_late` and a ledger with three rows (one `SENT`, two `SKIPPED_LATE`). `DispatcherTest` is rewritten around `handle` and `retryDue`, keeping its cases. New unit tests: monotonic upsert, detector conflict retry (a store failing the first N puts), ledger state machine.

### 7.4 Load test

- Workload: simulated shoppers, about 10 events per cart session, about 70% abandon, the rest purchase at a random point, some resume; `--rate` (default 5000) for `--duration`, using compressed timings; stays under the frequency cap.
- Drain wait after producing: window + last offset + lateness bound + one reconcile interval.
- Sampled every 5 s: achieved produce rate, consumer lag per group, Redis timer backlog past due.
- Outcomes read from `reminder-outcomes`, filtered to the run.
- Expected sends computed by loadgen from each cart's script, using the same `HashArmAssigner` for holdout.
- Report (console and `build/reports/load/<timestamp>.md`): achieved versus target rate; maximum and ending lag per group, naming the bottleneck stage; scheduled-to-sent latency p50/p95/p99 excluding a 30 s warm-up; expected, sent, skipped late, cancelled, dead-lettered; correctness checks with duplicate sends = 0, post-purchase sends = 0 (a `SENT` whose cart had a purchase at least 1 s before the send), unexplained missing (expected − sent − skipped late − cancelled − dead) under 0.1%; machine cores and memory, noting loadgen shares the machine.

## 8. Documentation updates

- `DESIGN.md`: sections 4 to 7 updated for the dispatcher-side dedupe, single lateness checkpoint, Redis-rebuildable rule, monotonic timers, consistent reads, and the retry index; section 8 capacity corrected (Redis 5 to 6 GB; DynamoDB write figures); section 11 gains the infra adapter column and how to run it; a note that per-shopper capping would use a counter item, not re-keying; the "Demonstrated by" citations extended to the infra tests.
- `README.md`: the runbook in section 6.5.

## 9. Assumptions

- Clocks are NTP-synced with skew well under the smallest lateness bound; leases carry a 2× processing-time margin.
- Docker Desktop has about 6 GB of memory for the full stack.
- The notification gateway honours the idempotency key; it remains out of scope and is stood in for by the recording sink.

## 10. Out of scope

Real sends and gateway integration, provisioning real AWS (Terraform or CloudFormation), CI workflows, TLS and authentication, multi-region, schema registry (payloads stay JSON), Kafka transactions and exactly-once semantics (version checks and idempotency keys suffice), and the open business questions in `DESIGN.md` section 13.
