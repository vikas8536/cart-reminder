# Production Infrastructure: Design Spec

Date: 2026-09-25 (revision 5, 2026-09-26)
Status: approved in brainstorming; revisions 2 to 5 apply four rounds of independent principal-architect review (the last found no Critical issues); awaiting written review
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
| Worker to dispatcher | Kafka intent topics, a fast lane (early offsets) and a slow lane (the 24 hour offset), sharing one send budget with fast-lane priority | Decouples scheduling from sending; the next-day 24 hour burst cannot starve the higher-value early reminders, and no lane strands capacity the other could use. |
| Dedupe point | A fenced lease in the dispatcher's ledger row, taken for every send attempt including retries | Dedupe where the side effect happens; the fencing token makes a stale holder harmless. |
| Lateness | One deadline: the scheduler stamps `sendBy` into the intent; the dispatcher checks it before spending anything and again right before sending | A stamped deadline cannot drift with replica config, and late work never consumes send capacity. |
| Cancellation freshness | A per-partition event-time watermark from the detectors; the dispatcher checks it immediately before the claim and cart re-check, and the scheduler checks it before declaring a cart abandoned; a lagging partition pauses only its own slice | A consistent read cannot see a purchase still in Kafka; gating per partition keeps one slow partition from stopping everything. |
| Durability rule | Redis holds only rebuildable indexes and the watermark; DynamoDB and Kafka are the sources of truth | Losing Redis loses time, not work. |
| Detector write order | Timer first (Redis, monotonic), then the conditional cart update | A process crash leaves only a missing timer, so the reconciler needs only keys and its index is rarely written. Partial Redis loss can leave an older timer (repaired by the failover replay, §6.2); a DLQ'd cart update leaves a newer one that self-heals. |
| Retry scheduling | The ledger's sparse retry index, polled directly | Retry rows are rare; no second structure to rebuild. |
| Cart writes | Field-scoped conditional `UpdateItem`, no read-modify-write in the detector | Removes the lost-update race on `sequenceStarts` and the detector's read and conflict loop. |
| Timer store | Redis, not DynamoDB-embedded timers | Cheaper per upsert at 5k to 12.5k per second. |
| Partition key | `cartId` for every topic; the detector records each cart's actual `cart-events` partition (`srcPartition`) and the gates use it, never a recomputed hash | Per-cart ordering; guest login changes the shopper key, not the cart. The Cart Service is external and its client's partitioner may not be Java's murmur2, so recomputing would gate on the wrong partition. Equal `P` across topics only keeps blast radius aligned. A future per-shopper cap uses a per-shopper counter item, not re-keying. |
| Load test | Measure on local containers and report the bottleneck honestly | A laptop cannot sustain 12.5k per second through DynamoDB Local. |
| Concurrency | Per-batch processing on virtual threads, grouped by `cartId` in the consumers | Serial per-partition processing caps at about 64 × 1/15 ms ≈ 4.3k per second, below 12.5k (Little's law). |

## 3. Invariants

| Invariant | Mechanism | Test |
|---|---|---|
| No send after a purchase appended to Kafka more than `CLOCK_SKEW` before the send's watermark check; the remaining window is `CLOCK_SKEW` plus claim, cart read, and one gateway round trip | Watermark of the cart's recorded `srcPartition` checked immediately before the claim and consistent cart re-check (§5.4); assumes Cart Service publish latency under `CLOCK_SKEW` | Verifier "lagging detector holds the reminder"; e2e 7; load test post-purchase = 0 at the sink with threshold `CLOCK_SKEW + 1 s` |
| Watermark safety: every record appended to `p` before time `W[p]` (within `CLOCK_SKEW` of Redis time) has been processed and committed | `W[p]` is set to a poll-start time `T` only after the committed position reaches the end offsets snapshotted at `T`; monotonic within a consumer generation; older generations rejected while the entry is fresh; no write without a satisfied snapshot | Watermark contract tests (slow batch, zombie writer, failed snapshot, rebalance) |
| Gate liveness: an idle partition or a healthy system never pauses sending | Every detector loop iteration writes the watermark, including empty polls and backoff; an idle or caught-up partition satisfies its snapshot at once; staleness only after 5 s without writes | Watermark contract test; e2e 1 with 1 cart over 8 partitions |
| A partition held by the gate costs bounded work per held timer | Held `CHECK_ABANDON` timers are released for `clamp(dueAt + CLOCK_SKEW − W[p], 1 s, 60 s)`, 60 s when `W[p] = 0`; total load still grows with outage length (about 10k Redis ops/s after 10 minutes of all detectors stopped, about 60k after an hour) and recovery releases the held checks within about 60 s | Scheduler unit test; "stop all detectors" drill |
| No send after `sendBy` except for the in-flight gateway call | `sendBy` checked before the token, after the claim, and immediately before `send` | Verifier 11, 9b; dispatcher unit tests |
| A late intent never consumes send capacity | `sendBy` pre-check before taking a token | Dispatcher unit test |
| At most one send per key, except (a) a lease holder paused past its lease during the gateway call, and (b) a crash between `send` and `finish`; both resend under the same idempotency key, deduped by the gateway | Conditional ledger claim with a fencing token for every attempt; every transition conditioned on the token; no `send` starts with less than `GATEWAY_TIMEOUT` of lease left; the retry loop takes its token before claiming, so it never holds a lease while waiting | Ledger contract tests; dispatcher lease-check and retry skip-on-no-token unit tests; e2e 4, 5, 6; load test duplicates = 0 at the sink |
| No lost reminder on any single process crash | Timer lease redelivery; timer-first detector order; redelivered `CHECK_ABANDON` on an abandoned cart re-arms `REMINDER 0`; every non-final ledger row in the retry index with `nextAttemptAt = leaseUntil`; outcome and DLQ records produced before `finish`; reconciler | Crash-point unit tests; contract tests for lease expiry; e2e 6 |
| Full Redis loss loses no durable work. A partial loss (a restart or failover dropping about 1 s of writes) is repaired within one reconciler cycle: only a lost detector timer upsert can hide a cart (lost scheduler writes are redelivered, lost removes and reconciler writes are harmless), and the failover replay re-issues those upserts | Missing watermark means lagging (pause); missing `epoch` triggers the key-only diff; a changed Redis `run_id` or role (compared with the value stored in `recovery-meta`) triggers a replay of the last 60 s of `cart-events` into timer upserts; no work lives only in Redis | e2e 6 (FLUSHALL); contract test for a Redis restart that dropped a detector upsert; runbook drill |
| Per-cart state never regresses | Detector updates conditioned on `version < :v`; scheduler updates on `version = :v AND status = <expected>` | Cart store contract tests |
| Frequency cap holds | Only the scheduler writes `sequenceStarts`, conditioned on version and status | Verifier 14; contract test for a concurrent detector write |
| While a timer entry exists, it never regresses | Upsert only if `(version, offset)` is greater; equal data is a no-op keeping the lease; remove only if stored version ≤ given; a removed timer can be recreated by a late older upsert and is then dropped on fire | Timer store contract tests |
| A final ledger status stays final, except the deliberate `DEAD` → `RETRYING` of replay | Transitions conditioned on `status = SENDING AND leaseToken = :mine`; `reopen` only from `DEAD` | Ledger contract tests |
| Every `DEAD` row is replayable | DLQ record produced before `finish(DEAD)`; poison intents have no row and are excluded | Dispatcher crash-point unit test |
| Each abandonment is recorded once per arm | `ABANDONED` outcome emitted on first abandonment and on the redelivery branch, deduped by `(cartId, version)` downstream | Scheduler unit test |
| Each key resolves to exactly one outcome for accounting | Consumers resolve a key's outcomes by precedence `SENT > DEAD > CANCELLED > SKIPPED_LATE` | Load test accounting unit test |
| Holdout carts are never sent | Arm fixed on first write; policy checked on abandonment and on re-arm | Verifier 12 |
| All replicas agree on `S` and `P`, and every intent topic has `cart-events`' `P`; the gate's partition is recorded, never recomputed | `init` persists `S` and `P`; roles check `recovery-meta` and topic metadata and refuse to start on a mismatch; `srcPartition` flows cart record → timer → intent | Startup unit test; contract test with a non-murmur2 producer partitioner |

## 4. Architecture

```
                 cart-events (keyed by cartId, P partitions)
Cart Service ───────────────► [detector] ──► Redis timers (monotonic upsert or conditional remove, FIRST)
 (loadgen stands in)               │    └──► DynamoDB carts (conditional UpdateItem, version < :v)
                                   └──► Redis watermarks (per partition, every loop, generation-fenced)
                                                    │
                      [scheduler] ◄── claimDue(TIME), ack if unchanged
                           │ CHECK_ABANDON: gate on watermark[p], then consistent reload
                           │ REMINDER: consistent reload, stamp sendBy
                           ▼
          reminder-intents-fast / reminder-intents-slow (keyed by cartId, same P, carrying srcPartition)
                           │
                      [dispatcher] ── sendBy? → token → watermark[p]? → fenced claim → sendBy?
                           │          → consistent cart re-check → sendBy? → send
                           │  lagging partition p: pause intent partition p only
                           │  outcomes (and DLQ on death) produced before the ledger finish
                           │  transient → ledger RETRYING → retry loop polls the ledger retry index
                           └── reminder-dlq ◄── [replay] reopens ledger rows
                      [reconciler] ── on interval and on Redis loss: open cart keys vs Redis, rebuild missing timers;
                                    on Redis failover: replay recent cart-events into timer upserts
                      [loadgen]    ── produces events at a rate, reads sink-sends and outcomes, writes a report
```

### Roles

| Role | Job | Scales by |
|---|---|---|
| `init` | Create topics (equal `P`), tables, indexes, the meta item, and the Redis `epoch`; idempotent | One-off |
| `detector` | Consume `cart-events`, write timers then cart updates, publish watermarks every loop | Partitions and concurrency |
| `scheduler` | Claim due timers from all shards, run `ReminderScheduler`, publish intents, ack | Instances; claims are atomic |
| `dispatcher` | Consume both intent lanes, run `Dispatcher.handle`, run the retry loop | Partitions and concurrency |
| `reconciler` | Rebuild missing timers | One instance; a second is harmless |
| `replay` | Read `reminder-dlq`, reopen ledger rows | One-off |
| `loadgen` | Generate workload, observe sends and outcomes, write the report | One-off |

`--mode=inmemory` is the default and runs the existing fake-clock demo. Infra roles use `SystemClock`.

## 5. Data model

Conventions: epoch-millisecond timestamps, JSON payloads (Jackson, `FAIL_ON_UNKNOWN_PROPERTIES` off, every payload carries `schemaVersion`). `S` shards and `P` partitions are configurable (64 production, 8 local) and persisted by `init`. `shard(cartId) = floorMod(cartId.hashCode(), S)`. A cart's `srcPartition` is the `cart-events` partition the detector actually consumed it from, stored on the cart record and carried into timers and intents; nothing recomputes it.

### 5.1 Kafka

| Topic | Key | Value | Retention |
|---|---|---|---|
| `cart-events` | cartId | `{schemaVersion, type, cartId, shopperKey, firstName?, version, occurredAt, items[]}` | 7 days |
| `cart-events-dlq` | cartId | original bytes; headers `error`, `source-offset` | 30 days |
| `reminder-intents-fast` | cartId | `{schemaVersion, key, cartId, version, offsetIndex, srcPartition, scheduledFor, sendBy}` for `offsetIndex < FAST_OFFSETS` (default 2) | 7 days |
| `reminder-intents-slow` | cartId | same, for `offsetIndex ≥ FAST_OFFSETS` | 7 days |
| `reminder-dlq` | cartId | intent plus `{reason, failedAt}`; reason `poison` for undeserializable intents | 30 days |
| `reminder-outcomes` | cartId | `{schemaVersion, key?, cartId, version, arm, kind, at, attempts}`, kind one of `ABANDONED` (per-arm dataset for the control-group metric), `SENT`, `SKIPPED_LATE`, `CANCELLED`, `DEAD` | 7 days |
| `sink-sends` | cartId | one record per `send()` call at the recording sink: `{key, cartId, at, hasFirstName, itemCount}` | 7 days |

`cart-events` and both intent topics have the same `P`. The watermark needs no record timestamps and no change to `cart-events` settings (§5.4), so the topic can stay owned and configured by the Cart Service. Outcome consumers resolve each key by precedence `SENT > DEAD > CANCELLED > SKIPPED_LATE`, because a pre-check skip of a duplicate intent can emit `SKIPPED_LATE` for a key already `SENT`. Intents carry no items or names; the dispatcher builds the message from its consistent cart read, which is the send-time personalization refresh DESIGN §10 describes. `sink-sends` carries no personal data.

Broker: `auto.create.topics.enable=false`; replication factor and `min.insync.replicas` from config (1/1 local, 3/2 production); single-node KRaft also sets `offsets.topic.replication.factor=1` and `transaction.state.log.replication.factor=1`. Producers: `acks=all`, `enable.idempotence=true`. Consumers: `group.protocol=classic` (the watermark fencing relies on classic generation ids, which KIP-848 member epochs do not replace), manual commits after processing, commit on partition revoke and on shutdown.

### 5.2 Redis (rebuildable only, AOF on)

| Key | Type | Content |
|---|---|---|
| `timers:{s}` | sorted set | member cartId, score due or lease-expiry time |
| `timerdata:{s}` | hash | cartId → `kind\|version\|offsetIndex\|srcPartition\|dueAt` |
| `watermarks` | hash | partition → `generation\|eventTime\|updatedAt` |
| `epoch` | string | sentinel written by `init` and the reconciler; missing means Redis lost data |

The Redis `run_id` and replication role last seen by the reconciler are stored in `recovery-meta`, not in Redis or process memory, so a reconciler restart does not look like a failover; a change means a restart or failover that may have dropped recent writes.

The `{s}` hash tag keeps each shard's keys in one Cluster slot, so Lua scripts stay cluster-safe (64 tags spread slightly unevenly over nodes; harmless at this size). Scripts take time from Redis `TIME`, never the client clock:

- `upsert(timer)`: writes only if the new `(version, offsetIndex)` is greater than the stored one, `CHECK_ABANDON` counting as −1; equal data is a no-op that keeps any lease score.
- `claim(n, leaseMs)`: takes up to `n` members with score ≤ `TIME` and sets their score to `TIME + lease`, returning their data.
- `release(cartId, data, delayMs)`: if the data is unchanged, sets the score to `TIME + delay`; used when a claimed timer must wait for the watermark.
- `ack(cartId, data)`: removes only if the stored data still equals the claimed data.
- `remove(cartId, version)`: removes only if the stored version is ≤ `version`.
- `wmSet(p, generation, eventTime)`: rejects a lower `generation` than stored while the stored entry is fresh (`TIME − updatedAt ≤ 5 s`); over a stale entry a lower generation is accepted, so a consumer-group reset (generations restart low) cannot pause a partition forever, and this is safe because a writer's `T` is always backed by its own commit; for the same generation stores `max(stored, eventTime)`; a higher generation overwrites; `updatedAt = TIME`.
- `wmGet(p)`: returns `eventTime` and `TIME`, with `eventTime = 0` if the field is missing or `TIME − updatedAt > 5 s`. After a `FLUSHALL` the first `wmSet` for a partition is accepted from any generation, so a zombie writer can win for one loop iteration (about 500 ms) before the owner's next write; negligible, documented.

A late upsert for an older version after a removal can recreate a timer; it fires, fails the version compare, and is dropped. Documented, not tombstoned.

### 5.3 DynamoDB

**`recovery-meta`**, one item: `shards`, `partitions`, `paused` (the guardrail pause switch, read by dispatchers every 5 s), `redisRunId`, `redisRole`, and `redisChangeAt` (the earliest unrepaired change, for the failover replay).

**`carts`**, partition key `cartId`:

- `shopperKey, firstName, status, version, lastActivityAt, items (at most 50), arm, sequenceStarts (pruned to the frequency window), srcPartition, ttl`
- `openShard` (`"s#<n>"`) and `openUntil` = `lastActivityAt + lastOffset + lastLatenessBound`, rounded up to the next hour. Present while the cart has a next step; removed on `CLOSED`, when the cart is abandoned but ineligible (holdout or capped), when the last offset is published, and by the reconciler when no on-time offset remains.
- Sparse GSI `open-by-shard`: partition `openShard`, sort `openUntil`, projection `KEYS_ONLY`. Only changes to `openShard` or the hour-rounded `openUntil` write it, so a cart moves in the index at most a few times per sequence.
- `ttl` = `lastActivityAt + 30 days`, longer than the frequency window and Kafka retention.
- Detector edit or resume: `UpdateItem SET status=ACTIVE, version, lastActivityAt, items (edit only), firstName, srcPartition, shopperKey and arm if not set, openShard, openUntil, ttl`, condition `attribute_not_exists(cartId) OR version < :v`, `ReturnValues ALL_NEW`. Purchase or clear: `SET status=CLOSED, version REMOVE openShard, openUntil`, same condition. A failed condition is a stale or duplicate event.
- Scheduler abandonment: `SET status=ABANDONED, sequenceStarts = :pruned+new` (plus `REMOVE openShard` when ineligible), condition `version = :v AND status = ACTIVE`. End of sequence: `REMOVE openShard`, condition `version = :v AND status = ABANDONED`.
- Base-table reads by the scheduler, dispatcher, and reconciler use `ConsistentRead=true`; GSI reads are eventually consistent, which is safe because every consumer of a GSI result reloads or conditions its write.

**`send-ledger`**, partition key `cartId`, sort key `sk = "<version, 20 digits>#<offset, 2 digits>"`, rows about 200 bytes:

- `status`: `SENDING` (with `leaseToken`, `leaseUntil`), `RETRYING`, final `SENT`, `SKIPPED_LATE`, `CANCELLED`, `DEAD`
- `sendBy`, `srcPartition`, `attempts`, `nextAttemptAt` (for `SENDING` equal to `leaseUntil`), `reason`, `ttl` of 30 days. No intent payload.
- `retryShard` present for every non-final status; sparse GSI `retrying-by-shard`: partition `retryShard`, sort `nextAttemptAt`, projection `INCLUDE (srcPartition)`; `srcPartition` never changes on a row, so the projection adds no index writes.
- Claim (one conditional write, `ReturnValues ALL_NEW`): create as `SENDING` with `attempts = 1` if absent; or take over if `RETRYING AND nextAttemptAt <= :now`, or `SENDING AND leaseUntil <= :now`, incrementing `attempts`. Every claim sets a fresh `leaseToken`. Later transitions are conditioned on `status = SENDING AND leaseToken = :mine`; a failed condition means the lease was lost, counted as `dispatch.lease_lost`, and the attempt stops. `:now` is the dispatcher's clock; the 3× lease margin absorbs clock skew well beyond `CLOCK_SKEW`.
- Highest offset for a version: query `begins_with(sk, "<version>#")`, descending, limit 1.

### 5.4 Watermark

- **Write (end-offset snapshots).** At the start of a poll-loop iteration the detector reads Redis `TIME` as `T` and snapshots `E = endOffsets(assigned)` from the broker, at most once per 250 ms (a failed or timed-out call reuses no snapshot and takes nothing). It keeps the latest few snapshots. At the end of every iteration, including empty polls and iterations spent backing off, for each assigned partition `p` it finds the newest snapshot `(T, E)` with committed position ≥ `E[p]` and calls `wmSet(p, generation, T)`; if no snapshot is satisfied, it writes nothing, and the entry goes stale after 5 s and the partition pauses. `generation` is the classic consumer group generation id, which rises on every rebalance.
- **Why this is safe.** Every record appended to `p` before `T` lies below `E[p]`, so once the committed position reaches `E[p]`, all of them have been processed. This holds by construction: no cached lag, no record timestamps, no dependency on the Cart Service's topic settings or clock. A detector cut off from the broker cannot take new snapshots, so its watermark goes stale. A zombie detector that lost `p` has an older generation and is rejected while the owner keeps the entry fresh; over a stale entry any writer's `T` is still backed by its own committed position. A new owner starting behind simply has no satisfied snapshot until it catches up. The remaining assumption is Cart Service publish latency under `CLOCK_SKEW`, and `T` is Redis time, so safety holds within `CLOCK_SKEW` of the broker's clock.
- **Why it stays live.** An idle or caught-up partition satisfies each new snapshot at once, so it reads as current within one iteration (poll timeout 500 ms). Under load the watermark trails by one or two iterations. A detector backing off from a DynamoDB error keeps writing whatever snapshots its committed position satisfies. A partition goes stale only after 5 s without writes, meaning its detector is dead, cut off from the broker, or behind.
- **Scheduler gate.** For a claimed `CHECK_ABANDON` with `wmGet(srcPartition) < dueAt + CLOCK_SKEW` (default 5 s), `release` it for `clamp(dueAt + CLOCK_SKEW − W, 1 s, 60 s)`, or 60 s when `W = 0`, instead of processing. The growing delay keeps a stuck partition from turning its held timers into a Redis busy-loop; total load still grows with the number of held timers (about 10k ops/s after 10 minutes with all detectors stopped, about 60k after an hour), and on recovery the held checks land within about 60 s and are absorbed by scheduler throughput and throttle retries. Reminder timers are not gated; the dispatcher is. A cart record without `srcPartition` (written before the field existed) gates on the minimum watermark over all partitions.
- **Dispatcher gate.** Right before the claim and cart re-check (see §6.2), require `wmGet(srcPartition) ≥ TIME − CLOCK_SKEW`, using the intent's recorded `srcPartition`. If not: for a consumed intent, pause the intent's own partition in that lane, seek it back to the held record, and commit that partition only up to the lowest held offset (records after it that already completed are redelivered and end as `NotClaimed`); for a retry row, skip it this round. Paused partitions are re-checked every loop and resumed when caught up.
- **Blast radius.** A dead detector, a rebalance, or a throttled partition pauses only the carts whose `srcPartition` it owns, about `1/P` of traffic, for its duration. If the Cart Service's partitioner differs from Java's, those carts may be spread over several intent partitions, so a pause can hold some unrelated carts too; correctness does not depend on the mapping.
- **In memory.** One partition; the detector is synchronous so the watermark is the clock, unless the `Pipeline` detector-lag option buffers events.

### 5.5 Write and read cost at 5k events per second (12.5k spike, 945 reminders per second, about 2.4k per second in the next-day burst)

| Store | Load | Figure |
|---|---|---|
| `carts` base writes | 1 UpdateItem per event at 1 to 3 WCU (item size with up to 50 items), plus 2 per abandonment | about 10k WCU/s baseline, about 25k spike |
| `carts` GSI writes | only when `openShard` or hour-rounded `openUntil` changes: cart open, hour boundary during a session, abandonment end, close; about 2 to 3 index writes per cart | about 1.5k to 2.5k WCU/s baseline; provision the GSI with headroom because a throttled GSI back-pressures base-table writes |
| `carts` reads | Consistent GetItem per timer fire and per dispatch attempt; the detector does not read | about 3k to 6k RCU/s |
| `send-ledger` | claim + GSI insert + finish + GSI delete = 4 WCU per reminder, plus failed conditional claims from competing retry pollers | about 3.8k WCU/s baseline, about 9.6k in the next-day burst |
| Redis | 1 script per event plus claims, acks, and watermark reads and writes | about 8k to 18k ops/s across the cluster; about 5 to 6 GB for about 28 to 32M timers, about 90 MB per shard |
| Reconciler | `open-by-shard` holds in-flight carts only (about 1.8M active + about 28M mid-sequence); KEYS_ONLY is about 60 bytes per entry, about 1.9 GB and about 0.24M eventually consistent RCU per sweep, plus a consistent BatchGetItem only for carts whose timer is missing | about 800 RCU/s at the default 5 minute interval; sweep time bounded by GSI partition throughput, about 30 to 80 s with one or two partitions, less as the index splits |
| Failover replay | re-read the last 60 s of `cart-events` and re-issue timer upserts, only after a Redis restart or failover | at most about 750k Redis upserts per failover, no DynamoDB reads |

On-demand capacity absorbs about 2× the previous peak instantly; the 2.5× spike needs warm throughput or provisioned capacity set in advance.

## 6. Ports and core changes

Core classes stay shared by both modes. Every port change lands in both the in-memory and the infra adapter.

### 6.1 Ports

| Port | Contract | In-memory | Infra |
|---|---|---|---|
| `Clock` | `now()` | `FakeClock` | `SystemClock` |
| `CartStateStore` | `get(cartId)` and `getAll(cartIds)` (consistent), `applyEvent(event, arm, srcPartition) → Optional<CartRecord>` (empty when stale), `markAbandoned(record, starts, eligible) → boolean`, `endSequence(cartId, version) → boolean`, `openCartIds(shard, now) → Stream<String>` | map | DynamoDB `carts` + `open-by-shard` |
| `TimerStore` | `upsert(timer) → boolean`, `remove(cartId, version)`, `claimDue(limit)`, `release(timer, delay)`, `ack(timer)`, `existing(shard, cartIds) → Set` | map + priority queue with real leases on the fake clock | Redis scripts |
| `Watermark` | `publish(partition, generation, eventTime)`, `current(srcPartition) → Instant`, `now() → Instant` (the watermark's time source) | the clock, or the buffered detector's position | Redis `watermarks` |
| `IntentPublisher` | `publish(intent)`, blocking until acknowledged, lane chosen by `offsetIndex ≥ FAST_OFFSETS` | queue drained by `Pipeline` | Kafka producer |
| `SendLedger` | `claim(key, sendBy, srcPartition, now) → Claimed(token, attempts, sendBy, srcPartition) \| NotClaimed(reason)`, `markRetry(key, token, nextAt) → boolean`, `finish(key, token, outcome, reason) → boolean`, `dueRetries(shard, now, limit) → (key, srcPartition)` pairs, `reopen(key, now) → boolean`, `highestOffsetIndex(cartId, version)` | map + retry priority queue with real leases and tokens | DynamoDB `send-ledger` + `retrying-by-shard` |
| `NotificationSink` | `send(message) → SendResult` | `RecordingNotificationSink` | recording sink producing to `sink-sends`, optional injected transient failure rate |
| `OutcomeRecorder` | `record(outcome)` | list | Kafka `reminder-outcomes` |
| `DeadLetterQueue` | `add(letter)`; replay reads its own consumer | list with `drain()` | Kafka `reminder-dlq` |
| `ArmAssigner` | unchanged | | |

The `Outbox` port, `InMemoryOutbox`, a Redis retry queue, and any retry-index rebuild do not exist.

### 6.2 Core classes

- **`AbandonmentDetector`**: for an edit or resume, `upsert(CHECK_ABANDON)` first, then `applyEvent`; for a purchase or clear, `remove(cartId, version)` first, then `applyEvent`. A stale event leaves at most a timer that is rejected by the monotonic upsert or later dropped on fire; a crash between the two writes is redelivered. A stale `applyEvent` counts `events.ignored`. A cart update that fails deterministically and goes to the DLQ leaves a timer newer than the record; it fires as stale and the cart is repaired by the next sweep. The detector passes the record's actual source partition to `applyEvent`, and every timer it writes carries it.
- **`ReminderScheduler`** (per claimed timer; the caller acks after processing; a deterministic error such as a missing offset after the offset count shrank is acked and counted `timers.poison`; other exceptions leave the timer un-acked for lease redelivery):
  - `CHECK_ABANDON`: watermark gate (§5.4), then consistent reload; version mismatch → `timers.stale`.
    - On `ACTIVE`: compute pruned `sequenceStarts` + this start and eligibility; `markAbandoned` (a failed condition means a newer event won: drop); record an `ABANDONED` outcome; eligible → upsert `REMINDER 0`, otherwise the same write removed `openShard`.
    - On `ABANDONED` at the same version (a redelivery after a crash between writes): record the `ABANDONED` outcome again (deduped downstream by `(cartId, version)`); if eligible, upsert `REMINDER 0`, idempotent through the monotonic upsert.
  - `REMINDER i` on `ABANDONED` at the same version: publish `{key, cartId, version, i, srcPartition, scheduledFor, sendBy = scheduledFor + bound[i]}`; if `i + 1` exists, upsert it, otherwise `endSequence`.
  - Any other status → `timers.wrong_status`.
- **`Dispatcher.handle(intent)`**, in this order:
  1. `now > sendBy` → record a `SKIPPED_LATE` outcome, count `dispatch.skipped_late_precheck`, and stop (no token, no ledger write; a redelivery or reconcile rebuild finds it late again, and outcome precedence resolves duplicates, §5.1).
  2. Take a token from the shared budget without waiting (fast lane with priority, §6.3); no token → hold the partition exactly as in step 3 and stop.
  3. Watermark gate for the intent's `srcPartition`; lagging → hold the partition (§5.4) and stop.
  4. `claim(key, sendBy, srcPartition, now)`; `NotClaimed` → count by reason (`dispatch.duplicate`), stop.
  5. `now > sendBy` → record `SKIPPED_LATE`, `finish`.
  6. Consistent cart read; not `ABANDONED` at the intent's version → record `CANCELLED`, `finish`.
  7. `now > sendBy` → record `SKIPPED_LATE`, `finish`. If `leaseUntil − now < GATEWAY_TIMEOUT`, stop without sending and count `dispatch.lease_expiring`; the row stays `SENDING` and the retry loop takes it over after the lease. Otherwise build the message (first name, items) and `send`:
     - `SENT`: record `SENT`, `finish(SENT)`.
     - Transient with `attempts < MAX_SEND_ATTEMPTS`: `markRetry` at `now + random(0, RETRY_BASE × 2^(attempts−1))` (full jitter).
     - Transient exhausted or permanent: produce the DLQ record, record `DEAD`, `finish(DEAD)`.
  - Every outcome and DLQ record is produced before its `finish`; duplicates from a crash in between are harmless (`ABANDONED` is deduped by `(cartId, version)`, reminder outcomes are resolved per key by precedence `SENT > DEAD > CANCELLED > SKIPPED_LATE` (§5.1), and `reopen` only moves `DEAD`).
  - A `false` from `markRetry` or `finish` counts `dispatch.lease_lost` and stops.
  - The lane is chosen from `offsetIndex ≥ FAST_OFFSETS`; the dispatcher never validates `offsetIndex` against `OFFSETS`, because `sendBy` already carries the deadline.
- **Retry loop** (every `RETRY_POLL`, default 1 s, while not paused): each replica starts at a random shard and walks all shards; per due `(key, srcPartition)` from `dueRetries`: watermark gate on `srcPartition` (skip if lagging) → non-blocking `tryAcquire` of a token for the key's lane (skip the key this round if none; the row stays in the index) → `claim` (the conditional write settles races between replicas) → steps 5 to 7. Taking the token before the claim means the loop never holds a lease while waiting, so the step 7 lease check rarely fires and `attempts` is not inflated by lease expiries.
- **Bounded waste**: a token taken before a gate hold (step 3) or a `NotClaimed` (step 4) is not returned; at most `max.poll.records` tokens per pause or duplicate burst, stated rather than engineered away.
- **Circuit breaker**: over the last 100 send attempts, a transient-failure ratio above 50% opens the breaker for 30 s: consumers and the retry loop pause, then one probe send decides. The guardrail switch `recovery-meta.paused` pauses the same way. Both reuse Kafka `pause`/`resume`.
- **Replay**: for each `reminder-dlq` record except reason `poison`, `ledger.reopen(key)` moves `DEAD` to `RETRYING`, due now, attempts reset. Replaying twice is harmless. A replay past `sendBy` is skipped as late.
- **`Reconciler`**: per shard in parallel on virtual threads, stream `openCartIds(shard, now)`, check `existing(shard, cartIds)` in pipelined batches, and only for cart ids with no timer: `getAll` (consistent), then `ACTIVE` → upsert `CHECK_ABANDON`; eligible `ABANDONED` → upsert the first offset after the ledger's highest that is still within its lateness bound, or `endSequence` if none remains; `CLOSED` or gone → nothing. Runs every `RECONCILE_INTERVAL` (default 5 min) and immediately when the Redis `epoch` sentinel is missing, then rewrites it.
  - **Failover replay.** Each cycle the reconciler compares Redis `run_id` and replication role with the values stored in `recovery-meta`. On a change (a restart or failover that may have dropped about 1 s of writes), it reads `cart-events` from `offsetsForTimes(changeDetectedAt − 60 s)` with a plain consumer (no group) up to the current end offsets, and re-issues `upsert(CHECK_ABANDON)` for every edit or resume, with the record's partition as `srcPartition`; then it stores the new `run_id` and role. Only a lost detector upsert can hide a cart: lost scheduler writes are redelivered, and lost removes or reconciler writes are harmless. The monotonic upsert makes the replay safe, and a recreated older timer is dropped on fire. If `run_id` changes again during the replay, it restarts from the earliest detected change time. Cost: at most 60 s × 12.5k = 750k Redis upserts and no DynamoDB reads.
  - Logs sweep duration; a sweep longer than the smallest lateness bound is reported on `/ready`. No lock: rebuilding is idempotent.
- **Model**: `CartEvent` and `CartRecord` gain an optional `firstName`; `CartRecord` prunes `sequenceStarts` and caps items at 50. `Metrics` becomes thread-safe (`ConcurrentHashMap` + `LongAdder`).
- **`Pipeline` (in-memory)**: drains intents into `dispatcher.handle` after each timer fire; `advanceTo` also stops at due retry times; options for a detector lag (buffered events, watermark behind) and a dispatch delay.

Metric changes: `reminders.scheduled` → `reminders.published`, `reminders.duplicate_timer` → `dispatch.duplicate`, `reminders.skipped_late` → `dispatch.skipped_late`; new `dispatch.skipped_late_precheck`, `dispatch.lease_lost`, `dispatch.lease_expiring`, `dispatch.breaker_open`, `timers.poison`, `watermark.lag_ms` per partition.

### 6.3 Concurrency and failure handling

- Consumer roles: one poll thread per consumer; each batch grouped by `cartId`; groups run concurrently on virtual threads, events within a group in order; after the batch, commit each partition up to its lowest held or unfinished offset.
- Scheduler: claimed timers run concurrently on virtual threads (one timer per cart); `publish` blocks until the broker acks; ack after processing.
- Send budget: one token bucket per replica at `MAX_SEND_RATE`; the fast lane takes any available token, the slow lane takes one only while more than `FAST_RESERVE` (default 30% of the bucket) remain. Rates are per replica; the global rate is replicas × rate.
- Dispatcher: one consumer and poll thread per lane, so a batch never mixes lanes; small `max.poll.records`; the fast consumer pauses its partitions when the bucket is empty, the slow consumer pauses while the bucket is at or below `FAST_RESERVE`, and neither blocks; the breaker or the guardrail switch pauses both; a lagging watermark pauses only the held partition; polling continues so blocking never exceeds `max.poll.interval.ms`.
- Failure classification:
  - Deterministic (bad JSON, unknown event type, DynamoDB `ValidationException` or `SerializationException`, an allow-list; any other 400 such as `ResourceNotFoundException` or `AccessDeniedException` is an infrastructure fault and is transient): consumers send the record to the topic's DLQ and commit; the scheduler acks the timer and counts `timers.poison`.
  - Transient (I/O, throttling, retryable SDK errors): retry the failed groups in process with backoff, then seek back to the last committed offset with exponential backoff capped at 30 s. The detector keeps writing watermarks while it backs off (§5.4).
- Thread safety: core classes are stateless beyond ports; AWS SDK clients, the Kafka producer, and Lettuce are thread-safe; the Kafka consumer is used only by its poll thread; `Metrics`, the bucket, and the breaker are thread-safe.
- Client sizing: in-flight work per role is bounded by `MAX_IN_FLIGHT` (default 256); the DynamoDB HTTP client's `maxConnections` matches it (the SDK default of 50 would cap a JVM near 5k events per second). Integration tests run with `-Djdk.tracePinnedThreads=full` and fail on any pinning report, including frames inside the SDK, Kafka, or Lettuce.
- Lease rule: `LEASE ≥ 3 × GATEWAY_TIMEOUT`, validated at startup. Retry spacing: startup logs the effective attempt count per offset given its lateness bound (with defaults, about 3 for the 5 minute bounds).
- Shutdown: on SIGTERM, stop polling, finish in-flight groups of the current batch, commit what completed, abandon the rest uncommitted (redelivered later), close clients, within 30 s. A consumer with all its partitions paused has nothing in flight and simply commits and closes; otherwise the rule above applies.

## 7. Runtime and operations

### 7.1 Configuration

Environment variables, validated at startup (invalid config exits with a clear message); every role logs a hash of its effective config; roles refuse to start if `SHARDS` or `PARTITIONS` differ from `recovery-meta` or any topic's partition count differs from `PARTITIONS`.

- Recovery: `WINDOW`, `OFFSETS`, `LATENESS_BOUNDS` (ISO-8601 durations), `FREQUENCY_CAP`, `FREQUENCY_WINDOW`, `HOLDOUT_PERCENT`, `MAX_SEND_ATTEMPTS`, `RETRY_BASE`, `FAST_OFFSETS`
- Infra: `KAFKA_BOOTSTRAP`, `REDIS_URL`, `DYNAMO_ENDPOINT` (set only for DynamoDB Local), `SHARDS`, `PARTITIONS`, `REPLICATION_FACTOR`, `MIN_INSYNC_REPLICAS`
- Runtime: `LEASE`, `GATEWAY_TIMEOUT`, `MAX_SEND_RATE`, `FAST_RESERVE`, `SEND_FAILURE_RATE` (load testing only), `RECONCILE_INTERVAL`, `RETRY_POLL`, `MAX_IN_FLIGHT`, `CLOCK_SKEW`

Recovery configuration changes apply to carts as their timers are next handled; `sendBy` already stamped on an intent is not recomputed. Changing `PARTITIONS` requires recreating the topics together, which `init` refuses to do on a running system.

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

- `/health` (JDK `com.sun.net.httpserver`): 200 while the role's loop threads are alive and have iterated within 3× their poll interval, including while backing off or paused; the compose healthcheck, so a dependency outage never causes restarts.
- `/ready`: dependency state, per-partition watermark lag, breaker and pause state, reconciler sweep duration, and a stuck-partition signal (committed offset unchanged for 5 minutes while lag > 0).
- `/metrics`: counters as text; also logged every 10 s.
- The scheduler idles 200 ms between empty claim rounds; the detector's poll timeout is 500 ms.

### 7.5 Runbook (README)

1. In-memory: `./gradlew test`, `./gradlew run`.
2. Infra: `docker compose up -d --build`, `docker compose logs -f`.
3. Visible demo: `demo.env`, then `docker compose --profile load run loadgen --rate=50 --duration=60s`.
4. Drills with expected outcomes: kill a scheduler mid-run (leased timers redelivered); `redis-cli FLUSHALL` (dispatchers pause until watermarks return within a second, the reconciler rebuilds timers at once); stop one detector (its partitions pause for the rebalance, the rest keep sending); stop all detectors (all sending pauses; nothing is sent against stale state); stop dispatchers past the lateness bound (backlog skipped at the pre-check, no send capacity spent); `SEND_FAILURE_RATE=0.3` (retries, DLQ, then `replay` recovers); set `recovery-meta.paused` (sending stops within 5 s).
5. Replay: `docker compose run --rm dispatcher --role=replay`.

## 8. Testing

### 8.1 Layers

| Layer | Command | Needs | Proves |
|---|---|---|---|
| Unit + fake-clock verifier | `./gradlew test` | JDK | Exact timing and all business rules; the source of truth for when |
| Contract tests | in-memory in `test`, infra in `integrationTest` | Docker for infra | Adapter parity: monotonic upsert and equal-data no-op, conditional remove, claim, release, lease expiry, ack-if-unchanged; watermark generation fencing, max-within-generation, staleness, stamping only from a satisfied end-offset snapshot (a failed snapshot or a behind new owner writes nothing); ledger claim, takeover with attempt increment, stale-token rejection; cart conditional updates including a concurrent detector write during abandonment |
| Infra end-to-end | `./gradlew integrationTest` | Docker | Wiring and failure semantics with roles as in-process threads |
| Load test | `docker compose --profile load run loadgen` | Docker | Throughput, lag, latency, and correctness under load |

`integrationTest` is a separate source set using `@Testcontainers(disabledWithoutDocker = true)`; `./gradlew check` runs both. Containers are shared; each test uses its own cart-id prefix and filters by it. Infra tests assert order, counts, and outcomes, never timestamps; timings are seconds-scale with lateness bounds at least 10× expected jitter; waiting uses an in-repo `await(condition, timeout)` helper. Crash points are proven deterministically in unit and contract tests; killing real processes is a runbook drill.

### 8.2 New verifier scenarios

- A lagging detector holds the reminder: a purchase buffered behind a lagging detector means the due reminder is not sent while the watermark lags, and is cancelled once the detector catches up.
- A redelivered `CHECK_ABANDON` after a crash between the abandonment write and the reminder upsert still sends `REMINDER 0` and records one `ABANDONED` outcome after dedupe.
- A dispatch delay longer than the lateness bound skips the reminder at the pre-check, spending no token.

### 8.3 Infra end-to-end tests (at most 7, each under 30 s)

1. Happy path, one cart over 8 partitions: three sends in order with the expected keys (also proves idle partitions do not pause the gate).
2. Purchase mid-sequence: no sends after the purchase.
3. Duplicate and out-of-order events: exactly one send per key.
4. Two schedulers and two dispatchers on shared shards: no duplicate sends across 200 carts, counted at `sink-sends`.
5. `SEND_FAILURE_RATE=0.3` (below the breaker threshold), 2 max attempts: some sends after a retry, some dead-lettered; `replay` then sends each dead key exactly once.
6. Redis `FLUSHALL` mid-sequence: dispatchers pause briefly, the reconciler rebuilds, the remaining sends happen with no duplicates.
7. Detector stopped, purchase produced, reminder due: no send; detector restarted: the reminder is cancelled.

### 8.4 Existing test updates

Duplicate-timer handling moves to the dispatcher claim: scenario 6 and `ReminderSchedulerTest` duplicate cases assert `dispatch.duplicate`, with unchanged send counts. Lateness moves to the dispatcher: scenario 11 asserts the late reminders are skipped at the pre-check (`dispatch.skipped_late_precheck`) and the ledger has one row. `DispatcherTest` is rewritten around `handle` and the retry loop, keeping its cases and adding fencing, breaker, token-order, and produce-before-finish crash points. `AbandonmentDetectorTest` asserts the timer-first order and drops the conflict-retry expectation. New unit tests: monotonic upsert, conditional remove, ledger state machine with fencing, the step 7 lease check, watermark write rules and gate, the growing release delay, failure classification, shared bucket priority and per-lane pausing, outcome precedence, commit-at-lowest-held-offset. A contract test produces `cart-events` with a non-murmur2 partitioner and checks that the gate follows the recorded `srcPartition`. A contract test restarts the Redis container with AOF after dropping one detector timer upsert (so `run_id` changes) and asserts the failover replay restores the cart's timer. Dispatcher unit tests cover the retry loop skipping a key when no token is available, and step 2 holding the partition when the bucket is empty.

### 8.5 Load test

- Workload: simulated shoppers, about 10 events per cart session, about 70% abandon, the rest purchase at a random point, some resume; `--rate` (default 5000) for `--duration`, compressed timings; stays under the frequency cap.
- Drain wait after producing: window + last offset + lateness bound + one reconcile interval.
- Sampled every 5 s: achieved produce rate, consumer lag per group, per-partition watermark lag, Redis timer backlog past due.
- Sends read from `sink-sends` (one record per `send()` call); accounting read from `reminder-outcomes`, each key resolved by precedence `SENT > DEAD > CANCELLED > SKIPPED_LATE`; both filtered to the run.
- Expected sends computed by loadgen from each cart's script, using the same `HashArmAssigner` for holdout.
- Report (console and `build/reports/load/<timestamp>.md`): achieved versus target rate; maximum and ending lag per group, naming the bottleneck stage; scheduled-to-sent latency p50/p95/p99 per lane, excluding a 30 s warm-up; expected, sent, skipped late, cancelled, dead-lettered; correctness at the sink: duplicate sends = 0, post-purchase sends = 0 (a send whose cart had a purchase with `occurredAt` more than `CLOCK_SKEW + 1 s` before it), unexplained missing (expected − sent − skipped late − cancelled − dead) under 0.1%; machine cores and memory, noting loadgen shares the machine.

## 9. Documentation updates

- `DESIGN.md`: sections 4 to 7 for the fenced ledger claim, the single lateness deadline, the per-partition watermark gate, the timer-first detector order, the Redis-rebuildable rule, monotonic timers, and field-scoped cart updates; section 9 for the fast and slow lanes on a shared budget; section 7 for the circuit breaker, jitter, and poison handling; section 3 for the guardrail pause switch and the `ABANDONED` per-arm outcome record; section 10 for first-name personalization built at send time; section 8 capacity from §5.5; section 11 gains the infra adapter column and how to run it; a note that per-shopper capping would use a counter item, not re-keying; the "Demonstrated by" citations extended to the infra tests.
- `DESIGN.md` §13 gains: how long does the gateway honour an idempotency key? The design depends on it for the two at-most-once exceptions in §3.
- `README.md`: the runbook in §7.5.

## 10. Assumptions

- Clocks are NTP-synced with skew under `CLOCK_SKEW`, and the Cart Service's publish latency (event to broker append) stays under `CLOCK_SKEW`. The watermark uses end offsets and Redis time, so the Cart Service's clock and topic timestamp settings do not matter.
- The Cart Service may use any Kafka client and partitioner; the design records the actual partition instead of assuming murmur2. Changing that partitioner on a live topic needs a drain first: afterwards a cart's new events can land in a different partition from its recorded `srcPartition` until its next event updates it.
- Docker Desktop has about 6 GB of memory for the full stack.
- The notification gateway honours the idempotency key for at least the longest lateness bound; it remains out of scope and is stood in for by the recording sink.

## 11. Out of scope

Real sends and gateway integration, provisioning real AWS (Terraform or CloudFormation), CI workflows, TLS and authentication, multi-region, schema registry (payloads stay JSON with `schemaVersion`), Kafka transactions and exactly-once semantics, timing-variant experiment arms (a single global `OFFSETS`; DESIGN §10 describes arms as future work), and the open business questions in `DESIGN.md` section 13.
