# Review fixes 1–3: watermark liveness, observable misses, dispatcher capacity

Date: 2026-09-28. Source: the principal-architect review of the production build (fixes 1–3 of its top five).
Builds on `2026-09-25-production-infra-design.md` (revision 5); where this spec differs, this spec wins.

## Goal

Three defects the review found in the built system:

1. **Watermark collapse.** A detector more than about 2 s behind stops publishing (`WatermarkSnapshots` ignores snapshots older than 2 s), so its partitions read as stale (`EPOCH`), the scheduler holds abandonment checks for a flat 60 s, and the dispatcher stops sending for those partitions. A lag spike becomes a halt, the main cause of the 22.6% skipped-late in the 250/s load run.
2. **Unobservable misses.** A reminder whose cycle is superseded by a resume or purchase before its intent is published leaves no outcome, and neither does a reminder the reconciler skips as already late. The "missed under 0.1%" target cannot be measured in production.
3. **Wasted send capacity.** The dispatcher takes a send token before the watermark gate and the ledger claim and never returns it, so held and duplicate intents burn capacity when it is scarcest. The retry index does not carry `sendBy`, so a late retry takes a token before it can know it is late, and the retry loop is serial across shards.

## Success criteria

- Under detector lag, the watermark reports "behind by X" and the gates hold proportionally; it goes stale only when lag exceeds its history.
- Every reminder a healthy system would have sent ends with an outcome, including `SUPERSEDED`, and the reconciler's skips are recorded as `SKIPPED_LATE`.
- Only an actual send consumes a send token; a late retry consumes none.
- A re-run of the 250/s load test shows skipped-late well below 22.6%, reports superseded and lateness misses from outcomes, and the "never superseded, no outcome" line is 0 or explained.
- The in-memory mode and every existing test keep working; interface changes are covered by the shared contract tests for both in-memory and real adapters.

## Out of scope

The cooperative-sticky assignor and static membership (deferred until real-infra load testing can measure rebalance cost); a global send-rate limit across replicas; the review's fixes 4 (real-infra load test) and 5 (faster reconciler rebuild).

## 1. Watermark liveness

### 1.1 Snapshot history

`WatermarkSnapshots` drops its age limit and keeps two rings:

- **Dense:** the latest 8 snapshots, one per 250 ms attempt (unchanged cadence).
- **Sparse:** one snapshot per second (the first snapshot taken in each second), back to `max(latenessBounds) + CLOCK_SKEW`, about 30 min 5 s in production (about 1,800 entries of a partition→offset map), 35 s with `demo.env`.

`satisfied(p, committed)` returns the time of the newest snapshot, dense first then sparse, whose end offset for `p` is at or below `committed`; empty only when the partition is behind every retained snapshot. A partition behind the whole history is by then past every lateness bound, so going stale there loses nothing.

### 1.2 Publishing and staleness

`DetectorWatermarkHooks.afterCommit` publishes the satisfied time every iteration, however old. The previous rule (stop publishing so a cut-off detector goes stale) is replaced: a detector that can no longer take snapshots keeps republishing its last satisfied time, which is frozen. This is as safe as a stale entry, since a frozen watermark only makes both gates hold, and it keeps the lag visible. Deliberate deviation from spec §5.4's "its watermark goes stale", with the same effect on sends. A dead detector still goes stale after 5 s, because nothing writes.

### 1.3 Reported lag

`/ready`'s `watermark.lag_ms.p<n>` becomes Redis `TIME` (`watermark.now()`) minus the published time, so it rises while a detector is behind or cut off. It stays `stale` when nothing is satisfied.

### 1.4 Scheduler hold

`ReminderScheduler.holdFor(w, needed)` keeps its gap-based clamp, with the cap derived from configuration:

`maxHold = max(1 s, min(60 s, smallest lateness bound ÷ 4))`

The stale (`EPOCH`) case also holds `maxHold`. Production (5 min smallest bound): 60 s, unchanged. `demo.env` (20 s): 5 s instead of 60 s. `MAX_HOLD` stops being a constant; the scheduler computes it from `RecoveryConfig` once at construction.

### 1.5 Tests

- `WatermarkSnapshotsTest`: a behind partition is satisfied by a sparse snapshot older than 2 s; snapshots older than the history are evicted; the sparse ring keeps one per second.
- `DetectorWatermarkHooksTest`: replace `brokerCutOffStopsWritingOnceEverySnapshotIsOlderThan2s` with "a cut-off detector keeps republishing its last satisfied time and its reported lag grows"; a behind partition publishes an old time rather than nothing.
- `ReminderSchedulerTest`: the cap comes from the smallest lateness bound (5 s at 20 s bounds, 60 s at 5 min bounds), including the `EPOCH` case.
- The existing `DetectorRoleIT`, `EndToEndIT` and `RedisRestartIT` pass unchanged.

## 2. Observable misses

### 2.1 Timer store reports what it displaced

- `upsert.lua` returns the previous packed value when it overwrites one (and whether it wrote); `remove.lua` returns the packed value it deleted.
- Port change (`TimerStore`, a frozen contract): `Upsert upsert(Timer timer)` with `record Upsert(boolean written, Optional<Timer> displaced)`, and `Optional<Timer> remove(String cartId, long version)`.
- `RedisTimerStore` and `PriorityQueueTimerStore` implement it. `TimerStoreContract` gains cases for both return values, run against both adapters. The master plan's §1.2 and §1.6 are updated.

### 2.2 `SUPERSEDED` outcome from the detector

- `OutcomeKind` gains `SUPERSEDED`. `AbandonmentDetector` gains an `OutcomeRecorder` constructor parameter.
- After its timer write for an event, the detector inspects the displaced timer:
  - **Displaced `REMINDER(v, i)`.** For each offset `j ≥ i` with `dueAt_j ≤ event.occurredAt`, where `dueAt_j = dueAt_i − offset_i + offset_j`, record `Outcome(key = cartId:v:j, cartId, v, TREATMENT, SUPERSEDED, at = event.occurredAt, attempts = 0)`. No extra read.
  - **Displaced `CHECK_ABANDON(v)` with `dueAt ≤ event.occurredAt`** (overdue, cart never marked abandoned). On this rare path only, read the cart before `applyEvent`. If its arm is `TREATMENT` and `ReminderPolicy` finds it eligible at the check's due time (frequency cap), record `SUPERSEDED` for each offset `j` with `dueAt_j = checkDue − window + offset_j ≤ event.occurredAt`.
  - **Anything else** (no displacement, a not-yet-due timer) records nothing, since those reminders were never owed.
- **Redelivery:** a redelivered event's upsert is a no-op and displaces nothing, so there are no duplicate outcomes.
- **Crash window:** a crash between the timer write and the outcome loses that outcome; the reminder then counts as unexplained. Documented, not engineered around.
- In memory, `Pipeline` wires the same recorder.

### 2.3 `SKIPPED_LATE` from the reconciler

- `Reconciler` gains an `OutcomeRecorder`.
- When a rebuild skips offsets already past their lateness bound, it records `SKIPPED_LATE` for each skipped offset's key (`cartId:v:j`, arm `TREATMENT`, `at = now`, attempts 0), but only when the rebuild's upsert wrote, or its `endSequence` succeeded. A skip is therefore recorded once, not on every sweep. `ReconcilerRole` and `Pipeline` wire the recorder.

### 2.4 Loadgen accounting

- `SUPERSEDED` joins outcome precedence below the existing kinds: `SENT > DEAD > CANCELLED > SKIPPED_LATE > SUPERSEDED`.
- A key resolved as `SUPERSEDED` with `at ≤ sendBy` (the offset's scheduled time plus its lateness bound, from the loadgen's own script) is an explained non-send. With `at > sendBy` it is "superseded after sendBy", a lateness miss counted inside unexplained.
- "Never superseded, no outcome" is every expected key with no outcome at all.
- Unexplained missing = expected − sent − skipped late − cancelled − dead − superseded before sendBy (identity unchanged; its inputs are now outcomes, not script inference). `MissingBreakdown`'s script inference stays as a cross-check reported beside the outcome-based numbers.

### 2.5 Tests

- `LuaScriptsTest` and `TimerStoreContract`: the displaced values, for overwrite, equal no-op, lower no-op, and remove.
- `AbandonmentDetectorTest`: resume over a due reminder; purchase over a due reminder; a not-yet-due reminder records nothing; an overdue check with treatment, holdout, and cap-reached; a redelivered event records nothing new.
- `ReconcilerTest`: a skip records `SKIPPED_LATE` once across two sweeps.
- Loadgen tests: the classification and the identity.
- `EndToEndIT` (stopped detector): the held reminder resolves to `SUPERSEDED` or `CANCELLED`, never to no outcome.

## 3. Dispatcher capacity

### 3.1 Refund unused tokens

- `SendBudget` (frozen port) gains `void release(Lane lane)`. `TokenBucket` returns one token, capped at capacity; `UnlimitedSendBudget` does nothing.
- `Dispatcher.handle` keeps its order (`sendBy` check, token, watermark gate, claim) and releases the token on every path after acquisition that does not call the sink:
  - the gate holds;
  - the claim is `NotClaimed`;
  - the post-claim cart re-read cancels or finds it late;
  - the lease is too short for the gateway timeout.

### 3.2 Late retries cost nothing

- The `retrying-by-shard` index projects `sendBy` alongside `srcPartition`. `DueRetry` becomes `DueRetry(String key, int srcPartition, Instant sendBy)`, and `InMemorySendLedger` returns it too. `SendLedgerContract` checks that `dueRetries` carries `sendBy`.
- `Dispatcher.retryDue`:
  - **Late row** (`sendBy < now`): claim it and finish it as `SKIPPED_LATE`, with no token and no watermark gate.
  - **On-time row:** gate, then token, then claim, with the refund rules of 3.1.
  - The `GONE` sentinel is removed.
- **Migration:** DynamoDB cannot change a GSI's projection in place. Locally, recreate the table (`docker compose down -v`). For a real deployment, add a new index and cut over; nothing is deployed today, so no migration code is written.

### 3.3 Parallel retry pass

`DispatcherRole`'s retry pass runs `retryDue` for all shards concurrently on virtual threads (one task per shard) and waits for all of them before the next `RETRY_POLL`. It still beats health every pass. `Dispatcher` has no mutable state of its own, so concurrent shards are safe.

### 3.4 Tests

- `DispatcherTest`: the token is released on each no-send path; a late retry takes no token and ends `SKIPPED_LATE`.
- `TokenBucketTest`: release is capped at capacity.
- `SendLedgerContract`: `dueRetries` returns `sendBy` (in-memory and DynamoDB).
- `DispatcherRoleIT`: a held partition's token spend stays flat, and retries on several shards progress within one pass.

## 4. Verification and documentation

- `./gradlew test integrationTest` passes, with nothing skipped and no pinning reports.
- Re-run the load test at 250/s with `demo.env`. Record a new report in `docs/load-reports/` comparing skipped-late (22.6% before), unexplained missing (2.1% before) and the new outcome lines. Keep the old report.
- Update DESIGN §6 (superseded outcomes now exist), §11 (the stale-watermark hold paragraph), §12.2 (the watermark history and the staleness rule), §12.4 (token refund, retry `sendBy`) and §13 (new results; remove the limitations that are fixed). Update the README's load-test accounting paragraph.
