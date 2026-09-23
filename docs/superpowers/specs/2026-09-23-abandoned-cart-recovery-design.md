# Abandoned Cart Recovery: Design Spec

Date: 2026-09-23
Status: approved in brainstorming, awaiting written review

## 1. Purpose and deliverables

This project answers a Principal Software Engineer case study. The platform loses a large share of carts before checkout, and the task is an automated recovery system that reminds shoppers about abandoned carts.

Two deliverables come out of this spec:

1. `DESIGN.md` at the repository root: the design document the reviewers score. It names the components, how they communicate, the reasoning behind each scored decision, the assumptions, and the open questions we would ask the business.
2. A runnable detection-and-scheduling pipeline in Java 21 with a fake-clock verifier. It does not perform real sends. It shows that the scheduled triggers would fire, and would not fire, at the right virtual times.

Success means the design covers the five scored areas with clear reasoning, the code is small enough to read in one sitting, and every claim in the design about cancellation, idempotency, and failure handling is backed by a passing scenario in the verifier.

## 2. Requirements from the brief

Required and scored:

1. Success and guardrail metrics, measured against a control group, plus what to watch to know the system is doing harm.
2. Detection: event-driven or batch, with the reason.
3. Mid-flight cancellation: never notify a shopper who just checked out.
4. Idempotency and delivery under at-least-once infrastructure: no duplicate reminders.
5. Failure handling when a send fails or a component goes down: retry, dead-letter, replay.

Secondary, addressed with stated assumptions and optional depth: guest users, experimentation, behaviour under load spikes, personalization.

Behavioural rules:

- A cart is abandoned when a shopper adds or edits items and then stays inactive for a configurable window, default 30 minutes, with no checkout. An edit resets the clock.
- Notifications are scheduled at configurable offsets, default 30 minutes, 1 hour, and 24 hours.
- The sequence stops when the cart is purchased, cleared, or resumed.
- About 5k cart events per second at peak, with spikes above that.

## 3. Decisions and assumptions

Decisions made during brainstorming, with the reason each was taken:

| Decision | Choice | Reason |
|---|---|---|
| Detection model | Event-driven state machine with lazily validated timers | Minute-level precision at 5k events per second, cancellation becomes a compare rather than a delete, and at-least-once redelivery is handled by the same version check. Batch scanning gives coarser timing, wide queries, and still needs versioning to close the cancellation race. |
| Offset semantics | Offsets are measured from last activity | With defaults, the first reminder fires the moment the cart is declared abandoned. Configuration validates that the first offset is at least the window. |
| Production stack | Kafka for events, Redis for timers, DynamoDB for cart state | The company is not known to run a workflow engine. Redis absorbs 12.5k timer upserts per second cheaply, DynamoDB gives durable state for reconciliation. |
| Temporal | Considered, not chosen | Durable workflows fit the problem but every edit would become a durable history write, roughly a billion actions per day, which is a material cost or a heavy self-hosted persistence tier. A hybrid that starts a workflow only for confirmed-abandoned carts is the recommended path if Temporal is already operated. |
| Code shape | Single-process, framework-free Java 21 with in-memory adapters and a fake clock | Reviewers need only a JDK. Interfaces map one to one onto the production components. |
| Code scope | The five scored areas plus holdout assignment | Secondary topics stay in the design document. |
| Build | Gradle with checked-in wrapper, JUnit 5 | Widest reviewer familiarity. |

Assumptions stated in the design document:

- About 10 events per cart session, about 70% of carts abandon, about 1 KB per event including the item snapshot.
- Spikes are 2.5 times baseline sustained for an hour.
- The Cart Service already exists, owns the durable cart, and can publish events with a monotonically increasing per-cart version.
- Reminders go through an existing notification gateway that accepts an idempotency key. The gateway is out of scope.
- A resume, meaning the shopper reopened the cart, counts as activity. It cancels pending reminders and restarts the inactivity clock.
- A cart-level frequency cap, default three sequences per cart per 7 days, prevents a shopper who keeps peeking from receiving endless reminder sequences.

## 4. Targets

Adopted service levels, asymmetric because a wrong send costs trust and a missed send costs a small expected revenue:

| Target | Value |
|---|---|
| Wrong sends: after purchase, or duplicates | under 0.01% of sends |
| Timely delivery: within 5 minutes of schedule for the 30 minute and 1 hour offsets, within 30 minutes for the 24 hour offset | 99.5% of scheduled reminders |
| Missed entirely: never sent when it should have been, excluding deliberate skips for lateness | under 0.1% per month |

The design drops when in doubt. A stale or too-late reminder is skipped and counted, never sent.

## 5. Capacity

| Figure | Baseline | Spike (2.5x for 1 hour) |
|---|---|---|
| Cart events per second | 5,000 | 12,500 |
| Events in the spike hour | | 45 million |
| Stream throughput | 5 MB/s | 12.5 MB/s |
| State store upserts per second | 5,000 | 12,500 |
| Timer upserts per second | 5,000 | 12,500 |
| Distinct carts per second | 500 | 1,250 |
| Carts becoming abandoned per second | 350 | 875 |
| Reminder sends per second, three per abandoned cart spread over offsets | about 1,000 | about 2,600 |

Consequences:

- Kafka topic with 64 partitions keyed by cart id, so each partition sees under 200 events per second at spike and the detector scales to 64 consumers.
- Timer set holds distinct in-flight carts, about 2 million members at spike, roughly 200 MB. Shard 64 ways by cart hash so sweepers run in parallel.
- The 24 hour reminders for a spike hour land as a burst 24 hours later. The dispatcher needs a rate limiter and a backlog, not just headroom.

## 6. Components and data flow

Every component has one job and talks to its neighbours through a queue or a store. Nothing here sits on the checkout path.

1. **Cart Service (existing)** publishes to the `cart-events` stream, partitioned by cart id. Events: `CartEdited` (add, update, remove), `CartResumed`, `CartCleared`, `CartPurchased`. Each carries cart id, shopper key, cart version, event time, and a compact item snapshot.
2. **Abandonment Detector** consumes the stream. It upserts the cart record in the **Cart State Store** and upserts one pending timer per cart in the **Timer Store**, tagged with the version. Events with a version at or below the stored one are ignored. Offsets are committed only after the state write succeeds.
3. **Cart State Store** (DynamoDB) holds one record per cart: cart id, shopper key, status, version, last activity time, item snapshot, experiment arm, sequence count for the frequency cap. Writes are conditional on the stored version so concurrent writers cannot regress a record.
4. **Timer Store** (Redis sorted set per shard, member cart id, score due time, payload version tag and timer kind). Upserting by cart id replaces the previous timer. Eager removal on purchase is a best-effort optimisation. It is a derived index, rebuildable from the state store.
5. **Timer Sweepers** pop due timers from each shard in bounded batches with a short lease and hand them to the Reminder Worker. A lease that expires returns the timer to the set, which gives at-least-once delivery of timers.
6. **Reminder Worker** reloads the record, validates the version tag and expected status, and either drops the timer, marks the cart abandoned and schedules reminder timers, or writes a `NotificationIntent` to the **Outbox** together with a **Send Ledger** row in one conditional transaction.
7. **Dispatcher** drains the outbox with two priority lanes, re-validates the cart immediately before calling the **Notification Gateway** with the idempotency key, retries transient failures with backoff, and moves permanent failures to the **Dead Letter Queue**. A replay tool re-enqueues dead letters with their original key.
8. **Reconciliation Sweeper** scans active and abandoned records every few minutes and reinserts any missing timer.
9. **Config** holds window, offsets, per-offset lateness bounds, frequency cap, holdout percentage, and experiment arms. **Metrics** are emitted from every stage.

Data flow in one line: Cart Service to stream, stream to detector, detector to state store and timer store, timer store to sweeper, sweeper to reminder worker, reminder worker to outbox and ledger, outbox to dispatcher, dispatcher to gateway or dead letter queue.

## 7. State machine and timer semantics

Cart record statuses: `ACTIVE`, `ABANDONED`, `CLOSED`.

Event handling:

- `CartEdited` or `CartResumed`: set `ACTIVE`, store version and last activity, upsert timer kind `CHECK_ABANDON` due at last activity plus window, tagged with the version. If the cart was `CLOSED`, this starts a new cycle.
- `CartPurchased` or `CartCleared`: set `CLOSED`, store version, best-effort remove the timer.
- Any event with version at or below the stored version: ignore and count as a duplicate or out-of-order event.

Timer fire, kind `CHECK_ABANDON`: reload the record. Drop if the version differs or the status is not `ACTIVE`. Otherwise set `ABANDONED`, increment the sequence count, and unless the arm is holdout or the frequency cap is reached, upsert timer kind `REMINDER` with offset index 0 due at last activity plus the first offset. Each reminder, once handled, schedules the next offset. Keeping one timer per cart at all times keeps the Redis upsert-by-cart-id model simple.

Timer fire, kind `REMINDER` with offset index `i`: reload the record. Drop if the version differs or the status is not `ABANDONED`. Skip and count if now exceeds due time plus the lateness bound for offset `i`, then schedule offset `i + 1` if any. Otherwise, in one transaction conditioned on the record version, write the ledger row keyed by cart id, version, and offset index, and write the outbox intent. If the ledger row already exists, the timer was redelivered and nothing is written. Then schedule offset `i + 1` if any.

## 8. Mid-flight cancellation

Three checkpoints, each cheap:

1. Version compare when the timer fires.
2. Ledger and outbox write conditioned on the record version being unchanged.
3. Reload of the record immediately before the gateway call. A purchase seen here cancels the intent.

The residual window is the gateway round trip. Sends after purchase are a guardrail metric with a target under 0.01%.

## 9. Idempotency layers

| Layer | Mechanism |
|---|---|
| Events | Per-cart version monotonicity. Redelivered or reordered events are ignored. |
| Timers | Version tag compared on fire. Redelivered timers fail the ledger existence check. |
| Intents | Ledger key of cart id, version, and offset index. |
| Gateway | The same key is passed as the provider idempotency key so a retry after a timeout does not double send. |

## 10. Failure handling

| Failure | Behaviour |
|---|---|
| Transient send failure | Exponential backoff with jitter, bounded attempts. The cart is re-validated before each attempt so a purchase during backoff stops the retry. |
| Permanent send failure | Dead letter with reason. Replay re-enqueues with the same key and re-validates, so it is safe. |
| Detector down | Kafka retains events. The consumer resumes from its committed offset. Reprocessing is idempotent by version. |
| Timer store loss | Reconciliation sweeper reinserts missing timers. Full rebuild scans the state store or replays the stream from seven days of retention. Timers past their lateness bound are skipped and counted. |
| Gateway down | Circuit breaker pauses dispatch, outbox backs up, drain resumes within lateness bounds. |
| Poison event | Schema validation, event dead letter, alert. |

## 11. Metrics, control group, experimentation

**Control group.** A holdout arm of about 10% of eligible carts is assigned by hashing the shopper key with an experiment salt. Holdout carts run through the full pipeline and are tracked identically but schedule no reminders.

**Success metrics.** Primary: recovery rate, the share of abandoned carts that purchase within 7 days of abandonment, treatment versus holdout, with confidence intervals. Secondary: recovered revenue per abandoned cart, time to recovery. Attribution uses the purchase event, not link clicks.

**Guardrails.** Unsubscribe and spam-complaint rates per send, bounce rate, sends after purchase, duplicate sends, sends per shopper per day against the frequency cap, holdout purchase rate not dropping.

**System health.** Consumer lag, timer backlog past due, fire latency p99, dead letter depth, dedupe hit rate, skipped-for-lateness count.

**Experimentation.** Arms cover timing, meaning alternate offset sets resolved at scheduling, and message variants resolved at dispatch. Assignment unit is the shopper key, user id when logged in and session id for guests, fixed on the cart record for its life. Trade-off: user id gives a consistent experience across devices and clean attribution but covers only logged-in shoppers, session id covers everyone but one person can land in several arms and attribution to a session is noisier.

## 12. Secondary topics

- **Guest users.** On login the Cart Service merges the guest cart into the user cart, keeping the guest quantities for overlapping items because they reflect the most recent intent, then emits a clear for the guest cart and an edit for the user cart. The pipeline needs no new logic. Guests get reminders only when a contact channel exists, such as a captured email or a web push subscription, and are otherwise tracked but not sent.
- **Load spikes.** Delay, never drop events, never reject upstream. Kafka absorbs, detectors autoscale on lag, sweepers pull bounded batches, the dispatcher rate limits to the gateway. Two priority lanes keep 30 minute reminders ahead of 24 hour ones. Lateness bounds turn an extreme backlog into skipped stale reminders rather than a flood.
- **Personalization.** The intent carries first name and item snapshot. The dispatcher refreshes the snapshot from the Cart Service at send time and falls back to the stored one.

## 13. Code design

Gradle project at the repository root, package `com.quince.cartrecovery`, Java 21, JUnit 5. Single-threaded by design: production ordering per cart comes from partitioning, and the in-memory pipeline processes one input at a time.

Packages:

- `model`: sealed `CartEvent` with records `CartEdited`, `CartResumed`, `CartCleared`, `CartPurchased`; `CartRecord`; `CartStatus`; `Timer` with kind, cart id, version tag, offset index, due time; `NotificationIntent`; `RecoveryConfig` with window, offsets, lateness bounds, frequency cap, holdout percentage, and validation that the first offset is at least the window.
- `ports`: `Clock`, `CartStateStore` with `get`, `put` conditioned on expected version, and `scanOpen` for reconciliation; `TimerStore` with `upsert`, `remove`, and `popDue` up to a time; `SendLedger` with `recordIfAbsent`; `NotificationSink` with `send` returning success, transient failure, or permanent failure; `DeadLetterQueue` with `add` and `drain`.
- `core`: `AbandonmentDetector.handle(event)`; `ReminderScheduler.onTimer(timer)`; `Dispatcher.drain()` with retry policy and dead-lettering; `Reconciler.rebuildTimers()`, which derives each open cart's next timer from its status and the highest offset index present in the ledger for its current version.
- `inmemory`: `FakeClock`, `InMemoryCartStateStore`, `PriorityQueueTimerStore`, `InMemorySendLedger`, `RecordingNotificationSink` with a scriptable failure sequence, `InMemoryDeadLetterQueue`.
- `Pipeline`: wires the components, exposes `ingest(event)`, `advanceTo(time)` which fires every due timer in order and runs dispatch, and `restart()` which discards the timer store and rebuilds it through the reconciler.
- `Main`: plays a scripted scenario and prints a virtual timeline of triggers fired, dropped, and skipped.

Pipeline processing is deterministic: `advanceTo` advances the fake clock in steps to each timer due time, so a timer scheduled during a fire is itself fired at its correct virtual time.

## 14. Fake-clock verifier scenarios

Each scenario is a JUnit 5 test driving `Pipeline` with `FakeClock`.

1. Single edit, no further activity: reminders fire at 30 minutes, 1 hour, 24 hours, exactly one send each.
2. Edit at 20 minutes resets the clock: reminders fire at 50 minutes, 80 minutes, and 24 hours 20 minutes.
3. Purchase at 10 minutes: no sends. Purchase at 45 minutes: first send only.
4. Clear cancels pending reminders. Resume cancels pending reminders and restarts the clock.
5. Duplicate delivery of the same edit event: same schedule, one send each.
6. Duplicate delivery of the same timer: one send.
7. Out-of-order events, an older version arriving after a newer one: the older is ignored.
8. Transient send failure twice then success: one send, ledger has one row.
9. Permanent send failure: dead-lettered, then replay sends once.
10. Restart between abandonment and the first reminder: reminders still fire on time.
11. Timer delivered past the lateness bound: skipped and counted, later offsets still scheduled.
12. Holdout arm: cart becomes abandoned, no sends.
13. Reopen after purchase with a higher version: a fresh cycle with its own sends.
14. Frequency cap reached: further sequences schedule nothing.
15. Config validation rejects a first offset smaller than the window.

## 15. Open questions for the business

Listed in the design document as questions we would ask before building:

- Which contact channels exist for guests, and is capturing an email before checkout acceptable?
- Is the 24 hour reminder subject to quiet hours or local-time delivery windows?
- Does the frequency cap apply per cart or per shopper across carts?
- Are discounts ever included in reminders, which would add a margin guardrail?

## 16. Out of scope

Real sends, notification templates, the Cart Service itself, the notification gateway, infrastructure provisioning, and multi-threaded execution of the in-memory pipeline.
