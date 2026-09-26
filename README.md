# Abandoned Cart Recovery

Case study submission: an event-driven detection-and-scheduling pipeline for abandoned-cart reminders, with a fake-clock verifier. No real sends are performed.

- `DESIGN.md` is the design document.
- `src/main/java` is the pipeline. `src/test/java/com/quince/cartrecovery/FakeClockVerifierTest.java` is the verifier.

## Run

Requires JDK 21 on `JAVA_HOME`. Gradle is bundled through the wrapper.

```bash
./gradlew test    # runs the verifier and unit tests
./gradlew run     # prints a scripted virtual timeline of which reminders fire
```

No JDK 21 on hand? Two quick options:

SDKMAN:

```bash
sdk install java 21.0.12+1.1-tem
```

Homebrew:

```bash
brew install openjdk@21
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
```

## Infra (Docker)

The same pipeline also runs as separately deployable roles on real Kafka, Redis, and DynamoDB, with only Docker required — no JDK on the host.

1. In-memory, no Docker (unchanged): `./gradlew test`, `./gradlew run`.
2. Infra: `docker compose --env-file demo.env up -d --build`, then `docker compose logs -f` to watch every role come up.
3. Visible demo: `demo.env` compresses the timings (window 30 s, offsets 30 s / 60 s / 120 s); drive it with `docker compose --env-file demo.env --profile load run --rm -e RATE=50 -e DURATION=PT60S loadgen` and watch reminders land on `sink-sends` within the minute.
4. Drills, each with its expected outcome:
   - Kill a scheduler mid-run: its leased timers are redelivered to a surviving replica once the lease expires — nothing is lost.
   - `docker compose exec redis redis-cli FLUSHALL`: dispatchers pause within one watermark staleness window (about 5 s) and resume once the reconciler rebuilds the missing timers; no duplicate or missed sends.
   - Stop one detector: only its partitions pause, for the rebalance; the rest of the traffic keeps sending.
   - Stop all detectors: all sending pauses, because every watermark goes stale; nothing sends against stale state.
   - Stop the dispatchers, wait past a reminder's lateness bound, then restart them: the backlog is skipped at the pre-check (`dispatch.skipped_late_precheck`), spending no send capacity.
   - Set `SEND_FAILURE_RATE=0.3` on the dispatchers (`SEND_FAILURE_RATE=0.3 docker compose --env-file demo.env up -d dispatcher`): some sends succeed after a retry, some dead-letter; `docker compose run --rm dispatcher --role=replay` then sends each dead key exactly once.
   - Flip `recovery-meta.paused` to true in DynamoDB: sending stops within 5 s; flip it back to resume.
5. Replay dead letters on demand: `docker compose run --rm dispatcher --role=replay`.

Load test: `docker compose --env-file demo.env --profile load run --rm -e RATE=5000 -e DURATION=PT5M loadgen` reports achieved throughput, per-stage lag, send latency per lane, and correctness (duplicate and post-purchase sends, unexplained missing) to the console and to `build/reports/load/<timestamp>.md`; `docs/load-reports/` holds a recorded run. The loadgen takes no command-line flags: `RATE` (events per second) and `DURATION` (ISO-8601, e.g. `PT5M`) are environment variables on the `loadgen` service, overridden with `-e`. It waits window + last offset + last lateness bound + one reconcile interval after publishing, which is why load runs use `demo.env`'s short timings. For long load runs, start DynamoDB Local in memory with `DYNAMO_STORAGE=-inMemory docker compose --env-file demo.env up -d` (a compose profile cannot change another service's flags, so this is an environment switch rather than the `load` profile).

Documented minimum: about 6 GB of memory for Docker Desktop.

## Layout

```
src/main/java/com/quince/cartrecovery/
  model/      events, cart record, timer, intent, config
  ports/      interfaces: clock, state store, timer store, ledger, outbox, sink, dead letter queue
  core/       AbandonmentDetector, ReminderScheduler, Dispatcher, Reconciler, Metrics
  inmemory/   adapters and FakeClock
  Pipeline    wiring; ingest, advanceTo, outage, restart, redeliver, replayDeadLetters
  Main        scripted demo
```
