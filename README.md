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
4. Drills, each with its expected outcome. First, once per shell session: `export COMPOSE_ENV_FILES=demo.env`, so every `docker compose` command below picks up the compressed timings without repeating `--env-file demo.env` on each one (forgetting it on just one command, mid-drill, silently reverts that service to the plain 30 minute/1 hour/24 hour defaults). To stop or kill **one** replica of a scaled service (`detector`, `scheduler`, `dispatcher`), name its container directly — `docker compose stop <svc>` / `docker compose kill <svc>` act on **every** replica of that service, not one:
   - One replica: `docker kill cart-recovery-<svc>-1` (or `docker stop ...` for a graceful stop), then `docker start cart-recovery-<svc>-1` to bring it back.
   - All replicas: `docker compose stop <svc>` / `docker compose start <svc>`.
   - Either way, a manually stopped or killed container is **not** auto-restarted: `restart: unless-stopped` treats any manual stop (including a `kill`) as intentional, so it stays down until you `start` it again yourself.

   Drills:
   - Kill a scheduler mid-run (`docker kill cart-recovery-scheduler-1`): its leased timers are redelivered to a surviving replica once the lease expires — nothing is lost. Bring it back with `docker start cart-recovery-scheduler-1`.
   - `docker compose exec redis redis-cli FLUSHALL`: dispatchers pause within one watermark staleness window (about 5 s) and resume once the reconciler rebuilds the missing timers; no duplicate or missed sends.
   - Stop one detector (`docker kill cart-recovery-detector-1`): only its partitions pause, for the rebalance; the rest of the traffic keeps sending. Restart it with `docker start cart-recovery-detector-1`.
   - Stop all detectors (`docker compose stop detector`): all sending pauses, because every watermark goes stale; nothing sends against stale state. Restart with `docker compose start detector`.
   - Stop the dispatchers (`docker compose stop dispatcher`), wait past a reminder's lateness bound, then restart them (`docker compose start dispatcher`): the backlog is skipped at the pre-check (`dispatch.skipped_late_precheck`), spending no send capacity.
   - Set `SEND_FAILURE_RATE=0.3` on the dispatchers (`SEND_FAILURE_RATE=0.3 docker compose up -d dispatcher`): some sends succeed after a retry, some dead-letter; `docker compose run --rm dispatcher --role=replay` then sends each dead key exactly once.
   - Flip `recovery-meta.paused` to true in DynamoDB: sending stops within 5 s; flip it back to resume.
5. Replay dead letters on demand: `docker compose run --rm dispatcher --role=replay`.

Load test: `docker compose --profile load run --rm -e RATE=5000 -e DURATION=PT5M loadgen` (with `COMPOSE_ENV_FILES=demo.env` exported as above) reports achieved throughput, per-stage lag, send latency per lane, and correctness (duplicate and post-purchase sends, superseded-before-send, missed-while-lagging, unexplained missing) to the console and to `build/reports/load/<timestamp>.md`; `docs/load-reports/` holds a recorded run. Unexplained missing is expected minus sent minus skipped-late minus cancelled minus dead minus superseded-before-send — a deliberate deviation from spec §8.5's plain formula, because a resume or purchase that supersedes a cart's cycle at or before that offset's send deadline correctly leaves no outcome behind at all (the pending timer is overwritten or deleted before it ever fires), so it isn't a failure; only a cart superseded *after* its send deadline (or never superseded) stays inside unexplained missing, as "missed while lagging" — and with that, unexplained missing equals missed-while-lagging exactly, key for key. Sent, skipped-late, cancelled and dead are counted only over expected keys; an outcome recorded for a key nothing expected (a real-vs-nominal timing edge at a cycle boundary, say) is reported on its own line instead of being folded in, so it can never silently cancel out a real miss elsewhere in the subtraction. The loadgen takes no command-line flags: `RATE` (events per second) and `DURATION` (ISO-8601, e.g. `PT5M`) are environment variables on the `loadgen` service, overridden with `-e`. Before its fixed drain wait (window + last offset + last lateness bound + one reconcile interval, sized for demo.env's compressed timings, which is why load runs use `demo.env`), it first waits — bounded, up to 10 minutes — for every sampled consumer group's lag and the Redis past-due timer backlog to reach zero, so an overloaded run's real backlog gets a chance to resolve before the fixed wait clips it. For long load runs, start DynamoDB Local in memory with `DYNAMO_STORAGE=-inMemory docker compose up -d` (a compose profile cannot change another service's flags, so this is an environment switch rather than the `load` profile).

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
