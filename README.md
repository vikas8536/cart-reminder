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
   - Kill a scheduler mid-run (`docker kill cart-recovery-scheduler-1`), then bring it back with `docker start cart-recovery-scheduler-1`. What this shows depends on timing, and the recorded drill run did not confirm a kill landing during a lease. If the kill lands while the replica holds leased timers, those timers become due again for the surviving replica once the lease expires (`LEASE`, 90 s by default, which `demo.env` does not shorten). Nothing is silently dropped, but a reminder whose lease outlives its lateness bound (20 to 30 s in `demo.env`) is then skipped late and counted, not sent. If the kill lands between claims, nothing was leased and the drill only shows the survivor carrying on. Lease-expiry redelivery itself is proven by `TimerStoreContract.aClaimedTimerIsLeasedAndRedeliveredAfterTheLease`.
   - `docker compose exec redis redis-cli FLUSHALL`: dispatchers pause immediately, because the watermark hash is gone and every partition reads as stale. They resume once the detectors rewrite their watermarks, about 1 s later, not when timers are rebuilt. The missing epoch key triggers an immediate reconciler sweep, which rebuilds the missing timers and skips offsets that are already past their lateness bound. Those skips record no outcome. Expect no duplicate sends, but some reminders can be lost: a reminder whose timer was flushed and whose bound passed before the rebuild is simply not sent.
   - Stop one detector (`docker kill cart-recovery-detector-1`): only its partitions pause, for the rebalance; the rest of the traffic keeps sending. Restart it with `docker start cart-recovery-detector-1`.
   - Stop all detectors (`docker compose stop detector`): all sending pauses, because every watermark goes stale; nothing sends against stale state. Restart with `docker compose start detector`.
   - Stop the dispatchers (`docker compose stop dispatcher`), wait past a reminder's lateness bound, then restart them (`docker compose start dispatcher`): the backlog is skipped at the pre-check (`dispatch.skipped_late_precheck`), spending no send capacity.
   - Set `SEND_FAILURE_RATE=0.3` on the dispatchers (`SEND_FAILURE_RATE=0.3 docker compose up -d dispatcher`): some sends succeed after a retry, some dead-letter; `docker compose run --rm dispatcher --role=replay` then sends each dead key exactly once.
   - Flip `recovery-meta.paused` to true in DynamoDB: sending stops within 5 s; flip it back to resume.
5. Replay dead letters on demand: `docker compose run --rm dispatcher --role=replay`.

The first few minutes after a cold start may show slow sends or none. Until every detector has caught up and published its watermarks, partitions read as stale. The scheduler holds a timer on a stale watermark for a fixed 60 s, which is longer than `demo.env`'s 20 to 30 s lateness bounds, so early reminders can be skipped late instead of sent.

Load test: `docker compose --profile load run --rm -e RATE=250 -e DURATION=PT5M loadgen` (with `COMPOSE_ENV_FILES=demo.env` exported as above). 250 events/s is the rate actually measured on the development machine (`docs/load-reports/`). The spec's target is the 5,000 events/s baseline (DESIGN §8), and a `RATE=5000` run aborted on that machine with unbounded detector lag, since every role and container shares one laptop. The load test reports achieved throughput, per-stage lag, send latency per lane, and correctness (duplicate and post-purchase sends, superseded-before-send, superseded-after-sendBy, never-superseded-no-outcome, unexplained missing) to the console and to `build/reports/load/<timestamp>.md`; `docs/load-reports/` holds a recorded run. Unexplained missing is expected minus sent minus skipped-late minus cancelled minus dead minus superseded-before-send — a deliberate deviation from spec §8.5's plain formula, because a resume or purchase that supersedes a cart's cycle at or before that offset's send deadline correctly leaves no outcome behind at all (the pending timer is overwritten or deleted before it ever fires), so it isn't a failure; only a key superseded *after* its send deadline ("superseded after sendBy", pure lateness) or never superseded yet left with no outcome ("never superseded, no outcome", possible silent loss) stays inside unexplained missing. The two are reported on separate lines, and their sum equals unexplained missing exactly, key for key. Sent, skipped-late, cancelled and dead are counted only over expected keys; an outcome recorded for a key nothing expected (a real-vs-nominal timing edge at a cycle boundary, say) is reported on its own line instead of being folded in, so it can never silently cancel out a real miss elsewhere in the subtraction. The loadgen takes no command-line flags: `RATE` (events per second) and `DURATION` (ISO-8601, e.g. `PT5M`) are environment variables on the `loadgen` service, overridden with `-e`. Before its fixed drain wait (window + last offset + last lateness bound + one reconcile interval, sized for demo.env's compressed timings, which is why load runs use `demo.env`), it first waits — bounded, up to 10 minutes — for every sampled consumer group's lag and the Redis past-due timer backlog to reach zero, so an overloaded run's real backlog gets a chance to resolve before the fixed wait clips it. For long load runs, start DynamoDB Local in memory with `DYNAMO_STORAGE=-inMemory docker compose up -d` (a compose profile cannot change another service's flags, so this is an environment switch rather than the `load` profile).

Documented minimum: about 6 GB of memory for Docker Desktop.

### Operator runbook: health endpoints

Every role serves three plain-text endpoints on `HEALTH_PORT` (8081). Role ports are not published to the host, so query them from inside the container, for example `docker exec cart-recovery-dispatcher-1 wget -qO- localhost:8081/ready`.

- `/health` returns 200 while every loop in the role has iterated within the last 15 s, and 503 with the silent loops' names otherwise. It is the compose healthcheck. Loops beat while backing off or paused, so a dependency outage never fails it. The 15 s is a fixed, deliberate upper bound, looser than spec §7.4's "3× the poll interval" (1.5 s for a 500 ms poll), so a GC pause or one slow dependency call never gets a container restarted.
- `/ready` always returns 200 and lists `key: value` state lines:
  - `watermark.lag_ms.p<n>` (detector): how far partition `n`'s published watermark trails the newest end-offset snapshot. `stale` means no snapshot is satisfied, so nothing is being written, the entry goes stale after 5 s, and gated sends for that partition pause.
  - `stuck.<topic>-<p>` (detector and dispatcher consumers): `true` when the committed offset has not moved for 5 minutes while lag is above 0.
  - `breaker` (`open` or `closed`) and `paused` (the `recovery-meta.paused` switch) on dispatchers.
  - `reconciler.sweep_ms` and `reconciler.sweep_slow` (the last sweep took longer than the smallest lateness bound) on the reconciler, and `loadgen.phase` on the load generator.
- `/metrics` lists every counter as `name value`. Each role also logs the same counters every 10 s.

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
