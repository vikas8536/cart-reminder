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
