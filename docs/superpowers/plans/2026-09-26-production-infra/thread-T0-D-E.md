# Thread T0/D/E: Build, Load Generator, Docs, Execution

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pin every third-party dependency and wire the `integrationTest` source set (T0); build a pure, dependency-light load-test simulator and its infra-facing role (D0, D1); update `DESIGN.md` and `README.md` to describe the production build honestly (E1); run the whole stack, the runbook drills, and a real load test, and commit the report (E2).

**Tasks in this file:** T0, D0, D1, E1, E2 — the critical-path root (T0) plus the load-test and documentation thread. See the master plan (`docs/superpowers/plans/2026-09-26-production-infra.md`) for the full dependency graph, frozen contracts (§1), and cross-thread protocol (§3).

**File ownership in this thread:**

| Task | Creates | Modifies |
|---|---|---|
| T0 | `src/test/java/com/quince/cartrecovery/Await.java`, `src/test/java/com/quince/cartrecovery/AwaitTest.java` | `build.gradle.kts` (full rewrite; owned by T0 for the whole project — no other thread touches this file) |
| D0 | `src/main/java/com/quince/cartrecovery/loadgen/{EventType,ScriptedEvent,Cycle,CartScript,Workload,Expected,SinkSend,OutcomeRow,Accounting,LatencySample,Percentiles,LoadTestSummary,Report}.java` and matching `src/test/java/.../loadgen/*Test.java` | none |
| D1 | `src/main/java/com/quince/cartrecovery/loadgen/{LoadgenRole,LagSampler,OutcomeCollector}.java`, `src/test/java/com/quince/cartrecovery/loadgen/LoadgenRoleTest.java` | none (C0c's `docker-compose.yml` already defines the `loadgen` service under profile `load` with `RATE` and `DURATION`) |
| E1 | none | `DESIGN.md`, `README.md` |
| E2 | `docs/load-reports/<timestamp>.md` (a copy of the generated report) | none |

**Binding note:** §1 of the master plan (frozen contracts) may not be changed by any task in this file. Thread B's adapter APIs this thread calls are listed in master §1.6 (controller rulings R6 and R7).

---

## Pinned dependency versions (T0)

Looked up against Maven Central / current release docs on 2026-09-26. All compatible with Java 21 and Gradle 8.10.2.

| Dependency | Version | Used by |
|---|---|---|
| `org.apache.kafka:kafka-clients` | `4.3.1` | B3 (producers/consumers), C0b (`BatchConsumerLoop`), D1 |
| `io.lettuce:lettuce-core` | `6.7.1.RELEASE` | B0, B2 |
| `software.amazon.awssdk:bom` | `2.54.17` | B1 (manages `dynamodb`, `apache-client` versions) |
| `com.fasterxml.jackson:jackson-bom` | `2.19.1` | B3 and anywhere JSON is read/written (manages `jackson-databind`, `jackson-datatype-jsr310`) |
| `org.testcontainers:testcontainers-bom` | `1.21.2` | B1–B3, C0b, C2 (manages `junit-jupiter`, `kafka` modules) |
| `org.junit:junit-bom` | `5.10.2` (unchanged — already pinned and green) | everywhere |
| `org.slf4j:slf4j-simple` | `2.0.16` | runtime-only, binds `kafka-clients`'/Lettuce's slf4j-api calls so logs are visible instead of "no provider" warnings |

`kafka-clients` 4.x requires Java 11+ for the client itself. Container images, one tag each for tests and compose (verified with `docker manifest inspect`, recorded in master Global Constraints): `apache/kafka:4.3.1` (matches `kafka-clients`), `redis:7.4-alpine`, `amazon/dynamodb-local:3.3.1`. These versions are authoritative for every thread (controller ruling R2).

---

### Task T0: Gradle build — pinned versions, `integrationTest` source set, `Await` helper

**Files:**
- Modify: `build.gradle.kts` (full rewrite, shown below)
- Create: `src/test/java/com/quince/cartrecovery/Await.java`
- Create: `src/test/java/com/quince/cartrecovery/AwaitTest.java`

**Interfaces:**
- Consumes: nothing (root task).
- Produces: the `integrationTest` Gradle source set and `Test` task, wired so its compile and runtime classpaths include `main` and `test` output (every later thread's contract-test subclasses and infra end-to-end tests compile against it); `./gradlew check` runs `test` then `integrationTest`; `Await.until(BooleanSupplier condition, Duration timeout)` in package `com.quince.cartrecovery`, available to `test` and `integrationTest` alike since `integrationTest` includes `test`'s output on its classpath.

This exact `build.gradle.kts` was verified against this repository (`./gradlew test`, `./gradlew integrationTest`, `./gradlew check`, `./gradlew installDist` all succeed; the existing 78 tests stay green; `integrationTest` reports `NO-SOURCE` since no infra tests exist yet):

- [ ] **Step 1: Replace `build.gradle.kts` in full**

```kotlin
plugins {
    java
    application
}

group = "com.quince"
version = "0.1.0"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

repositories {
    mavenCentral()
}

sourceSets {
    create("integrationTest") {
        java.srcDir("src/integrationTest/java")
        resources.srcDir("src/integrationTest/resources")
        compileClasspath += sourceSets["main"].output + sourceSets["test"].output
        runtimeClasspath += sourceSets["main"].output + sourceSets["test"].output
    }
}

configurations["integrationTestImplementation"].extendsFrom(configurations["testImplementation"])
configurations["integrationTestRuntimeOnly"].extendsFrom(configurations["testRuntimeOnly"])

dependencies {
    implementation(platform("software.amazon.awssdk:bom:2.54.17"))
    implementation(platform("com.fasterxml.jackson:jackson-bom:2.19.1"))

    implementation("org.apache.kafka:kafka-clients:4.3.1")
    implementation("io.lettuce:lettuce-core:6.7.1.RELEASE")
    implementation("software.amazon.awssdk:dynamodb")
    implementation("software.amazon.awssdk:apache-client")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")

    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    "integrationTestImplementation"(platform("org.testcontainers:testcontainers-bom:1.21.2"))
    "integrationTestImplementation"("org.testcontainers:junit-jupiter")
    "integrationTestImplementation"("org.testcontainers:kafka")
}

application {
    mainClass.set("com.quince.cartrecovery.Main")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "failed", "skipped")
        showStandardStreams = false
    }
}

val integrationTest = tasks.register<Test>("integrationTest") {
    description = "Runs integration tests against Docker (Testcontainers)."
    group = "verification"
    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    useJUnitPlatform()
    jvmArgs("-Djdk.tracePinnedThreads=full")
    shouldRunAfter(tasks.test)
    testLogging {
        events("passed", "failed", "skipped")
        showStandardStreams = false
    }
}

tasks.check {
    dependsOn(integrationTest)
}
```

Note on ordering: `sourceSets { create("integrationTest") {...} }` and the `configurations[...].extendsFrom(...)` lines must appear **before** the `dependencies {}` block that references `"integrationTestImplementation"` — Gradle Kotlin DSL scripts execute top to bottom, and the configuration must exist before it can be referenced by name.

- [ ] **Step 2: Run the full test suite to confirm nothing broke**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --console=plain`
Expected: `BUILD SUCCESSFUL`, all 78 existing tests pass (same count as before this task).

- [ ] **Step 3: Run `integrationTest` with no sources yet**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --console=plain`
Expected: `BUILD SUCCESSFUL`; the task log shows `> Task :integrationTest NO-SOURCE`.

- [ ] **Step 4: Run `check` and `installDist`**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew check installDist --console=plain`
Expected: `BUILD SUCCESSFUL`; `check` depends on both `test` and `integrationTest`; `installDist` (from the `application` plugin, unchanged) produces `build/install/abandoned-cart-recovery/`.

- [ ] **Step 5: Commit the build changes**

```bash
git add build.gradle.kts
git commit -m "$(cat <<'EOF'
Pin infra dependency versions and wire an integrationTest source set

kafka-clients, lettuce, the AWS SDK v2 BOM (dynamodb, apache-client),
the Jackson BOM, and the Testcontainers BOM (junit-jupiter, kafka) are
now pinned at current stable versions. integrationTest compiles and
runs against main and test output so later threads can subclass the
test-scope contract test bases from Docker-backed adapters; check now
runs both test and integrationTest.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 6: Write the failing test for `Await`**

Create `src/test/java/com/quince/cartrecovery/AwaitTest.java`:

```java
package com.quince.cartrecovery;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwaitTest {
    @Test void returnsAsSoonAsConditionIsTrue() {
        AtomicInteger calls = new AtomicInteger();
        Await.until(() -> calls.incrementAndGet() >= 3, Duration.ofSeconds(1));
        assertTrue(calls.get() >= 3);
    }

    @Test void throwsAssertionErrorOnTimeout() {
        AssertionError e = assertThrows(AssertionError.class,
            () -> Await.until(() -> false, Duration.ofMillis(120)));
        assertTrue(e.getMessage().contains("condition not met"));
    }
}
```

- [ ] **Step 7: Run it to see it fail to compile**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests "com.quince.cartrecovery.AwaitTest" --console=plain`
Expected: FAIL — compile error, `cannot find symbol: class Await`.

- [ ] **Step 8: Write `Await`**

Create `src/test/java/com/quince/cartrecovery/Await.java`:

```java
package com.quince.cartrecovery;

import java.time.Duration;
import java.time.Instant;
import java.util.function.BooleanSupplier;

/** Polls a condition every 50 ms until it is true, or throws after the timeout elapses. */
public final class Await {
    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);

    private Await() {}

    public static void until(BooleanSupplier condition, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (!condition.getAsBoolean()) {
            if (Instant.now().isAfter(deadline)) {
                throw new AssertionError("condition not met within " + timeout);
            }
            try {
                Thread.sleep(POLL_INTERVAL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for condition", e);
            }
        }
    }
}
```

`Await` lives in `src/test`, not `src/main`, because it is a test helper for both `test` and `integrationTest` (the latter's classpath includes `test`'s output per Step 1); it must never be reachable from production code.

- [ ] **Step 9: Run the test to see it pass**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests "com.quince.cartrecovery.AwaitTest" --console=plain`
Expected: PASS — `AwaitTest > returnsAsSoonAsConditionIsTrue() PASSED`, `AwaitTest > throwsAssertionErrorOnTimeout() PASSED`.

- [ ] **Step 10: Run the whole suite once more**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --console=plain`
Expected: `BUILD SUCCESSFUL`, 80 tests pass (78 existing + 2 new).

- [ ] **Step 11: Commit**

```bash
git add src/test/java/com/quince/cartrecovery/Await.java src/test/java/com/quince/cartrecovery/AwaitTest.java
git commit -m "$(cat <<'EOF'
Add an Await test helper for infra and end-to-end tests

Polls a BooleanSupplier every 50 ms and fails with a clear message on
timeout, so contract and infra end-to-end tests never need a raw sleep
loop of their own.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task D0: Load-test simulator — workload, expected sends, accounting, percentiles, report

**Files:**
- Create: `src/main/java/com/quince/cartrecovery/loadgen/EventType.java`
- Create: `src/main/java/com/quince/cartrecovery/loadgen/ScriptedEvent.java`
- Create: `src/main/java/com/quince/cartrecovery/loadgen/Cycle.java`
- Create: `src/main/java/com/quince/cartrecovery/loadgen/CartScript.java`
- Create: `src/main/java/com/quince/cartrecovery/loadgen/Workload.java`
- Create: `src/main/java/com/quince/cartrecovery/loadgen/Expected.java`
- Create: `src/main/java/com/quince/cartrecovery/loadgen/SinkSend.java`
- Create: `src/main/java/com/quince/cartrecovery/loadgen/OutcomeRow.java`
- Create: `src/main/java/com/quince/cartrecovery/loadgen/Accounting.java`
- Create: `src/main/java/com/quince/cartrecovery/loadgen/LatencySample.java`
- Create: `src/main/java/com/quince/cartrecovery/loadgen/Percentiles.java`
- Create: `src/main/java/com/quince/cartrecovery/loadgen/LoadTestSummary.java`
- Create: `src/main/java/com/quince/cartrecovery/loadgen/Report.java`
- Create: matching test files under `src/test/java/com/quince/cartrecovery/loadgen/`

**Interfaces:**
- Consumes: `com.quince.cartrecovery.model.RecoveryConfig` (unchanged by A1 — used directly for `offsets()`, `latenessBounds()`, `frequencyCap()`, `holdoutPercent()`, `window()`), `com.quince.cartrecovery.model.Arm`, `com.quince.cartrecovery.ports.ArmAssigner` (unchanged, `@FunctionalInterface Arm assign(String shopperKey)`). Nothing from thread A's new model (`CartEvent`, `LedgerKey`, `OutcomeKind`, …) or thread B's codecs — this task depends only on T0.
- Produces (exact signatures D1 and E2 rely on):
  - `Workload.Result` = `record(List<ScriptedEvent> events, List<CartScript> scripts)`
  - `Workload.generate(long seed, String runPrefix, double ratePerSecond, Duration duration, Instant start, RecoveryConfig config) -> Workload.Result`
  - `Workload.generateCarts(long seed, String runPrefix, int cartCount, Duration duration, Instant start, RecoveryConfig config) -> Workload.Result`
  - `Expected.keys(List<CartScript> scripts, RecoveryConfig config, ArmAssigner assigner) -> Set<String>` — keys in the exact `cartId:version:offsetIndex` format `LedgerKey.toString()` (thread A) produces
  - `CartScript.purchaseAt() -> Instant` (nullable) — the cart's purchase instant, or null if it never purchases
  - `Accounting.resolveOutcomes(List<OutcomeRow>) -> Map<String,String>`, `Accounting.countByKind(Map<String,String>, String kind) -> long`, `Accounting.duplicateSends(List<SinkSend>) -> long`, `Accounting.postPurchaseSends(List<SinkSend>, Map<String,Instant> purchaseAtByCart, Duration clockSkew) -> long`, `Accounting.unexplainedMissing(long expected, long sent, long skippedLate, long cancelled, long dead) -> long`, `Accounting.unexplainedMissingRatio(long unexplainedMissing, long expected) -> double`
  - `Percentiles.compute(List<LatencySample> samples, Instant testStart, Duration warmup) -> Percentiles.Result` = `record(long p50Millis, long p95Millis, long p99Millis, int sampleCount)`
  - `Report.render(LoadTestSummary) -> String`, `Report.write(LoadTestSummary, Path reportsDir) -> Path`
  - `LoadTestSummary` — the full record shown in Step 12 below; every field spec §8.5 asks the report to carry

**Design note carried into every step below:** `ScriptedEvent.occurredAt()` and every `Instant` on a `Cycle` live on one nominal timeline anchored at the `start` instant passed into `Workload.generate`. `Expected` computes cancellation purely from those nominal instants. D1's publisher preserves each event's offset from `start` when it replays the script against the real clock (anchoring `start` to the real test-start instant), so the real pipeline's cancellation decisions land on the same instants `Expected` assumed — this is what makes the "expected" count trustworthy.

- [ ] **Step 1: Write `EventType`, `ScriptedEvent`, `Cycle`, `CartScript` (pure data, no test needed)**

`src/main/java/com/quince/cartrecovery/loadgen/EventType.java`:
```java
package com.quince.cartrecovery.loadgen;

/** The scripted event kinds the load generator produces. No CLEAR: not needed to exercise the pipeline. */
public enum EventType { EDIT, RESUME, PURCHASE }
```

`src/main/java/com/quince/cartrecovery/loadgen/ScriptedEvent.java`:
```java
package com.quince.cartrecovery.loadgen;

import java.time.Instant;

/**
 * One scripted cart event, ready to publish to {@code cart-events}. {@code occurredAt} is the
 * script's nominal timeline (relative ordering and gaps only); the publisher anchors the whole
 * script to a real start instant and uses each event's own send time as the real {@code occurredAt}.
 */
public record ScriptedEvent(String cartId, String shopperKey, EventType type, long version,
                             Instant occurredAt, int itemCount) {}
```

`src/main/java/com/quince/cartrecovery/loadgen/Cycle.java`:
```java
package com.quince.cartrecovery.loadgen;

import java.time.Instant;

/**
 * One abandonment cycle in a cart's script: the version and last-activity time the real pipeline
 * would record when the cart falls quiet, and when (if ever) something cancels the pending
 * reminder sequence for this cycle. For every cycle but the last, that is the next cycle's resume
 * time; for the last cycle, it is the cart's purchase time, or null if the cart never purchases.
 */
public record Cycle(long version, Instant lastActivityAt, Instant cancelledAt) {}
```

`src/main/java/com/quince/cartrecovery/loadgen/CartScript.java`:
```java
package com.quince.cartrecovery.loadgen;

import java.time.Instant;
import java.util.List;

/** One cart's whole scripted life: every abandonment cycle it goes through, in order. */
public record CartScript(String cartId, String shopperKey, List<Cycle> cycles) {

    public CartScript {
        cycles = List.copyOf(cycles);
        if (cycles.isEmpty()) throw new IllegalArgumentException("a cart script needs at least one cycle");
    }

    /** The cart's purchase instant, or null if it never purchases. Only the last cycle can end in a purchase. */
    public Instant purchaseAt() {
        return cycles.get(cycles.size() - 1).cancelledAt();
    }
}
```

- [ ] **Step 2: Write the failing test for `Workload`**

Create `src/test/java/com/quince/cartrecovery/loadgen/WorkloadTest.java`:

```java
package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.RecoveryConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkloadTest {
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    private static final RecoveryConfig CONFIG = RecoveryConfig.defaults();

    @Test void sameSeedProducesTheSameScript() {
        Workload.Result a = Workload.generateCarts(42L, "run1", 50, Duration.ofMinutes(5), START, CONFIG);
        Workload.Result b = Workload.generateCarts(42L, "run1", 50, Duration.ofMinutes(5), START, CONFIG);
        assertEquals(a.events(), b.events());
        assertEquals(a.scripts(), b.scripts());
    }

    @Test void generatesExactlyTheRequestedCartCount() {
        Workload.Result result = Workload.generateCarts(1L, "run2", 200, Duration.ofMinutes(5), START, CONFIG);
        assertEquals(200, result.scripts().size());
    }

    @Test void ratePacedGenerationSizesCartCountFromRateAndDuration() {
        Workload.Result result = Workload.generate(1L, "run3", 100.0, Duration.ofMinutes(1), START, CONFIG);
        // ~9.5 events per cart, 100 events/s * 60s = 6000 events, so roughly 6000/9.5 ~= 632 carts
        assertTrue(result.scripts().size() > 500 && result.scripts().size() < 800,
            "expected several hundred carts, got " + result.scripts().size());
    }

    @Test void eventsAreTimeOrdered() {
        Workload.Result result = Workload.generateCarts(7L, "run4", 300, Duration.ofMinutes(5), START, CONFIG);
        List<ScriptedEvent> events = result.events();
        for (int i = 1; i < events.size(); i++) {
            assertTrue(!events.get(i).occurredAt().isBefore(events.get(i - 1).occurredAt()),
                "events must be time-ordered");
        }
    }

    @Test void roughlySeventyPercentOfCartsNeverPurchase() {
        Workload.Result result = Workload.generateCarts(99L, "run5", 2000, Duration.ofMinutes(10), START, CONFIG);
        long neverPurchase = result.scripts().stream().filter(s -> s.purchaseAt() == null).count();
        double ratio = neverPurchase / (double) result.scripts().size();
        assertTrue(ratio > 0.60 && ratio < 0.80, "abandon ratio out of range: " + ratio);
    }

    @Test void noCartExceedsTheDefaultFrequencyCapOfThreeCycles() {
        Workload.Result result = Workload.generateCarts(5L, "run6", 1000, Duration.ofMinutes(10), START, CONFIG);
        for (CartScript script : result.scripts()) {
            assertTrue(script.cycles().size() <= CONFIG.frequencyCap(),
                script.cartId() + " has " + script.cycles().size() + " cycles");
        }
    }

    @Test void everyCartIdCarriesTheRunPrefix() {
        Workload.Result result = Workload.generateCarts(3L, "prefix-xyz", 20, Duration.ofMinutes(1), START, CONFIG);
        for (CartScript script : result.scripts()) {
            assertTrue(script.cartId().startsWith("prefix-xyz-"));
        }
    }

    @Test void anEarlierCyclesCancellationAlwaysLeadsIntoTheNextCycle() {
        Workload.Result result = Workload.generateCarts(11L, "run7", 500, Duration.ofMinutes(5), START, CONFIG);
        for (CartScript script : result.scripts()) {
            List<Cycle> cycles = script.cycles();
            for (int i = 0; i < cycles.size() - 1; i++) {
                if (cycles.get(i).cancelledAt() != null) {
                    assertTrue(!cycles.get(i + 1).lastActivityAt().isBefore(cycles.get(i).cancelledAt()));
                }
            }
        }
    }
}
```

- [ ] **Step 3: Run it to see it fail to compile**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests "com.quince.cartrecovery.loadgen.WorkloadTest" --console=plain`
Expected: FAIL — `cannot find symbol: class Workload`.

- [ ] **Step 4: Write `Workload`**

Create `src/main/java/com/quince/cartrecovery/loadgen/Workload.java`:

```java
package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.RecoveryConfig;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates a deterministic (seeded) simulated shopper workload: about 10 events per cart
 * session, about 70% of carts abandon and never purchase, the rest purchase at a random point
 * (some before any pipeline reminder would fire, some after one or more would already have been
 * sent), and some carts resume after abandoning, starting a new cycle, capped at 3 cycles so no
 * cart ever exceeds the default frequency cap. Every {@code Instant} on the returned events is on
 * a nominal timeline anchored at {@code start}; the publisher preserves each event's offset from
 * {@code start} when replaying against a real clock, so the cancellation math in {@link Expected}
 * (computed from the same nominal timeline) matches what the real pipeline will decide.
 */
public final class Workload {
    private Workload() {}

    private static final double ABANDON_PROBABILITY = 0.70;
    private static final double RESUME_ONE_PROBABILITY = 0.25;
    private static final double RESUME_TWO_PROBABILITY = 0.05;
    private static final double APPROX_EVENTS_PER_CART = 9.5;

    public record Result(List<ScriptedEvent> events, List<CartScript> scripts) {}

    /** Sizes the cart count from the target rate and duration so the script density approximates {@code ratePerSecond}. */
    public static Result generate(long seed, String runPrefix, double ratePerSecond, Duration duration,
                                   Instant start, RecoveryConfig config) {
        int cartCount = Math.max(1, (int) Math.round(ratePerSecond * duration.toSeconds() / APPROX_EVENTS_PER_CART));
        return generateCarts(seed, runPrefix, cartCount, duration, start, config);
    }

    /** Generates exactly {@code cartCount} cart scripts, evenly staggered across {@code duration}. */
    public static Result generateCarts(long seed, String runPrefix, int cartCount, Duration duration,
                                        Instant start, RecoveryConfig config) {
        Random rnd = new Random(seed);
        Duration lastOffset = config.offsets().get(config.offsets().size() - 1);
        List<ScriptedEvent> events = new ArrayList<>();
        List<CartScript> scripts = new ArrayList<>();
        long spacingMillis = cartCount <= 1 ? 0 : Math.max(1, duration.toMillis() / cartCount);

        for (int i = 0; i < cartCount; i++) {
            String cartId = runPrefix + "-" + i;
            String shopperKey = cartId + "-shopper";
            Instant t = start.plusMillis(i * spacingMillis);

            int resumes = pickResumeCount(rnd);
            boolean purchases = rnd.nextDouble() >= ABANDON_PROBABILITY;
            int cycleCount = resumes + 1;

            List<Cycle> cycles = new ArrayList<>();
            long version = 0;
            for (int cycleIndex = 0; cycleIndex < cycleCount; cycleIndex++) {
                boolean isLast = cycleIndex == cycleCount - 1;
                int editCount = 2 + rnd.nextInt(4);
                for (int e = 0; e < editCount; e++) {
                    version++;
                    t = t.plusMillis(50 + rnd.nextInt(250));
                    events.add(new ScriptedEvent(cartId, shopperKey, EventType.EDIT, version, t, 1 + rnd.nextInt(3)));
                }
                Instant lastActivityAt = t;
                long cycleVersion = version;
                Instant cancelledAt = null;
                if (isLast) {
                    if (purchases) {
                        Instant purchaseAt = lastActivityAt.plus(randomCancelGap(rnd, lastOffset));
                        version++;
                        events.add(new ScriptedEvent(cartId, shopperKey, EventType.PURCHASE, version, purchaseAt, 0));
                        cancelledAt = purchaseAt;
                        t = purchaseAt;
                    }
                } else {
                    Instant resumeAt = lastActivityAt.plus(randomCancelGap(rnd, lastOffset));
                    version++;
                    events.add(new ScriptedEvent(cartId, shopperKey, EventType.RESUME, version, resumeAt, 0));
                    cancelledAt = resumeAt;
                    t = resumeAt;
                }
                cycles.add(new Cycle(cycleVersion, lastActivityAt, cancelledAt));
            }
            scripts.add(new CartScript(cartId, shopperKey, cycles));
        }

        events.sort((a, b) -> a.occurredAt().compareTo(b.occurredAt()));
        return new Result(events, scripts);
    }

    private static int pickResumeCount(Random rnd) {
        double r = rnd.nextDouble();
        if (r < RESUME_TWO_PROBABILITY) return 2;
        if (r < RESUME_TWO_PROBABILITY + RESUME_ONE_PROBABILITY) return 1;
        return 0;
    }

    private static Duration randomCancelGap(Random rnd, Duration lastOffset) {
        long maxMillis = Math.max(1, lastOffset.toMillis() * 2);
        return Duration.ofMillis(rnd.nextLong(maxMillis));
    }
}
```

- [ ] **Step 5: Run the test to see it pass**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests "com.quince.cartrecovery.loadgen.WorkloadTest" --console=plain`
Expected: PASS — all 8 `WorkloadTest` cases pass.

- [ ] **Step 6: Write the failing test for `Expected`**

Create `src/test/java/com/quince/cartrecovery/loadgen/ExpectedTest.java`:

```java
package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.ArmAssigner;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExpectedTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final RecoveryConfig CONFIG = RecoveryConfig.defaults(); // offsets 30m, 1h, 24h

    private static final ArmAssigner ALL_TREATMENT = shopperKey -> Arm.TREATMENT;
    private static final ArmAssigner ALL_HOLDOUT = shopperKey -> Arm.HOLDOUT;

    @Test void aCartThatNeverPurchasesGetsEveryOffset() {
        Cycle cycle = new Cycle(1L, T0, null);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(cycle));

        Set<String> keys = Expected.keys(List.of(script), CONFIG, ALL_TREATMENT);

        assertEquals(Set.of("cart-1:1:0", "cart-1:1:1", "cart-1:1:2"), keys);
    }

    @Test void holdoutCartsGetNoKeysAtAll() {
        Cycle cycle = new Cycle(1L, T0, null);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(cycle));

        Set<String> keys = Expected.keys(List.of(script), CONFIG, ALL_HOLDOUT);

        assertTrue(keys.isEmpty());
    }

    @Test void aPurchaseBeforeTheFirstOffsetIsDueCancelsEverything() {
        Instant purchaseAt = T0.plus(CONFIG.offsets().get(0).dividedBy(2));
        Cycle cycle = new Cycle(1L, T0, purchaseAt);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(cycle));

        Set<String> keys = Expected.keys(List.of(script), CONFIG, ALL_TREATMENT);

        assertTrue(keys.isEmpty());
    }

    @Test void aPurchaseBetweenTheFirstAndSecondOffsetLeavesOnlyTheFirstExpected() {
        Instant purchaseAt = T0.plus(CONFIG.offsets().get(0)).plusSeconds(1);
        Cycle cycle = new Cycle(1L, T0, purchaseAt);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(cycle));

        Set<String> keys = Expected.keys(List.of(script), CONFIG, ALL_TREATMENT);

        assertEquals(Set.of("cart-1:1:0"), keys);
    }

    @Test void aPurchaseExactlyAtTheDueInstantCancelsThatOffsetToo() {
        Instant purchaseAt = T0.plus(CONFIG.offsets().get(0));
        Cycle cycle = new Cycle(1L, T0, purchaseAt);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(cycle));

        Set<String> keys = Expected.keys(List.of(script), CONFIG, ALL_TREATMENT);

        assertTrue(keys.isEmpty());
    }

    @Test void aResumeCancelsOnlyTheEarlierCycleNotTheNewOne() {
        Instant resumeAt = T0.plus(CONFIG.offsets().get(0)).plusSeconds(1);
        Cycle first = new Cycle(1L, T0, resumeAt);
        Cycle second = new Cycle(2L, resumeAt.plusSeconds(1), null);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(first, second));

        Set<String> keys = Expected.keys(List.of(script), CONFIG, ALL_TREATMENT);

        assertEquals(Set.of("cart-1:1:0", "cart-1:2:0", "cart-1:2:1", "cart-1:2:2"), keys);
    }

    @Test void aCycleBeyondTheFrequencyCapSendsNothing() {
        RecoveryConfig capOfOne = CONFIG.withFrequencyCap(1);
        Cycle first = new Cycle(1L, T0, T0.plusSeconds(1));
        Cycle second = new Cycle(2L, T0.plusSeconds(2), null);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(first, second));

        Set<String> keys = Expected.keys(List.of(script), capOfOne, ALL_TREATMENT);

        assertTrue(keys.stream().noneMatch(k -> k.startsWith("cart-1:2:")));
    }
}
```

- [ ] **Step 7: Run it to see it fail to compile**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests "com.quince.cartrecovery.loadgen.ExpectedTest" --console=plain`
Expected: FAIL — `cannot find symbol: class Expected`.

- [ ] **Step 8: Write `Expected`**

Create `src/main/java/com/quince/cartrecovery/loadgen/Expected.java`:

```java
package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.ArmAssigner;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Computes, from each cart's script alone, the set of reminder keys ({@code cartId:version:offsetIndex},
 * the same format as the real ledger key) a correct pipeline should send: holdout carts send nothing;
 * within a cycle, offset {@code i} is expected unless something (a resume starting the next cycle, or
 * the cart's purchase on the last cycle) cancelled it at or before its due time; a cycle beyond the
 * configured frequency cap sends nothing.
 */
public final class Expected {
    private Expected() {}

    public static Set<String> keys(List<CartScript> scripts, RecoveryConfig config, ArmAssigner assigner) {
        Set<String> keys = new LinkedHashSet<>();
        for (CartScript script : scripts) {
            if (assigner.assign(script.shopperKey()) == Arm.HOLDOUT) continue;
            List<Cycle> cycles = script.cycles();
            for (int cycleIndex = 0; cycleIndex < cycles.size(); cycleIndex++) {
                if (cycleIndex + 1 > config.frequencyCap()) break;
                Cycle cycle = cycles.get(cycleIndex);
                keys.addAll(keysForCycle(script.cartId(), cycle, config));
            }
        }
        return keys;
    }

    private static Set<String> keysForCycle(String cartId, Cycle cycle, RecoveryConfig config) {
        Set<String> keys = new LinkedHashSet<>();
        List<Duration> offsets = config.offsets();
        for (int i = 0; i < offsets.size(); i++) {
            Instant dueAt = cycle.lastActivityAt().plus(offsets.get(i));
            if (cycle.cancelledAt() != null && !cycle.cancelledAt().isAfter(dueAt)) break;
            keys.add(cartId + ":" + cycle.version() + ":" + i);
        }
        return keys;
    }
}
```

- [ ] **Step 9: Run the test to see it pass**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests "com.quince.cartrecovery.loadgen.ExpectedTest" --console=plain`
Expected: PASS — all 7 `ExpectedTest` cases pass.

- [ ] **Step 10: Write the failing test for `Accounting`**

Create `src/test/java/com/quince/cartrecovery/loadgen/AccountingTest.java`:

```java
package com.quince.cartrecovery.loadgen;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AccountingTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    @Test void eachKeyResolvesByPrecedenceSentBeatsEverythingElse() {
        List<OutcomeRow> rows = List.of(
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "SKIPPED_LATE", T0, 1),
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "SENT", T0.plusSeconds(1), 1));

        Map<String, String> resolved = Accounting.resolveOutcomes(rows);

        assertEquals("SENT", resolved.get("k1"));
    }

    @Test void deadBeatsCancelledBeatsSkippedLate() {
        assertEquals("DEAD", Accounting.resolveOutcomes(List.of(
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "CANCELLED", T0, 1),
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "DEAD", T0, 1))).get("k1"));
        assertEquals("CANCELLED", Accounting.resolveOutcomes(List.of(
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "SKIPPED_LATE", T0, 1),
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "CANCELLED", T0, 1))).get("k1"));
    }

    @Test void abandonedRowsWithNoKeyAreDropped() {
        List<OutcomeRow> rows = List.of(new OutcomeRow(null, "cart-1", 1, "TREATMENT", "ABANDONED", T0, 0));
        assertEquals(Map.of(), Accounting.resolveOutcomes(rows));
    }

    @Test void countByKindCountsResolvedKinds() {
        Map<String, String> resolved = Map.of("k1", "SENT", "k2", "SENT", "k3", "DEAD");
        assertEquals(2, Accounting.countByKind(resolved, "SENT"));
        assertEquals(1, Accounting.countByKind(resolved, "DEAD"));
        assertEquals(0, Accounting.countByKind(resolved, "CANCELLED"));
    }

    @Test void duplicateSendsCountsSendsBeyondTheFirstPerKey() {
        List<SinkSend> sends = List.of(
            new SinkSend("k1", "cart-1", T0, true, 1),
            new SinkSend("k1", "cart-1", T0.plusSeconds(1), true, 1),
            new SinkSend("k2", "cart-2", T0, true, 1));

        assertEquals(1, Accounting.duplicateSends(sends));
    }

    @Test void noDuplicatesWhenEveryKeySendsOnce() {
        List<SinkSend> sends = List.of(
            new SinkSend("k1", "cart-1", T0, true, 1),
            new SinkSend("k2", "cart-2", T0, true, 1));

        assertEquals(0, Accounting.duplicateSends(sends));
    }

    @Test void postPurchaseSendsCountsOnlySendsWellAfterThePurchase() {
        Duration clockSkew = Duration.ofSeconds(5);
        Instant purchaseAt = T0;
        Map<String, Instant> purchaseAtByCart = Map.of("cart-1", purchaseAt);
        List<SinkSend> sends = List.of(
            new SinkSend("k1", "cart-1", purchaseAt.plusSeconds(3), true, 1),   // within clockSkew + 1s: not counted
            new SinkSend("k2", "cart-1", purchaseAt.plusSeconds(10), true, 1),  // well after: counted
            new SinkSend("k3", "cart-2", purchaseAt.plusSeconds(100), true, 1)); // no purchase on record: not counted

        assertEquals(1, Accounting.postPurchaseSends(sends, purchaseAtByCart, clockSkew));
    }

    @Test void unexplainedMissingIsExpectedMinusEveryAccountedOutcome() {
        assertEquals(2, Accounting.unexplainedMissing(100, 80, 10, 5, 3));
    }

    @Test void unexplainedMissingRatioGuardsAgainstZeroExpected() {
        assertEquals(0.0, Accounting.unexplainedMissingRatio(0, 0));
        assertEquals(0.02, Accounting.unexplainedMissingRatio(2, 100));
    }
}
```

- [ ] **Step 11: Run it to see it fail to compile**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests "com.quince.cartrecovery.loadgen.AccountingTest" --console=plain`
Expected: FAIL — `cannot find symbol: class SinkSend` (and `OutcomeRow`, `Accounting`).

- [ ] **Step 12: Write `SinkSend`, `OutcomeRow`, `Accounting`**

Create `src/main/java/com/quince/cartrecovery/loadgen/SinkSend.java`:

```java
package com.quince.cartrecovery.loadgen;

import java.time.Instant;

/** Mirrors one {@code sink-sends} record: one row per {@code NotificationSink.send()} call. */
public record SinkSend(String key, String cartId, Instant at, boolean hasFirstName, int itemCount) {}
```

Create `src/main/java/com/quince/cartrecovery/loadgen/OutcomeRow.java`:

```java
package com.quince.cartrecovery.loadgen;

import java.time.Instant;

/**
 * Mirrors one {@code reminder-outcomes} record. {@code key} is null for {@code ABANDONED} rows,
 * exactly as the real {@code Outcome} model leaves it; {@code kind} is one of {@code ABANDONED},
 * {@code SENT}, {@code SKIPPED_LATE}, {@code CANCELLED}, {@code DEAD}, kept as a plain string so
 * this thread never depends on thread A's {@code OutcomeKind} enum.
 */
public record OutcomeRow(String key, String cartId, long version, String arm, String kind, Instant at, int attempts) {}
```

Create `src/main/java/com/quince/cartrecovery/loadgen/Accounting.java`:

```java
package com.quince.cartrecovery.loadgen;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves observed {@code sink-sends} and {@code reminder-outcomes} rows into the correctness
 * counts the load test reports. Every method is a pure function over the rows passed in; the
 * caller (the loadgen role) is responsible for reading those rows from Kafka, filtered to its run.
 */
public final class Accounting {
    private static final List<String> PRECEDENCE = List.of("SENT", "DEAD", "CANCELLED", "SKIPPED_LATE");

    private Accounting() {}

    /** Each key resolves to exactly one kind, by precedence SENT > DEAD > CANCELLED > SKIPPED_LATE. Rows with a null key ({@code ABANDONED}) are dropped. */
    public static Map<String, String> resolveOutcomes(List<OutcomeRow> outcomes) {
        Map<String, String> best = new HashMap<>();
        for (OutcomeRow row : outcomes) {
            if (row.key() == null) continue;
            String current = best.get(row.key());
            if (current == null || precedenceRank(row.kind()) < precedenceRank(current)) {
                best.put(row.key(), row.kind());
            }
        }
        return best;
    }

    private static int precedenceRank(String kind) {
        int i = PRECEDENCE.indexOf(kind);
        return i < 0 ? PRECEDENCE.size() : i;
    }

    public static long countByKind(Map<String, String> resolved, String kind) {
        return resolved.values().stream().filter(kind::equals).count();
    }

    /** Sends beyond the first per key: a healthy pipeline has zero. */
    public static long duplicateSends(List<SinkSend> sends) {
        Map<String, Long> perKey = new HashMap<>();
        for (SinkSend send : sends) {
            perKey.merge(send.key(), 1L, Long::sum);
        }
        return perKey.values().stream().mapToLong(count -> Math.max(0, count - 1)).sum();
    }

    /** A send whose cart had a purchase more than {@code clockSkew + 1s} before the send: a healthy pipeline has zero. */
    public static long postPurchaseSends(List<SinkSend> sends, Map<String, Instant> purchaseAtByCart, Duration clockSkew) {
        Duration threshold = clockSkew.plusSeconds(1);
        long count = 0;
        for (SinkSend send : sends) {
            Instant purchaseAt = purchaseAtByCart.get(send.cartId());
            if (purchaseAt == null) continue;
            if (Duration.between(purchaseAt, send.at()).compareTo(threshold) > 0) count++;
        }
        return count;
    }

    /** expected - sent - skippedLate - cancelled - dead, per spec §8.5. */
    public static long unexplainedMissing(long expected, long sent, long skippedLate, long cancelled, long dead) {
        return expected - sent - skippedLate - cancelled - dead;
    }

    public static double unexplainedMissingRatio(long unexplainedMissing, long expected) {
        return expected == 0 ? 0.0 : unexplainedMissing / (double) expected;
    }
}
```

- [ ] **Step 13: Run the test to see it pass**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests "com.quince.cartrecovery.loadgen.AccountingTest" --console=plain`
Expected: PASS — all 9 `AccountingTest` cases pass.

- [ ] **Step 14: Write the failing test for `Percentiles`**

Create `src/test/java/com/quince/cartrecovery/loadgen/PercentilesTest.java`:

```java
package com.quince.cartrecovery.loadgen;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PercentilesTest {
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    @Test void hundredEvenlySpacedSamplesGiveTheExpectedRankedPercentiles() {
        List<LatencySample> samples = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            samples.add(new LatencySample(START.plusSeconds(60), Duration.ofMillis(i)));
        }

        Percentiles.Result result = Percentiles.compute(samples, START, Duration.ofSeconds(30));

        assertEquals(50, result.p50Millis());
        assertEquals(95, result.p95Millis());
        assertEquals(99, result.p99Millis());
        assertEquals(100, result.sampleCount());
    }

    @Test void samplesScheduledDuringWarmupAreExcluded() {
        List<LatencySample> samples = List.of(
            new LatencySample(START.plusSeconds(5), Duration.ofMillis(9999)),   // during warm-up: excluded
            new LatencySample(START.plusSeconds(31), Duration.ofMillis(100)));  // after warm-up: included

        Percentiles.Result result = Percentiles.compute(samples, START, Duration.ofSeconds(30));

        assertEquals(1, result.sampleCount());
        assertEquals(100, result.p50Millis());
    }

    @Test void noSamplesAfterWarmupGivesAnEmptyResultNotAnException() {
        List<LatencySample> samples = List.of(
            new LatencySample(START.plusSeconds(5), Duration.ofMillis(50)));

        Percentiles.Result result = Percentiles.compute(samples, START, Duration.ofSeconds(30));

        assertEquals(0, result.sampleCount());
        assertEquals(0, result.p50Millis());
    }

    @Test void aSampleExactlyAtTheWarmupBoundaryIsIncluded() {
        List<LatencySample> samples = List.of(
            new LatencySample(START.plusSeconds(30), Duration.ofMillis(42)));

        Percentiles.Result result = Percentiles.compute(samples, START, Duration.ofSeconds(30));

        assertEquals(1, result.sampleCount());
    }
}
```

- [ ] **Step 15: Run it to see it fail to compile**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests "com.quince.cartrecovery.loadgen.PercentilesTest" --console=plain`
Expected: FAIL — `cannot find symbol: class LatencySample`.

- [ ] **Step 16: Write `LatencySample` and `Percentiles`**

Create `src/main/java/com/quince/cartrecovery/loadgen/LatencySample.java`:

```java
package com.quince.cartrecovery.loadgen;

import java.time.Duration;
import java.time.Instant;

/** One scheduled-to-sent latency observation, timestamped by when the reminder was scheduled for. */
public record LatencySample(Instant scheduledFor, Duration latency) {}
```

Create `src/main/java/com/quince/cartrecovery/loadgen/Percentiles.java`:

```java
package com.quince.cartrecovery.loadgen;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/** p50/p95/p99 of scheduled-to-sent latency, excluding a warm-up window at the start of the run. */
public final class Percentiles {
    private Percentiles() {}

    public record Result(long p50Millis, long p95Millis, long p99Millis, int sampleCount) {
        static final Result EMPTY = new Result(0, 0, 0, 0);
    }

    public static Result compute(List<LatencySample> samples, Instant testStart, Duration warmup) {
        Instant warmupEnds = testStart.plus(warmup);
        long[] sortedMillis = samples.stream()
            .filter(s -> !s.scheduledFor().isBefore(warmupEnds))
            .map(LatencySample::latency)
            .mapToLong(Duration::toMillis)
            .sorted()
            .toArray();

        if (sortedMillis.length == 0) return Result.EMPTY;

        return new Result(percentile(sortedMillis, 50), percentile(sortedMillis, 95),
            percentile(sortedMillis, 99), sortedMillis.length);
    }

    /** Nearest-rank percentile: index = ceil(p/100 * n) - 1, clamped into range. */
    private static long percentile(long[] sortedMillis, int p) {
        int n = sortedMillis.length;
        int index = (int) Math.ceil(p / 100.0 * n) - 1;
        index = Math.max(0, Math.min(n - 1, index));
        return sortedMillis[index];
    }
}
```

- [ ] **Step 17: Run the test to see it pass**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests "com.quince.cartrecovery.loadgen.PercentilesTest" --console=plain`
Expected: PASS — all 4 `PercentilesTest` cases pass.

- [ ] **Step 18: Write the failing test for `Report`**

Create `src/test/java/com/quince/cartrecovery/loadgen/ReportTest.java`:

```java
package com.quince.cartrecovery.loadgen;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportTest {
    private static LoadTestSummary sample() {
        return new LoadTestSummary(
            "run-abc123",
            Instant.parse("2026-09-26T10:00:00Z"),
            Instant.parse("2026-09-26T10:05:12Z"),
            5000.0, 4870.3,
            Map.of("detector", 12000L, "dispatcher-fast", 300L),
            Map.of("detector", 0L, "dispatcher-fast", 0L),
            "detector",
            Map.of("fast", new Percentiles.Result(800, 1500, 2200, 4000),
                   "slow", new Percentiles.Result(900, 1600, 2400, 500)),
            10000, 9990, 5, 3, 2,
            0, 0,
            0, 0.0,
            8, 16L * 1024 * 1024 * 1024);
    }

    @Test void rendersEveryFieldFromSpec85() {
        String md = Report.render(sample());

        assertTrue(md.contains("run-abc123"));
        assertTrue(md.contains("5000.0"));
        assertTrue(md.contains("4870.3"));
        assertTrue(md.contains("detector"));
        assertTrue(md.contains("Bottleneck stage: **detector**"));
        assertTrue(md.contains("fast"));
        assertTrue(md.contains("slow"));
        assertTrue(md.contains("Duplicate sends: **0**"));
        assertTrue(md.contains("Post-purchase sends: **0**"));
        assertTrue(md.contains("Unexplained missing: 0"));
        assertTrue(md.contains("Cores: 8"));
        assertTrue(md.contains("shares this machine"));
    }

    @Test void writeCreatesATimestampedMarkdownFileUnderTheReportsDir(@TempDir Path tempDir) throws IOException {
        Path reportsDir = tempDir.resolve("build/reports/load");

        Path written = Report.write(sample(), reportsDir);

        assertTrue(Files.exists(written));
        assertTrue(written.getFileName().toString().endsWith(".md"));
        assertEquals(reportsDir, written.getParent());
        assertTrue(Files.readString(written).contains("run-abc123"));
    }
}
```

- [ ] **Step 19: Run it to see it fail to compile**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests "com.quince.cartrecovery.loadgen.ReportTest" --console=plain`
Expected: FAIL — `cannot find symbol: class LoadTestSummary`.

- [ ] **Step 20: Write `LoadTestSummary` and `Report`**

Create `src/main/java/com/quince/cartrecovery/loadgen/LoadTestSummary.java`:

```java
package com.quince.cartrecovery.loadgen;

import java.time.Instant;
import java.util.Map;

/** Every field spec section 8.5 asks the load test report to carry. */
public record LoadTestSummary(
    String runId,
    Instant startedAt,
    Instant finishedAt,
    double targetRatePerSecond,
    double achievedRatePerSecond,
    Map<String, Long> maxConsumerLagByGroup,
    Map<String, Long> endingConsumerLagByGroup,
    String bottleneckStage,
    Map<String, Percentiles.Result> latencyByLane,
    long expectedSends,
    long sentSends,
    long skippedLate,
    long cancelled,
    long dead,
    long duplicateSends,
    long postPurchaseSends,
    long unexplainedMissing,
    double unexplainedMissingRatio,
    int machineCores,
    long machineMemoryBytes) {}
```

Create `src/main/java/com/quince/cartrecovery/loadgen/Report.java`:

```java
package com.quince.cartrecovery.loadgen;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/** Renders the load test summary as markdown (spec 8.5) and writes it to {@code build/reports/load/<timestamp>.md}. */
public final class Report {
    private static final DateTimeFormatter FILE_STAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss");

    private Report() {}

    public static String render(LoadTestSummary s) {
        StringBuilder md = new StringBuilder();
        md.append("# Load test report: ").append(s.runId()).append("\n\n");
        md.append("Started: ").append(s.startedAt()).append("  \n");
        md.append("Finished: ").append(s.finishedAt()).append("\n\n");

        md.append("## Throughput\n\n");
        md.append("- Target rate: ").append(s.targetRatePerSecond()).append(" events/s\n");
        md.append("- Achieved rate: ").append(s.achievedRatePerSecond()).append(" events/s\n\n");

        md.append("## Consumer lag by group\n\n");
        md.append("| Group | Max lag | Ending lag |\n|---|---|---|\n");
        for (String group : s.maxConsumerLagByGroup().keySet()) {
            md.append("| ").append(group).append(" | ")
              .append(s.maxConsumerLagByGroup().get(group)).append(" | ")
              .append(s.endingConsumerLagByGroup().getOrDefault(group, 0L)).append(" |\n");
        }
        md.append("\nBottleneck stage: **").append(s.bottleneckStage()).append("**\n\n");

        md.append("## Scheduled-to-sent latency by lane (ms, excluding the 30 s warm-up)\n\n");
        md.append("| Lane | p50 | p95 | p99 | samples |\n|---|---|---|---|---|\n");
        for (Map.Entry<String, Percentiles.Result> e : s.latencyByLane().entrySet()) {
            Percentiles.Result r = e.getValue();
            md.append("| ").append(e.getKey()).append(" | ").append(r.p50Millis()).append(" | ")
              .append(r.p95Millis()).append(" | ").append(r.p99Millis()).append(" | ")
              .append(r.sampleCount()).append(" |\n");
        }
        md.append("\n");

        md.append("## Outcomes\n\n");
        md.append("| Expected | Sent | Skipped late | Cancelled | Dead |\n|---|---|---|---|---|\n");
        md.append("| ").append(s.expectedSends()).append(" | ").append(s.sentSends()).append(" | ")
          .append(s.skippedLate()).append(" | ").append(s.cancelled()).append(" | ").append(s.dead()).append(" |\n\n");

        md.append("## Correctness at the sink\n\n");
        md.append("- Duplicate sends: **").append(s.duplicateSends()).append("**\n");
        md.append("- Post-purchase sends: **").append(s.postPurchaseSends()).append("**\n");
        md.append("- Unexplained missing: ").append(s.unexplainedMissing())
          .append(" (").append(String.format("%.4f", s.unexplainedMissingRatio() * 100)).append("% of expected)\n\n");

        md.append("## Machine\n\n");
        md.append("- Cores: ").append(s.machineCores()).append("\n");
        md.append("- Memory: ").append(s.machineMemoryBytes() / (1024 * 1024)).append(" MB\n");
        md.append("- Note: the loadgen process shares this machine with every role and every infra container; ")
          .append("these figures are not an isolated benchmark.\n");

        return md.toString();
    }

    /** Writes the rendered report to {@code reportsDir/<timestamp>.md} and returns the path written. */
    public static Path write(LoadTestSummary s, Path reportsDir) {
        try {
            Files.createDirectories(reportsDir);
            String fileName = FILE_STAMP.format(s.startedAt().atZone(java.time.ZoneOffset.UTC)) + ".md";
            Path target = reportsDir.resolve(fileName);
            Files.writeString(target, render(s));
            return target;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
```

- [ ] **Step 21: Run the test to see it pass**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests "com.quince.cartrecovery.loadgen.ReportTest" --console=plain`
Expected: PASS — both `ReportTest` cases pass.

- [ ] **Step 22: Run the whole suite**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --console=plain`
Expected: `BUILD SUCCESSFUL`, 110 tests pass (80 from T0 + 30 new `loadgen` tests: 8 `WorkloadTest` + 7 `ExpectedTest` + 9 `AccountingTest` + 4 `PercentilesTest` + 2 `ReportTest`).

- [ ] **Step 23: Commit**

```bash
git add src/main/java/com/quince/cartrecovery/loadgen src/test/java/com/quince/cartrecovery/loadgen
git commit -m "$(cat <<'EOF'
Add the load-test simulator: workload script, expected sends, accounting, report

Pure, dependency-light logic with no Kafka: a seeded Workload generates
cart scripts (~10 events per session, ~70% abandon, some resume, capped
at 3 cycles); Expected replays each script's cancellation math against
RecoveryConfig and the holdout assignment to compute the keys a correct
pipeline should send; Accounting resolves observed sink-sends and
reminder-outcomes rows into duplicate, post-purchase, and unexplained-
missing counts; Percentiles and Report cover the rest of spec 8.5.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task D1: Loadgen role — paced producer, outcome collection, lag sampling, compose wiring

**Files:**
- Create: `src/main/java/com/quince/cartrecovery/loadgen/LoadgenRole.java`
- Create: `src/main/java/com/quince/cartrecovery/loadgen/LagSampler.java`
- Create: `src/main/java/com/quince/cartrecovery/loadgen/OutcomeCollector.java`
- Create: `src/test/java/com/quince/cartrecovery/loadgen/LoadgenRoleTest.java`

**Interfaces:**
- Consumes: `com.quince.cartrecovery.app.Role` (frozen §1.5: `String name(); void run(InfraConfig config, Health health, Metrics metrics) throws Exception;`, returns when its thread is interrupted), `com.quince.cartrecovery.app.InfraConfig`, `com.quince.cartrecovery.app.Health`, `com.quince.cartrecovery.core.Metrics`, `com.quince.cartrecovery.model.{CartEvent,CartItem,LedgerKey,Lane}` (thread A), B3's `JsonCodec.encode(CartEvent)` and `Topics.{CART_EVENTS,SINK_SENDS,OUTCOMES}` (master §1.6), everything from D0.
- Produces: `LoadgenRole` (public no-arg constructor) registered by C1c's `RoleRegistry` under role name `"loadgen"`; `LoadgenRole.drainWait(RecoveryConfig, Duration reconcileInterval) -> Duration` (package-visible static helper, unit tested without Docker).

**Resolved by controller rulings:**
1. B3's codec is called directly: `JsonCodec.encode(CartEvent)` (controller ruling R7). Reading `sink-sends` and `reminder-outcomes` uses a small local Jackson `ObjectMapper` against the wire field names (epoch-millisecond times), so this task depends on no B3 read-side method.
2. Consumer group ids `"detector"`, `"dispatcher-fast"`, `"dispatcher-slow"` match C1a and C1b.
3. No CLI arguments (controller ruling R9): `LoadgenRole` reads `RATE` (events/second, default `5000`), `DURATION` (ISO-8601, default `PT5M`) and `RUN_PREFIX` (default `"load-" + currentTimeMillis()`) from the environment. C0c's compose `loadgen` service sets `RATE` and `DURATION`; override per run with `docker compose --profile load run --rm -e RATE=50 -e DURATION=PT60S loadgen`.
4. Latency by lane (controller ruling R11): no payload change. For each `SENT` outcome the collector parses the ledger key (`cartId`, `version`, `offsetIndex`), looks up that version's `lastActivityAt` in its own script (shifted onto the real timeline the publisher uses), sets `scheduledFor = lastActivityAt + offsets[offsetIndex]`, and picks the lane with `Lane.of(offsetIndex, FAST_OFFSETS)`.
5. The arm salt is `"cart-recovery-v1"`, the salt every infra role uses (`RoleContext.ARM_SALT`), so `Expected` excludes exactly the holdout carts the detector assigns.

- [ ] **Step 1: Write the failing test for the pure helper**

Create `src/test/java/com/quince/cartrecovery/loadgen/LoadgenRoleTest.java`:

```java
package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.RecoveryConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LoadgenRoleTest {
    @Test void drainWaitIsWindowPlusLastOffsetPlusLastLatenessPlusOneReconcileInterval() {
        RecoveryConfig config = RecoveryConfig.defaults(); // window 30m, last offset 24h, last lateness 30m
        Duration reconcileInterval = Duration.ofMinutes(5);

        Duration wait = LoadgenRole.drainWait(config, reconcileInterval);

        Duration expected = config.window()
            .plus(config.offsets().get(config.offsets().size() - 1))
            .plus(config.latenessBounds().get(config.latenessBounds().size() - 1))
            .plus(reconcileInterval);
        assertEquals(expected, wait);
    }
}
```

- [ ] **Step 2: Run it to see it fail to compile**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests "com.quince.cartrecovery.loadgen.LoadgenRoleTest" --console=plain`
Expected: FAIL — `cannot find symbol: class LoadgenRole` (this will keep failing until thread C0a's `app.Role`/`app.InfraConfig`/`app.Health` and thread A's `core.Metrics`/`model.CartEvent` exist on this branch — confirm those tasks are merged into `infra` first, per the master plan's wave table).

- [ ] **Step 3: Write `LoadgenRole`**

Create `src/main/java/com/quince/cartrecovery/loadgen/LoadgenRole.java`:

```java
package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.app.Health;
import com.quince.cartrecovery.app.InfraConfig;
import com.quince.cartrecovery.app.Role;
import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.infra.kafka.JsonCodec;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.inmemory.HashArmAssigner;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.ArmAssigner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * One-off role: scripts a workload (D0), publishes it to {@code cart-events} at a real-time-paced
 * rate, waits out the drain period, reads back {@code sink-sends} and {@code reminder-outcomes}
 * filtered to its own run prefix, and writes the load test report (spec 8.5). Takes no CLI arguments:
 * RATE, DURATION and RUN_PREFIX come from the environment (controller ruling R9).
 */
public final class LoadgenRole implements Role {
    /** The salt every infra role uses (RoleContext.ARM_SALT), so Expected skips exactly the detector's holdout carts. */
    static final String ARM_SALT = "cart-recovery-v1";

    @Override public String name() { return "loadgen"; }

    @Override public void run(InfraConfig config, Health health, Metrics metrics) throws Exception {
        double rate = Double.parseDouble(System.getenv().getOrDefault("RATE", "5000"));
        Duration duration = Duration.parse(System.getenv().getOrDefault("DURATION", "PT5M"));
        String runPrefix = System.getenv().getOrDefault("RUN_PREFIX", "load-" + System.currentTimeMillis());

        Instant testStart = Instant.now();
        Workload.Result workload = Workload.generate(testStart.toEpochMilli(), runPrefix, rate, duration, testStart, config.recovery());

        ArmAssigner assigner = new HashArmAssigner(ARM_SALT, config.recovery().holdoutPercent());
        Set<String> expectedKeys = Expected.keys(workload.scripts(), config.recovery(), assigner);
        // The publisher replays the nominal script shifted so its first event lands at testStart.
        Duration shift = workload.events().isEmpty() ? Duration.ZERO
            : Duration.between(workload.events().get(0).occurredAt(), testStart);
        Map<String, Instant> purchaseAtByCart = new HashMap<>();
        Map<String, Instant> lastActivityByCartVersion = new HashMap<>();   // "cartId:version" -> real lastActivityAt
        for (CartScript script : workload.scripts()) {
            if (script.purchaseAt() != null) purchaseAtByCart.put(script.cartId(), script.purchaseAt().plus(shift));
            for (Cycle cycle : script.cycles()) {
                lastActivityByCartVersion.put(script.cartId() + ":" + cycle.version(), cycle.lastActivityAt().plus(shift));
            }
        }

        List<String> groups = List.of("detector", "dispatcher-fast", "dispatcher-slow");
        LagSampler lagSampler = new LagSampler(config.kafkaBootstrap(), groups, health);
        OutcomeCollector collector = new OutcomeCollector(config.kafkaBootstrap(), runPrefix, lastActivityByCartVersion,
            config.recovery().offsets(), config.dispatch().fastOffsets());

        lagSampler.start();
        collector.start();

        try (Producer<String, byte[]> producer = buildProducer(config.kafkaBootstrap())) {
            publishPaced(producer, workload.events(), testStart, health);
        }

        Duration drainWait = drainWait(config.recovery(), config.reconcileInterval());
        health.setReady("loadgen.phase", "draining");
        Thread.sleep(drainWait.toMillis());

        lagSampler.stop();
        collector.stop();

        List<SinkSend> sends = collector.sinkSends();
        List<OutcomeRow> outcomes = collector.outcomes();

        Map<String, String> resolved = Accounting.resolveOutcomes(outcomes);
        long sent = Accounting.countByKind(resolved, "SENT");
        long skippedLate = Accounting.countByKind(resolved, "SKIPPED_LATE");
        long cancelled = Accounting.countByKind(resolved, "CANCELLED");
        long dead = Accounting.countByKind(resolved, "DEAD");
        long duplicates = Accounting.duplicateSends(sends);
        long postPurchase = Accounting.postPurchaseSends(sends, purchaseAtByCart, config.dispatch().clockSkew());
        long unexplainedMissing = Accounting.unexplainedMissing(expectedKeys.size(), sent, skippedLate, cancelled, dead);
        double ratio = Accounting.unexplainedMissingRatio(unexplainedMissing, expectedKeys.size());

        Map<String, Percentiles.Result> latencyByLane = Map.of(
            "fast", Percentiles.compute(collector.fastLatencies(), testStart, Duration.ofSeconds(30)),
            "slow", Percentiles.compute(collector.slowLatencies(), testStart, Duration.ofSeconds(30)));

        Instant finished = Instant.now();
        double achievedRate = workload.events().size() / (double) Duration.between(testStart, finished).toSeconds();

        LoadTestSummary summary = new LoadTestSummary(
            runPrefix, testStart, finished, rate, achievedRate,
            lagSampler.maxLagByGroup(), lagSampler.endingLagByGroup(), lagSampler.bottleneckStage(),
            latencyByLane,
            expectedKeys.size(), sent, skippedLate, cancelled, dead,
            duplicates, postPurchase, unexplainedMissing, ratio,
            Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().maxMemory());

        java.nio.file.Path written = Report.write(summary, java.nio.file.Path.of("build/reports/load"));
        System.out.println(Report.render(summary));
        System.out.println("Report written to " + written);
        health.beat("loadgen");
    }

    /** window + last offset + last lateness bound + one reconcile interval: how long to wait for the tail of the sequence to finish. */
    static Duration drainWait(RecoveryConfig recovery, Duration reconcileInterval) {
        Duration lastOffset = recovery.offsets().get(recovery.offsets().size() - 1);
        Duration lastLateness = recovery.latenessBounds().get(recovery.latenessBounds().size() - 1);
        return recovery.window().plus(lastOffset).plus(lastLateness).plus(reconcileInterval);
    }

    private static Producer<String, byte[]> buildProducer(String bootstrap) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new KafkaProducer<>(props);
    }

    /** Anchors the script's nominal timeline to the real test start and sleeps to each event's real send time. */
    private static void publishPaced(Producer<String, byte[]> producer, List<ScriptedEvent> events,
                                       Instant testStart, Health health) throws InterruptedException {
        if (events.isEmpty()) return;
        Instant nominalStart = events.get(0).occurredAt();
        int beat = 0;
        for (ScriptedEvent event : events) {
            Instant target = testStart.plus(Duration.between(nominalStart, event.occurredAt()));
            long waitMillis = Duration.between(Instant.now(), target).toMillis();
            if (waitMillis > 0) Thread.sleep(waitMillis);

            CartEvent cartEvent = toCartEvent(event, Instant.now());
            producer.send(new ProducerRecord<>(Topics.CART_EVENTS, event.cartId(), JsonCodec.encode(cartEvent)));
            if (++beat % 200 == 0) health.beat("loadgen");
        }
        producer.flush();
    }

    private static CartEvent toCartEvent(ScriptedEvent event, Instant realOccurredAt) {
        return switch (event.type()) {
            case EDIT -> new CartEvent.CartEdited(event.cartId(), event.shopperKey(), event.version(), realOccurredAt,
                IntStream.range(0, event.itemCount())
                    .mapToObj(i -> new CartItem("SKU-" + i, "Item " + i, 1, 999))
                    .toList(),
                null);
            case RESUME -> new CartEvent.CartResumed(event.cartId(), event.shopperKey(), event.version(), realOccurredAt);
            case PURCHASE -> new CartEvent.CartPurchased(event.cartId(), event.shopperKey(), event.version(), realOccurredAt);
        };
    }
}
```

Now add the two Kafka helper classes `LoadgenRole` uses.

Create `src/main/java/com/quince/cartrecovery/loadgen/LagSampler.java`:

```java
package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.app.Health;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Samples consumer group lag every 5 s via AdminClient; tracks the max and the last (ending) lag per group. */
final class LagSampler {
    private final AdminClient admin;
    private final List<String> groups;
    private final Health health;
    private final Map<String, Long> maxLag = new HashMap<>();
    private final Map<String, Long> endingLag = new HashMap<>();
    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);

    LagSampler(String bootstrap, List<String> groups, Health health) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        this.admin = AdminClient.create(props);
        this.groups = groups;
        this.health = health;
        for (String g : groups) { maxLag.put(g, 0L); endingLag.put(g, 0L); }
    }

    void start() {
        scheduler.scheduleAtFixedRate(this::sampleOnce, 0, 5, TimeUnit.SECONDS);
    }

    void stop() {
        scheduler.shutdownNow();
        admin.close();
    }

    private void sampleOnce() {
        try {
            for (String group : groups) {
                long lag = lagFor(group);
                endingLag.put(group, lag);
                maxLag.merge(group, lag, Math::max);
            }
            health.beat("loadgen.lag-sampler");
        } catch (Exception e) {
            System.err.println("lag sample failed: " + e.getMessage());
        }
    }

    private long lagFor(String group) throws InterruptedException, ExecutionException {
        Map<TopicPartition, org.apache.kafka.clients.admin.OffsetAndMetadata> committed =
            admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
        if (committed.isEmpty()) return 0L;

        Map<TopicPartition, OffsetSpec> latestSpecs = new HashMap<>();
        for (TopicPartition tp : committed.keySet()) latestSpecs.put(tp, OffsetSpec.latest());
        ListOffsetsResult ends = admin.listOffsets(latestSpecs);

        long total = 0L;
        for (TopicPartition tp : committed.keySet()) {
            long end = ends.partitionResult(tp).get().offset();
            long pos = committed.get(tp).offset();
            total += Math.max(0, end - pos);
        }
        return total;
    }

    Map<String, Long> maxLagByGroup() { return Map.copyOf(maxLag); }
    Map<String, Long> endingLagByGroup() { return Map.copyOf(endingLag); }

    /** The group with the largest observed max lag, or "none" if every group stayed caught up. */
    String bottleneckStage() {
        return maxLag.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .filter(e -> e.getValue() > 100)
            .map(Map.Entry::getKey)
            .orElse("none — every group stayed within 100 records of caught up");
    }
}
```

Create `src/main/java/com/quince/cartrecovery/loadgen/OutcomeCollector.java`:

```java
package com.quince.cartrecovery.loadgen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.model.Lane;
import com.quince.cartrecovery.model.LedgerKey;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reads {@code sink-sends} and {@code reminder-outcomes} from the beginning on a throwaway
 * consumer group, keeping only rows whose {@code cartId} carries this run's prefix. Send latency and
 * lane come from the loadgen's own script (controller ruling R11): the ledger key gives cartId, version
 * and offsetIndex; the script gives that version's lastActivityAt.
 */
final class OutcomeCollector implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final String bootstrap;
    private final String runPrefix;
    private final Map<String, Instant> lastActivityByCartVersion;
    private final List<Duration> offsets;
    private final int fastOffsets;
    private final List<SinkSend> sends = new CopyOnWriteArrayList<>();
    private final List<OutcomeRow> outcomes = new CopyOnWriteArrayList<>();
    private final List<LatencySample> fastLatencies = new CopyOnWriteArrayList<>();
    private final List<LatencySample> slowLatencies = new CopyOnWriteArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread pollThread;

    OutcomeCollector(String bootstrap, String runPrefix, Map<String, Instant> lastActivityByCartVersion,
                     List<Duration> offsets, int fastOffsets) {
        this.bootstrap = bootstrap;
        this.runPrefix = runPrefix;
        this.lastActivityByCartVersion = Map.copyOf(lastActivityByCartVersion);
        this.offsets = List.copyOf(offsets);
        this.fastOffsets = fastOffsets;
    }

    void start() {
        running.set(true);
        pollThread = new Thread(this::pollLoop, "loadgen-outcome-collector");
        pollThread.start();
    }

    void stop() throws InterruptedException {
        running.set(false);
        pollThread.join(Duration.ofSeconds(10).toMillis());
    }

    private void pollLoop() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "loadgen-observer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(Topics.SINK_SENDS, Topics.OUTCOMES));
            while (running.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (record.key() == null || !record.key().startsWith(runPrefix)) continue;
                    try {
                        JsonNode node = JSON.readTree(record.value());
                        if (record.topic().equals(Topics.SINK_SENDS)) handleSinkSend(node);
                        else handleOutcome(node);
                    } catch (Exception e) {
                        System.err.println("skipping unparsable record on " + record.topic() + ": " + e.getMessage());
                    }
                }
            }
        }
    }

    private void handleSinkSend(JsonNode node) {
        SinkSend send = new SinkSend(
            node.get("key").asText(),
            node.get("cartId").asText(),
            Instant.ofEpochMilli(node.get("at").asLong()),
            node.get("hasFirstName").asBoolean(),
            node.get("itemCount").asInt());
        sends.add(send);
    }

    private void handleOutcome(JsonNode node) {
        JsonNode keyNode = node.get("key");
        OutcomeRow row = new OutcomeRow(
            keyNode == null || keyNode.isNull() ? null : keyNode.asText(),
            node.get("cartId").asText(),
            node.get("version").asLong(),
            node.get("arm").asText(),
            node.get("kind").asText(),
            Instant.ofEpochMilli(node.get("at").asLong()),
            node.get("attempts").asInt());
        outcomes.add(row);

        if ("SENT".equals(row.kind()) && row.key() != null) {
            LedgerKey k = LedgerKey.parse(row.key());
            Instant lastActivityAt = lastActivityByCartVersion.get(k.cartId() + ":" + k.version());
            if (lastActivityAt == null || k.offsetIndex() >= offsets.size()) return;   // not scripted by this run
            Instant scheduledFor = lastActivityAt.plus(offsets.get(k.offsetIndex()));
            LatencySample sample = new LatencySample(scheduledFor, Duration.between(scheduledFor, row.at()));
            (Lane.of(k.offsetIndex(), fastOffsets) == Lane.FAST ? fastLatencies : slowLatencies).add(sample);
        }
    }

    List<SinkSend> sinkSends() { return List.copyOf(sends); }
    List<OutcomeRow> outcomes() { return List.copyOf(outcomes); }
    List<LatencySample> fastLatencies() { return List.copyOf(fastLatencies); }
    List<LatencySample> slowLatencies() { return List.copyOf(slowLatencies); }

    @Override public void close() {
        running.set(false);
    }
}
```

Latency is measured from the scripted `scheduledFor` shifted onto the real timeline, so it includes any publisher pacing lag for that cart's last event (a few milliseconds when the loadgen keeps up); the payloads stay as spec §5.1 defines them (controller ruling R11).

- [ ] **Step 4: Run the pure-helper test**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests "com.quince.cartrecovery.loadgen.LoadgenRoleTest" --console=plain`
Expected: PASS, once `app.Role`/`app.InfraConfig`/`app.Health`, `core.Metrics`, and `infra.kafka.{JsonCodec,Topics}` exist on the branch (per this task's dependency on B3, C0a).

- [ ] **Step 5: Compile the whole main source set**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew compileJava --console=plain`
Expected: `BUILD SUCCESSFUL`. If it fails on a Kafka AdminClient/consumer API name, these are wiring calls against `kafka-clients:4.3.1` — fix the call, not the design; `LagSampler`/`OutcomeCollector` isolate every such call to one small file each.

- [ ] **Step 6: Check the compose `loadgen` service passes the load settings**

C0c's `docker-compose.yml` already defines `loadgen` under profile `load`, with `RATE` and `DURATION` on top of the shared environment; this task does not edit it.

Run: `RATE=50 DURATION=PT60S docker compose --profile load config loadgen | grep -E 'RATE|DURATION'`
Expected: lines `RATE: "50"` and `DURATION: PT60S` (among the shared `MAX_SEND_RATE` and `SEND_FAILURE_RATE` lines).

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/quince/cartrecovery/loadgen/LoadgenRole.java \
        src/main/java/com/quince/cartrecovery/loadgen/LagSampler.java \
        src/main/java/com/quince/cartrecovery/loadgen/OutcomeCollector.java \
        src/test/java/com/quince/cartrecovery/loadgen/LoadgenRoleTest.java
git commit -m "$(cat <<'EOF'
Add the loadgen role: paced producer, outcome collection, lag sampling

LoadgenRole scripts a workload with D0, publishes it to cart-events at
a rate paced against the real clock while preserving each event's
offset from the script's nominal timeline (so Expected's cancellation
math lines up with what the real pipeline decides), waits out the
drain period, and reads back sink-sends and reminder-outcomes filtered
to its own run prefix to write the load test report, with send latency
per lane derived from its own script.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task E1: `DESIGN.md` and `README.md` updates

**Files:**
- Modify: `DESIGN.md`
- Modify: `README.md`

**Interfaces:** none (documentation only). Every edit below is a literal find-and-replace against the current text of both files.

- [ ] **Step 1: `DESIGN.md` §3 — guardrail pause switch and the `ABANDONED` outcome**

Replace:
```
**Guardrails, the signs of harm.** Unsubscribe and spam-complaint rates per send, bounce rate, sends after purchase, duplicate sends, sends per shopper per day against the frequency cap, and holdout purchase rate not dropping. Any of these breaching its threshold pauses dispatch. Production design only: the in-memory pipeline counts sends, cancellations, skips, and dead letters, but has no thresholds or pause switch.
```
with:
```
**Guardrails, the signs of harm.** Unsubscribe and spam-complaint rates per send, bounce rate, sends after purchase, duplicate sends, sends per shopper per day against the frequency cap, and holdout purchase rate not dropping. Any of these breaching its threshold pauses dispatch. The infra build implements the switch itself: a `recovery-meta.paused` flag, checked by every dispatcher replica every 5 seconds, pauses both lanes within one poll cycle once set. The in-memory pipeline still only counts sends, cancellations, skips, and dead letters, with no threshold logic of its own. Every abandonment, treatment or holdout, is recorded once as an `ABANDONED` outcome tagged with its arm, so the control-group denominator (abandoned carts per arm) comes from the same event stream as the guardrail counts, not a separate query.
```

- [ ] **Step 2: `DESIGN.md` §4 — timer-first write order and field-scoped updates**

Replace:
```
| Abandonment Detector | Upserts the cart record and one version-tagged timer per cart. Ignores events at or below the stored version. Commits the offset after the state write. | Cart State Store, Timer Store |
```
with:
```
| Abandonment Detector | Writes the timer first, then a field-scoped conditional update to the cart record; ignores events at or below the stored version; commits the offset after both writes. | Cart State Store, Timer Store |
```

Then, immediately after the closing `` ``` `` of the `flowchart LR` mermaid block that follows the components table, insert a new paragraph:

```

Production writes the timer before the cart record, and only ever touches the fields an event actually changes: no read-modify-write. Timer-first means a crash between the two writes leaves at most a stray timer, which the reconciler repairs from the record; leaving the record first would let a crash hide an abandoned cart until the next sweep finds it by a wider scan. Field-scoped `UpdateItem`, conditioned on `version <` the incoming event's version, removes the lost-update race on `sequenceStarts` that a read-then-write update would have. In-memory keeps the simpler upsert-then-apply order (Section 11) because its single-threaded pipeline has no such race to remove; the timer store there is also just a map, so write order carries no crash-safety meaning.
```

- [ ] **Step 3: `DESIGN.md` §5 — fenced ledger claim, one lateness deadline, per-partition watermark gate**

Insert a new paragraph immediately after the numbered list of three checkpoints and before `## 6. Idempotency under at-least-once delivery`:

```

**Production infra adds two more checkpoints.** Before checkpoint 3's reload, the dispatcher checks a per-partition watermark — the newest event time each detector has fully processed and committed, published every poll loop — for the intent's recorded source partition; a lagging partition holds only its own reminders, pausing that slice of the dispatch consumer, so a purchase still sitting in Kafka cannot be missed by a read that runs ahead of it. And where the in-memory dispatcher re-validates lateness against whatever the caller passes at each of the three checkpoints, production stamps one deadline, `sendBy`, into the intent when it is scheduled, and checks it before taking a send token, after the claim, and immediately before the call — a late intent is dropped before it can spend any capacity, not just before it can send. The ledger write at checkpoint 2 also moves: production takes a fenced claim (a token good for one lease, conditioned on the ledger row's current status) at send time, for every attempt including retries, rather than once at scheduling time, so a lease holder that overruns its lease is fenced out rather than risking a double send.
```

- [ ] **Step 4: `DESIGN.md` §6 — monotonic timers**

Replace:
```
| Timers | Version tag compared on fire. A redelivered timer fails the ledger existence check. | Verifier 6; `ReminderSchedulerTest.staleVersionTimerIsDropped`, `duplicateReminderTimerWritesNothingTwice` |
```
with:
```
| Timers | The Redis upsert only writes if the new `(version, offsetIndex)` pair is greater than what is stored; equal data is a no-op that keeps the current lease. A redelivered or stale timer is dropped when it fires and its version no longer matches the record. | Verifier 6; `ReminderSchedulerTest.staleVersionTimerIsDropped`, `duplicateReminderTimerWritesNothingTwice`; production: timer store contract tests for the monotonic upsert |
```

- [ ] **Step 5: `DESIGN.md` §7 — jitter, breaker, poison handling, Redis-rebuildable rule and failover replay**

Replace:
```
| Transient send failure | Exponential backoff with jitter, bounded attempts. The cart is re-validated before each attempt, so a purchase during backoff stops the retry, and a retry that would land past the reminder's lateness bound is dropped and counted instead of sent. | Verifier 8; `DispatcherTest.transientFailureRetriesWithExponentialBackoffThenSucceedsOnce`, `purchaseDuringBackoffCancelsTheRetry`, `retryLandingAfterTheLatenessBoundIsDroppedNotSent`. Jitter is production design only: the in-memory dispatcher uses fixed doubling so fake-clock times are exact. |
```
with:
```
| Transient send failure | Exponential backoff with full jitter, bounded attempts. The cart is re-validated before each attempt, so a purchase during backoff stops the retry, and a retry that would land past the reminder's lateness bound is dropped and counted instead of sent. | Verifier 8; `DispatcherTest.transientFailureRetriesWithExponentialBackoffThenSucceedsOnce`, `purchaseDuringBackoffCancelsTheRetry`, `retryLandingAfterTheLatenessBoundIsDroppedNotSent`. Production backs off `RETRY_BASE × 2^(attempts−1)` with full jitter (uniform up to that bound), so replicas retrying together don't resonate; the in-memory dispatcher keeps fixed doubling because the fake-clock verifier needs exact times. |
```

Replace:
```
| Detector down | Kafka retains events for seven days. The consumer resumes from its committed offset. Reprocessing is idempotent by version. | Idempotent reprocessing: Verifier 5, 7. Retention and offset commits are production design only (no broker in memory). |
| Timer store loss | The store is derived data. The reconciliation sweeper reinserts missing timers from records and the ledger, starting at the first offset still within its lateness bound, so a restart after an outage does not re-run reminders already skipped. Full rebuild scans the state store or replays the stream. Timers that fire past their lateness bound are skipped and counted rather than sent stale. | Verifier 10, 10b, 11; `PipelineTest.restartWhileActiveRebuildsTheCheckTimer`, `restartWhileAbandonedResumesFromTheLedger`, `restartAfterAllRemindersSchedulesNothing`, `restartAfterEveryReminderWasSkippedRebuildsNothing`, `restartAfterPartialOutageSkipsToTheNextOnTimeOffset`. Rebuild by stream replay is production design only. |
| Gateway down | Circuit breaker pauses dispatch, the outbox backs up, drain resumes within lateness bounds. | Production design only: the in-memory dispatcher has no breaker. The backlog behaviour it relies on, retries and dropping past the bound, is Verifier 8 and 9b. |
| Poison event | Schema validation, event dead letter, alert. | Production design only: in-memory events are typed records, so there is no schema to fail. |
```
with:
```
| Detector down | Kafka retains events for seven days. The consumer resumes from its committed offset. Reprocessing is idempotent by version. | Idempotent reprocessing: Verifier 5, 7. Retention and offset commits are production design only (no broker in memory). |
| Timer store loss | Redis holds only rebuildable state — timers and the watermark, never a system of record. The reconciler's periodic sweep reinserts any timer missing from Redis, from the cart record and the ledger's highest sent offset, skipping offsets already past their lateness bound. On top of the sweep, production detects a Redis restart or failover (comparing Redis's `run_id` and replication role against the values it last stored in `recovery-meta`) and replays the last 60 seconds of `cart-events` into timer upserts — the only kind of write a restart or failover can actually lose, since a lost scheduler write is redelivered by its own lease and a lost remove or reconciler write is harmless. | Verifier 10, 10b, 11; `PipelineTest.restartWhileActiveRebuildsTheCheckTimer`, `restartWhileAbandonedResumesFromTheLedger`, `restartAfterAllRemindersSchedulesNothing`, `restartAfterEveryReminderWasSkippedRebuildsNothing`, `restartAfterPartialOutageSkipsToTheNextOnTimeOffset`; production: the Redis-restart contract test and e2e 6 (`FLUSHALL`) |
| Gateway down | A circuit breaker wraps the notification sink: over the last 100 send attempts, a transient-failure ratio above 50% opens it for 30 seconds, pausing both lane consumers and the retry loop; one probe send on a half-open breaker decides whether to close it again. | The backlog behaviour behind a paused or open breaker resolves the same way regardless of cause — retries within budget, drop past the lateness bound: Verifier 8 and 9b. In-memory has no breaker of its own. |
| Poison event | A record that fails to deserialize, or an intent whose offset no longer exists after the offset count shrank, is classified deterministic: consumers send it to the topic's dead-letter topic and commit past it; the scheduler acks a poison timer and counts `timers.poison` instead of leaving it for lease redelivery. | In-memory events are typed records, so there is no schema to fail — this row has no in-memory analogue. |
```

- [ ] **Step 6: `DESIGN.md` §8 — capacity from spec §5.5**

Replace:
```
| Figure | Baseline | Spike (2.5x for 1 hour) |
|---|---|---|
| Cart events per second | 5,000 | 12,500 |
| Events in the spike hour | | 45 million |
| Stream throughput | 5 MB/s | 12.5 MB/s |
| State and timer upserts per second, each | 5,000 | 12,500 |
| Distinct carts per second | 500 | 1,250 |
| Carts becoming abandoned per second | 350 | 875 |
| Reminder sends per second | about 1,000 | about 2,600 |

Sixty-four partitions keep each under 200 events per second at spike. Redis and DynamoDB absorb 12.5k writes per second each, which a single relational primary would handle only with careful tuning. Abandoned carts keep a timer until their 24 hour reminder, so the in-flight timer set is about 350 per second times 86,400 seconds, roughly 30 million members and about 3 GB, or about 50 MB per shard across 64 shards, which is comfortable and lets sweepers run in parallel. The reconciliation sweep reads only carts that still have a next step: a sparse index of open carts whose last offset has not passed, equivalently records moved to a terminal status or given a TTL after their last offset, so the scan stays bounded by the in-flight set rather than every cart ever seen. The 24 hour reminders for a spike hour land as a burst a day later, so the dispatcher has a rate limiter and a backlog.
```
with:
```
| Figure | Baseline | Spike (2.5x for 1 hour) |
|---|---|---|
| Cart events per second | 5,000 | 12,500 |
| Events in the spike hour | | 45 million |
| Stream throughput | 5 MB/s | 12.5 MB/s |
| State and timer upserts per second, each | 5,000 | 12,500 |
| Distinct carts per second | 500 | 1,250 |
| Carts becoming abandoned per second | 350 | 875 |
| Reminder sends per second | about 945 | about 2,400 (next-day burst) |

Sixty-four partitions keep each under 200 events per second at spike. Abandoned carts keep a timer until their 24 hour reminder, so the in-flight timer set is about 350 per second times 86,400 seconds, roughly 30 million members and about 3 GB, or about 50 MB per shard across 64 shards, which is comfortable and lets sweepers run in parallel. The reconciliation sweep reads only carts that still have a next step, so the scan stays bounded by the in-flight set rather than every cart ever seen. The 24 hour reminders for a spike hour land as a burst a day later, so the dispatcher has a rate limiter and a backlog.

### Store load (production infra, at the baseline 5k events/s)

| Store | Load | Figure |
|---|---|---|
| `carts` base writes | 1 `UpdateItem` per event, plus 2 per abandonment | about 10k WCU/s baseline, about 25k at spike |
| `carts` GSI writes | only when the open-cart index entry changes | about 1.5k to 2.5k WCU/s baseline; provisioned with headroom, since a throttled GSI back-pressures the base table |
| `carts` reads | consistent `GetItem` per timer fire and per dispatch attempt | about 3k to 6k RCU/s |
| `send-ledger` | claim + GSI insert + finish + GSI delete per reminder, plus failed conditional claims from competing retry pollers | about 3.8k WCU/s baseline, about 9.6k in the next-day burst |
| Redis | one script per event plus claims, acks, and watermark reads and writes | about 8k to 18k ops/s across the cluster; about 5 to 6 GB for about 28 to 32M timers |
| Reconciler sweep | the sparse open-cart index holds in-flight carts only | about 800 RCU/s at the default 5 minute interval; about 30 to 80 s per sweep |
| Failover replay | re-reads the last 60 s of `cart-events`, only after a Redis restart or failover | at most about 750k Redis upserts, no DynamoDB reads |

On-demand capacity absorbs about 2x the previous peak instantly; the 2.5x spike needs warm throughput or provisioned capacity set in advance.
```

- [ ] **Step 7: `DESIGN.md` §9 — fast and slow lanes on a shared budget**

Replace:
```
Delay, never drop events, never reject upstream. The pipeline is off the checkout path, so cart events are always accepted. Kafka absorbs the burst, detectors autoscale on consumer lag, sweepers pull bounded batches, and the dispatcher rate limits to the gateway. Two priority lanes keep 30 minute reminders ahead of 24 hour ones, because a fresh reminder recovers more revenue than a stale one; this is production design only, the in-memory dispatcher drains in due order. If the backlog grows past the lateness bounds, reminders are skipped and counted rather than sent late, so an extreme spike degrades into fewer reminders instead of a flood of stale ones.
```
with:
```
Delay, never drop events, never reject upstream. The pipeline is off the checkout path, so cart events are always accepted. Kafka absorbs the burst, detectors autoscale on consumer lag, sweepers pull bounded batches, and the dispatcher rate limits to the gateway. Two Kafka topics carry the priority lanes, split by `offsetIndex < FAST_OFFSETS` (by default the 30 minute and 1 hour offsets are fast, the 24 hour offset is slow), consumed by separate dispatcher threads sharing one token bucket per replica: the fast lane takes any available token, the slow lane only while more than `FAST_RESERVE` (default 30%) of the bucket remains — so the next-day burst can never strand capacity the fast lane could use, and can never starve it either. In-memory still drains in due order: there is only one process and one thread, so lane priority has nothing to arbitrate. If the backlog grows past the lateness bounds, reminders are skipped and counted rather than sent late, so an extreme spike degrades into fewer reminders instead of a flood of stale ones.
```

- [ ] **Step 8: `DESIGN.md` §10 — first-name personalization at send time**

Replace:
```
**Personalization.** The intent carries first name and item snapshot. The dispatcher refreshes the snapshot from the Cart Service at send time, so prices and stock are current, and falls back to the stored snapshot if that call fails.
```
with:
```
**Personalization.** The intent carries no items or name, only the identifiers needed to gate and dedupe. The dispatcher builds the message from its consistent cart read, taken right before sending — the same read that re-validates the cart is still abandoned — so first name and the item snapshot are always as current as the cart record, with no separate refresh call and nothing to fall back to if one had failed.
```

- [ ] **Step 9: `DESIGN.md` §11 — infra adapter column, how to run it, per-shopper capping, extended citations**

Replace:
```
| Interface | In-memory adapter | Production backing |
|---|---|---|
| `Clock` | `FakeClock`, moves forward only | System clock |
| `CartStateStore` | `InMemoryCartStateStore`, conditional put on version | DynamoDB, conditional write |
| `TimerStore` | `PriorityQueueTimerStore`, upsert by cart id | Redis sorted set per shard, `ZADD` replaces |
| `SendLedger` | `InMemorySendLedger` | DynamoDB conditional put on the key |
| `Outbox` | `InMemoryOutbox` | Same table as the ledger, written in one transaction |
| `NotificationSink` | `RecordingNotificationSink`, never sends, scriptable failures | Notification gateway client |
| `DeadLetterQueue` | `InMemoryDeadLetterQueue` | Kafka topic plus replay tool |
```
with:
```
| Interface | In-memory adapter | Infra adapter |
|---|---|---|
| `Clock` | `FakeClock`, moves forward only | `SystemClock` |
| `CartStateStore` | `InMemoryCartStateStore`, conditional put on version | DynamoDB `carts` table, field-scoped conditional `UpdateItem`, sparse GSI `open-by-shard` for the reconciler |
| `TimerStore` | `PriorityQueueTimerStore`, upsert by cart id | Redis sorted set per shard, Lua scripts for the monotonic upsert, claim, release, and ack |
| `Watermark` | the clock, or the buffered detector's position | Redis hash, one entry per partition, generation-fenced end-offset snapshots |
| `IntentPublisher` | in-memory queue drained by `Pipeline` | Kafka, two topics split by lane (`reminder-intents-fast` / `reminder-intents-slow`) |
| `SendLedger` | `InMemorySendLedger` | DynamoDB `send-ledger` table, fenced conditional claim, sparse GSI `retrying-by-shard` for the retry loop |
| `NotificationSink` | `RecordingNotificationSink`, never sends, scriptable failures | recording sink producing to Kafka `sink-sends`, with an injectable transient-failure rate for load testing |
| `OutcomeRecorder` | list | Kafka `reminder-outcomes` |
| `DeadLetterQueue` | `InMemoryDeadLetterQueue` | Kafka `reminder-dlq`, replayed by the `replay` role |

The `Outbox` port and `InMemoryOutbox` no longer exist: dedupe moved from a scheduling-time ledger-plus-outbox transaction to a fenced claim taken by the dispatcher at send time (§5, §6).

**Running the infra build.** `docker compose --env-file demo.env up -d --build` starts Kafka, Redis, and DynamoDB Local plus every role at 2 replicas (1 for the reconciler), against `demo.env`'s compressed timings; `docker compose --env-file demo.env --profile load run --rm -e RATE=50 -e DURATION=PT60S loadgen` drives a load test and writes a report under `build/reports/load/`. Every invariant this section's in-memory tests demonstrate is proven again on real infra: contract tests prove adapter parity directly (the monotonic upsert, conditional remove, claim and lease expiry, watermark generation fencing, ledger claim and takeover), and the infra end-to-end tests — roles running as in-process threads against real containers — prove wiring and failure semantics: idle partitions do not pause the gate, a mid-sequence purchase stops the sequence, duplicate and out-of-order events send exactly once, two schedulers and two dispatchers on shared shards send no duplicates across 200 carts, a partial failure rate dead-letters and replays cleanly, a Redis `FLUSHALL` pauses sending and self-heals, and a stopped detector holds only its own partition's carts. A future per-shopper send cap (rather than per-cart) would add a counter item keyed by shopper, incremented by the scheduler alongside `sequenceStarts` — not a change to any key shown above.
```

- [ ] **Step 10: `DESIGN.md` §13 — gateway idempotency-key duration**

Replace:
```
- Are discounts ever included in reminders, which would add a margin guardrail?
```
with:
```
- Are discounts ever included in reminders, which would add a margin guardrail?
- How long does the notification gateway honour an idempotency key? The design's two at-most-once exceptions (§3) both assume it is honoured for at least the longest lateness bound; today that assumption is stood in for by the recording sink and has never been checked against a real gateway.
```

- [ ] **Step 11: `README.md` — the infra runbook**

Replace:
```
## Layout
```
with:
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

- [ ] **Step 12: Commit**

```bash
git add DESIGN.md README.md
git commit -m "$(cat <<'EOF'
Document the production infra build in DESIGN.md and README.md

DESIGN.md sections 3-11 and 13 now describe the fenced ledger claim,
the single sendBy deadline, the per-partition watermark gate,
timer-first field-scoped writes, the Redis-rebuildable rule and
failover replay, the shared fast/slow send budget, the circuit
breaker, jitter, and poison handling, send-time personalization, the
infra capacity figures, and the infra adapter column; section 13 adds
the open question about gateway idempotency-key duration the design
depends on. README.md gains the Docker runbook from spec 7.5.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task E2: Run the stack, the drills, and the load test; commit the report

**Files:**
- Create: `docs/load-reports/<timestamp>.md` (a copy of the report `loadgen` writes to `build/reports/load/`)

**Interfaces:** none — this task executes the system the other threads built; it changes no source.

Run every step from the repository root, with Docker Desktop running and at least 6 GB of memory allocated to it.

- [ ] **Step 1: Bring the stack up**

Run: `DYNAMO_STORAGE=-inMemory docker compose --env-file demo.env up -d --build`
Expected: every service reports healthy in `docker compose ps` within a couple of minutes; `docker compose logs -f init` shows topics, tables, indexes, and the `epoch` sentinel created and the container exiting 0.

- [ ] **Step 2: Run the visible demo**

Run: `docker compose --env-file demo.env --profile load run --rm -e RATE=50 -e DURATION=PT60S loadgen`
Expected: the console prints a load test report; `docker compose logs dispatcher` shows sends within the demo's compressed 30s/60s/120s offsets.

- [ ] **Step 3: Run each drill from spec §7.5 / the README and record the actual outcome next to the expected one**

| Drill | Expected | Actually observed (fill in honestly) |
|---|---|---|
| Kill a scheduler mid-run (`docker compose kill -s SIGKILL scheduler` then let compose restart it, or `docker compose stop scheduler` on one replica if scaled) | Its leased timers are claimed by the surviving replica once the lease (90 s) expires; no reminder is lost | |
| `docker compose exec redis redis-cli FLUSHALL` | Dispatchers pause within about 5 s (watermark staleness), the reconciler rebuilds timers on the missing `epoch`, sending resumes with no duplicates | |
| `docker compose stop detector` (one replica) | Only that replica's partitions pause, for the rebalance; the rest of the traffic keeps sending | |
| `docker compose stop detector` (all replicas) | All sending pauses — every watermark goes stale | |
| `docker compose stop dispatcher`, wait past a reminder's lateness bound, `docker compose start dispatcher` | The backlog is skipped at the pre-check (`dispatch.skipped_late_precheck`), not sent late | |
| `SEND_FAILURE_RATE=0.3 docker compose --env-file demo.env up -d dispatcher`, then run the demo again | Some sends succeed after a retry, some dead-letter; `docker compose run --rm dispatcher --role=replay` sends each dead key exactly once | |
| Flip `recovery-meta.paused` to `true` (DynamoDB Local admin/CLI `UpdateItem`), then back to `false` | Sending stops within 5 s of the flip, resumes within 5 s of the reset | |

Expected overall: every row's actual outcome matches its expected outcome. Any mismatch is a bug to fix (in the relevant thread's code, not in this document) before continuing to Step 4.

- [ ] **Step 4: Run the load test at the target rate**

Run: `docker compose --env-file demo.env --profile load run --rm -e RATE=5000 -e DURATION=PT5M loadgen`
Expected: `BUILD SUCCESSFUL` is not applicable here (this is a run, not a build) — expected is that the command completes and prints a report. If the laptop cannot sustain 5000 events/s (rising, unrecovered consumer lag; the achieved rate trailing the target by more than 20%), stop it and re-run at a lower rate:

Run: `docker compose --env-file demo.env --profile load run --rm -e RATE=1000 -e DURATION=PT5M loadgen`
Expected: a report where the achieved rate is within a small margin of the target, and consumer lag returns to near zero by the end of the drain wait.

- [ ] **Step 5: Copy the generated report into the repo**

Run:
```bash
mkdir -p docs/load-reports
cp build/reports/load/*.md docs/load-reports/
```
Expected: `docs/load-reports/<timestamp>.md` now contains the report from whichever run (Step 4's 5000/s run, or the fallback lower rate) is being recorded.

- [ ] **Step 6: Record honestly — checklist**

Before committing, confirm the report (or an added note above the commit) states, without softening:

- The **bottleneck stage** the report named (or, if the loadgen's own heuristic named "none" while lag visibly built up somewhere in the logs, override it by hand and say which stage and why).
- The **achieved rate** versus the target, and whether a lower rate had to be used because the laptop (which also runs every infra container and every role replica) could not sustain the target — say so plainly rather than only reporting the lower number.
- **Duplicate sends** and **post-purchase sends** — both must be exactly 0; if either is not, this is a correctness bug in an earlier thread, not a load-test footnote, and must be fixed (with a new test pinning the failure) before this task is considered done.
- **Unexplained missing**, both as a count and as the percentage of expected sends, and whether it is under the spec's 0.1% target.
- Machine **cores and memory**, with the note that the loadgen process and every container share this one machine — the numbers are not an isolated benchmark.

- [ ] **Step 7: Commit the report**

```bash
git add docs/load-reports
git commit -m "$(cat <<'EOF'
Record a load test run against the infra build

Ran the demo, every runbook drill, and a load test (rate and any
fallback noted in the committed report); duplicate and post-purchase
sends were both 0, and the bottleneck stage and unexplained-missing
figure are reported as observed, not adjusted.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 8: Tear down**

Run: `docker compose --profile load down -v`
Expected: containers and their named volumes (`redis-data`, `dynamodb-data`) are removed.
