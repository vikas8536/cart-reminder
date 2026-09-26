package com.quince.cartrecovery;

import com.quince.cartrecovery.app.ConfigException;
import com.quince.cartrecovery.app.Health;
import com.quince.cartrecovery.app.HealthServer;
import com.quince.cartrecovery.app.InfraConfig;
import com.quince.cartrecovery.app.Role;
import com.quince.cartrecovery.app.RoleRegistry;
import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.inmemory.RecordingNotificationSink;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.RecoveryConfig;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Plays a scripted scenario through the pipeline on a fake clock and prints which triggers fired, or runs one infra role. */
public final class Main {
    private static final Instant T0 = Instant.parse("2026-01-01T09:00:00Z");
    private static final List<CartItem> ITEMS = List.of(
        new CartItem("SKU-1", "Linen Shirt", 1, 4990),
        new CartItem("SKU-2", "Cashmere Sweater", 1, 9900));

    // Deliberately a fixed upper bound over the spec's "3x poll interval" (spec §7.4, controller ruling F9):
    // a GC pause or a slow dependency call must never fail /health and cause the container to be restarted.
    static final Duration MAX_SILENCE = Duration.ofSeconds(15);
    // How often every role process logs its metrics (spec §7.4(a), controller ruling F5).
    static final Duration METRICS_LOG_INTERVAL = Duration.ofSeconds(10);

    public static void main(String[] args) {
        int code = run(args, System.getenv());
        if (code != 0 || option(args, "--role") != null) System.exit(code);
    }

    /**
     * --mode=inmemory (default): the fake-clock demo. --role=<name>: one infra role until SIGTERM.
     * --role=healthcheck: GET localhost /health, exit 0 when healthy (compose healthcheck without curl).
     */
    static int run(String[] args, Map<String, String> env) {
        String role = option(args, "--role");
        String mode = option(args, "--mode");
        if (role == null) {
            if (mode == null || mode.equals("inmemory")) {
                runDemo();
                return 0;
            }
            System.err.println("unknown mode: " + mode + " (expected --mode=inmemory or --role=<name>)");
            return 2;
        }
        if (role.equals("healthcheck")) return healthcheck(env);
        Optional<Role> found = RoleRegistry.create(role);
        if (found.isEmpty()) {
            System.err.println("unknown role: " + role + " (expected one of " + RoleRegistry.NAMES + " or healthcheck)");
            return 2;
        }
        InfraConfig config;
        try {
            config = InfraConfig.fromEnv(env);
        } catch (ConfigException e) {
            System.err.println("invalid configuration: " + e.getMessage());
            return 2;
        }
        return runRole(found.get(), config);
    }

    /** SIGTERM runs the shutdown hook, which interrupts this thread and waits up to 30 s for the role to return (master §1.5). */
    private static int runRole(Role role, InfraConfig config) {
        System.out.printf("role=%s config=%s%n", role.name(), config.hash());
        Health health = new Health();
        Metrics metrics = new Metrics();
        Thread roleThread = Thread.currentThread();
        CountDownLatch done = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            roleThread.interrupt();
            try {
                done.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }));
        Thread metricsLogger = startMetricsLogger(role.name(), metrics);
        try (HealthServer server = new HealthServer(config.healthPort(), health, metrics, MAX_SILENCE)) {
            role.run(config, health, metrics);
            return 0;
        } catch (InterruptedException e) {
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        } finally {
            done.countDown();
            metricsLogger.interrupt();
        }
    }

    /** Logs metrics.snapshot() every METRICS_LOG_INTERVAL until the role finishes (spec §7.4(a), controller ruling F5). */
    private static Thread startMetricsLogger(String roleName, Metrics metrics) {
        return Thread.ofPlatform().daemon().name("metrics-logger").start(() -> {
            while (true) {
                try {
                    Thread.sleep(METRICS_LOG_INTERVAL);
                } catch (InterruptedException e) {
                    return;
                }
                System.out.printf("role=%s metrics=%s%n", roleName, metrics.snapshot());
            }
        });
    }

    private static int healthcheck(Map<String, String> env) {
        try {
            int port = Integer.parseInt(env.getOrDefault("HEALTH_PORT", "8081"));   // InfraConfig's default
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
            HttpResponse<Void> response = client.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/health")).timeout(Duration.ofSeconds(2)).build(),
                HttpResponse.BodyHandlers.discarding());
            return response.statusCode() == 200 ? 0 : 1;
        } catch (Exception e) {
            return 1;
        }
    }

    private static String option(String[] args, String name) {
        for (String a : args) {
            if (a.startsWith(name + "=")) return a.substring(name.length() + 1);
        }
        return null;
    }

    static void runDemo() {
        Pipeline p = new Pipeline(RecoveryConfig.defaults(), T0, key -> Arm.TREATMENT);
        int[] seen = {0};

        System.out.println("virtual start " + T0);
        System.out.println();

        step(p, seen, "cart A edited at +0m, cart B edited at +0m, cart C edited at +0m", () -> {
            p.ingest(new CartEvent.CartEdited("A", "user-a", 1, T0, ITEMS));
            p.ingest(new CartEvent.CartEdited("B", "user-b", 1, T0, ITEMS));
            p.ingest(new CartEvent.CartEdited("C", "user-c", 1, T0, ITEMS));
        });
        step(p, seen, "cart B edited again at +20m (clock reset)", () ->
            p.ingest(new CartEvent.CartEdited("B", "user-b", 2, T0.plus(Duration.ofMinutes(20)), ITEMS)));
        step(p, seen, "cart C purchased at +25m (cancels)", () ->
            p.ingest(new CartEvent.CartPurchased("C", "user-c", 2, T0.plus(Duration.ofMinutes(25)))));
        step(p, seen, "advance to +30m", () -> p.advanceTo(T0.plus(Duration.ofMinutes(30))));
        step(p, seen, "duplicate delivery of cart A's first edit (ignored)", () ->
            p.ingest(new CartEvent.CartEdited("A", "user-a", 1, T0, ITEMS)));
        step(p, seen, "advance to +1h", () -> p.advanceTo(T0.plus(Duration.ofHours(1))));
        step(p, seen, "restart: timer index lost and rebuilt from cart records", p::restart);
        step(p, seen, "cart A purchased at +1h10m (stops the 24h reminder)", () ->
            p.ingest(new CartEvent.CartPurchased("A", "user-a", 2, T0.plus(Duration.ofMinutes(70)))));
        step(p, seen, "advance to +25h", () -> p.advanceTo(T0.plus(Duration.ofHours(25))));

        System.out.println("metrics");
        for (Map.Entry<String, Long> e : p.metrics().snapshot().entrySet()) {
            System.out.printf("  %-28s %d%n", e.getKey(), e.getValue());
        }
    }

    private static void step(Pipeline p, int[] seen, String label, Runnable action) {
        System.out.println("== " + label);
        action.run();
        List<RecordingNotificationSink.Sent> sent = p.sink().sent();
        for (int i = seen[0]; i < sent.size(); i++) {
            RecordingNotificationSink.Sent s = sent.get(i);
            System.out.printf("   FIRED  %s  cart=%s offset=%d key=%s%n",
                s.sentAt(), s.message().cartId(), LedgerKey.parse(s.message().key()).offsetIndex(), s.message().key());
        }
        if (seen[0] == sent.size()) System.out.println("   (no sends)");
        seen[0] = sent.size();
        System.out.println("   clock now " + p.clock().now() + ", pending timers " + p.timers().size());
        System.out.println();
    }
}
