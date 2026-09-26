package com.quince.cartrecovery;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.app.Health;
import com.quince.cartrecovery.app.HealthServer;
import com.quince.cartrecovery.app.InfraConfig;
import com.quince.cartrecovery.app.Role;
import com.quince.cartrecovery.app.RoleRegistry;
import com.quince.cartrecovery.core.Metrics;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MainTest {
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();
    private PrintStream oldOut;
    private PrintStream oldErr;

    @BeforeEach
    void capture() {
        oldOut = System.out;
        oldErr = System.err;
        System.setOut(new PrintStream(out, true, UTF_8));
        System.setErr(new PrintStream(err, true, UTF_8));
    }

    @AfterEach
    void restore() {
        System.setOut(oldOut);
        System.setErr(oldErr);
    }

    @Test
    void defaultModeRunsTheInMemoryDemo() {
        assertEquals(0, Main.run(new String[0], Map.of()));
        assertFalse(out.toString(UTF_8).isBlank());
    }

    @Test
    void explicitInMemoryModeRunsTheDemo() {
        assertEquals(0, Main.run(new String[] {"--mode=inmemory"}, Map.of()));
    }

    @Test
    void unknownModeExits2() {
        assertEquals(2, Main.run(new String[] {"--mode=cloud"}, Map.of()));
        assertTrue(err.toString(UTF_8).contains("unknown mode"));
    }

    @Test
    void unknownRoleExits2WithoutReadingConfig() {
        assertEquals(2, Main.run(new String[] {"--role=nope"}, Map.of()));
        assertTrue(err.toString(UTF_8).contains("unknown role: nope"));
    }

    @Test
    void badConfigExits2WithOneLineNamingTheVariable() {
        Map<String, String> env = Map.of("KAFKA_BOOTSTRAP", "localhost:9092", "REDIS_URL", "redis://localhost:6379", "SHARDS", "abc");
        assertEquals(2, Main.run(new String[] {"--role=detector"}, env));
        List<String> lines = err.toString(UTF_8).lines().toList();
        assertEquals(1, lines.size(), err.toString(UTF_8));
        assertTrue(lines.get(0).contains("SHARDS"), lines.get(0));
    }

    @Test
    void healthcheckReportsTheServerState() throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }
        Map<String, String> env = Map.of("HEALTH_PORT", Integer.toString(port));
        try (HealthServer server = new HealthServer(port, new Health(), new Metrics(), Duration.ofSeconds(15))) {
            assertEquals(0, Main.run(new String[] {"--role=healthcheck"}, env));
        }
        assertEquals(1, Main.run(new String[] {"--role=healthcheck"}, env));
    }

    @Test
    void registryBuildsEveryRoleByName() {
        for (String name : RoleRegistry.NAMES) {
            assertEquals(name, RoleRegistry.create(name).orElseThrow().name());
        }
    }

    @Test
    void startupLogsTheLiteralConfigHash() throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }
        InfraConfig config = InfraConfig.fromEnv(Map.of("HEALTH_PORT", Integer.toString(port)));
        assertEquals(0, Main.runRole(new NoopRole(), config));
        assertTrue(out.toString(UTF_8).contains("config hash " + config.hash()), out.toString(UTF_8));
    }

    /**
     * Fix round 1, review finding: ReplayRole drives KafkaConsumer.poll directly on the role thread. Kafka wraps
     * an interrupted blocking call in its own unchecked org.apache.kafka.common.errors.InterruptException
     * (cause: the original InterruptedException, and it re-sets the thread's interrupt flag), not the raw
     * InterruptedException. A role that propagates this must still return cleanly, per the Role contract.
     */
    @Test
    void aKafkaInterruptExceptionDuringShutdownReturnsCleanlyLikeAnInterruptedException() throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }
        InfraConfig config = InfraConfig.fromEnv(Map.of("HEALTH_PORT", Integer.toString(port)));
        Role role = new Role() {
            @Override public String name() { return "fake-kafka-interrupt"; }
            @Override public void run(InfraConfig c, Health health, Metrics metrics) {
                throw new org.apache.kafka.common.errors.InterruptException(new InterruptedException());
            }
        };
        assertEquals(0, Main.runRole(role, config));
        assertTrue(err.toString(UTF_8).isBlank(), err.toString(UTF_8));
    }

    private static final class NoopRole implements Role {
        @Override public String name() { return "noop"; }
        @Override public void run(InfraConfig config, Health health, Metrics metrics) {}
    }
}
