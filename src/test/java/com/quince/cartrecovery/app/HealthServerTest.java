package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.core.Metrics;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class HealthServerTest {
    private final AtomicLong nanos = new AtomicLong();
    private final Health health = new Health(nanos::get);
    private final Metrics metrics = new Metrics();
    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void healthyOnlyWhileEveryRegisteredLoopBeatWithinMaxSilence() {
        assertTrue(health.healthy(Duration.ofSeconds(1)), "no loop registered yet");
        health.beat("poll");
        health.beat("retry");
        nanos.addAndGet(Duration.ofSeconds(2).toNanos());
        health.beat("poll");
        assertFalse(health.healthy(Duration.ofSeconds(1)));
        assertEquals(List.of("retry"), health.stale(Duration.ofSeconds(1)));
        assertTrue(health.healthy(Duration.ofSeconds(3)));
    }

    @Test
    void servesHealthReadyAndMetrics() throws Exception {
        int port = freePort();
        health.beat("poll");
        health.setReady("breaker", "closed");
        health.setReady("breaker", "open");
        metrics.increment("dispatch.sent");
        metrics.increment("dispatch.sent");
        try (HealthServer ignored = new HealthServer(port, health, metrics, Duration.ofSeconds(1))) {
            HttpResponse<String> ok = get(port, "/health");
            assertEquals(200, ok.statusCode());
            assertEquals("ok\n", ok.body());

            nanos.addAndGet(Duration.ofSeconds(2).toNanos());
            HttpResponse<String> stale = get(port, "/health");
            assertEquals(503, stale.statusCode());
            assertEquals("stale: poll\n", stale.body());

            HttpResponse<String> ready = get(port, "/ready");
            assertEquals(200, ready.statusCode());
            assertEquals("breaker: open\n", ready.body());

            HttpResponse<String> m = get(port, "/metrics");
            assertEquals(200, m.statusCode());
            assertTrue(m.body().contains("dispatch.sent 2\n"), m.body());
        }
    }

    private HttpResponse<String> get(int port, String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).build(),
            HttpResponse.BodyHandlers.ofString());
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
