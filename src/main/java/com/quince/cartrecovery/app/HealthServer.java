package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Metrics;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** /health, /ready and /metrics on the JDK HTTP server (spec §7.4). Plain text. */
public final class HealthServer implements AutoCloseable {
    private final HttpServer server;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public HealthServer(int port, Health health, Metrics metrics, Duration maxSilence) {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot bind health port " + port, e);
        }
        server.createContext("/health", ex -> {
            List<String> stale = health.stale(maxSilence);
            if (stale.isEmpty()) respond(ex, 200, "ok\n");
            else respond(ex, 503, "stale: " + String.join(",", stale) + "\n");
        });
        server.createContext("/ready", ex -> respond(ex, 200, lines(health.readiness(), ": ")));
        server.createContext("/metrics", ex -> respond(ex, 200, lines(metrics.snapshot(), " ")));
        server.setExecutor(executor);
        server.start();
    }

    @Override
    public void close() {
        server.stop(0);
        executor.close();
    }

    private static String lines(Map<String, ?> values, String separator) {
        StringBuilder sb = new StringBuilder();
        values.forEach((k, v) -> sb.append(k).append(separator).append(v).append('\n'));
        return sb.toString();
    }

    private static void respond(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }
}
