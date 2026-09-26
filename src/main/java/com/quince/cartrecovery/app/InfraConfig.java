package com.quince.cartrecovery.app;

import com.quince.cartrecovery.model.DispatchConfig;
import com.quince.cartrecovery.model.RecoveryConfig;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/** Effective runtime configuration, read once from the environment (spec §7.1). */
public record InfraConfig(RecoveryConfig recovery, DispatchConfig dispatch,
                          String kafkaBootstrap, String redisUrl, String dynamoEndpoint,
                          int shards, int partitions, int replicationFactor, int minInsyncReplicas,
                          double maxSendRate, double fastReserve, double sendFailureRate,
                          Duration reconcileInterval, Duration retryPoll, int maxInFlight, int healthPort) {

    /** Parses and validates every §7.1 variable; unset or blank values take the defaults. */
    public static InfraConfig fromEnv(Map<String, String> env) {
        Env e = new Env(env);

        Duration window = e.duration("WINDOW", "PT30M");
        List<Duration> offsets = e.durations("OFFSETS", "PT30M,PT1H,PT24H", false);
        List<Duration> bounds = e.durations("LATENESS_BOUNDS", "PT5M,PT5M,PT30M", true);
        if (bounds.size() != offsets.size())
            throw new ConfigException("LATENESS_BOUNDS: needs one bound per OFFSETS entry ("
                + offsets.size() + "), got " + bounds.size());
        int frequencyCap = e.integer("FREQUENCY_CAP", 3, 0);
        Duration frequencyWindow = e.duration("FREQUENCY_WINDOW", "P7D");
        int holdout = e.integer("HOLDOUT_PERCENT", 10, 0);
        if (holdout > 100) throw new ConfigException("HOLDOUT_PERCENT: must be at most 100, got " + holdout);
        int maxSendAttempts = e.integer("MAX_SEND_ATTEMPTS", 5, 1);
        Duration retryBase = e.duration("RETRY_BASE", "PT1M");
        RecoveryConfig recovery;
        try {
            recovery = new RecoveryConfig(window, offsets, bounds, frequencyCap, frequencyWindow,
                holdout, maxSendAttempts, retryBase);
        } catch (IllegalArgumentException ex) {
            // Every other RecoveryConfig rule is checked above; what remains is offset order against WINDOW.
            throw new ConfigException("OFFSETS: " + ex.getMessage());
        }

        int fastOffsets = e.integer("FAST_OFFSETS", 2, 0);
        Duration lease = e.duration("LEASE", "PT90S");
        Duration gatewayTimeout = e.duration("GATEWAY_TIMEOUT", "PT30S");
        Duration clockSkew = e.duration("CLOCK_SKEW", "PT5S");
        if (lease.compareTo(gatewayTimeout.multipliedBy(3)) < 0)
            throw new ConfigException("LEASE: must be at least 3 x GATEWAY_TIMEOUT (" + gatewayTimeout + "), got " + lease);
        DispatchConfig dispatch = new DispatchConfig(lease, gatewayTimeout, clockSkew, fastOffsets);

        int replicationFactor = e.integer("REPLICATION_FACTOR", 1, 1);
        int minInsync = e.integer("MIN_INSYNC_REPLICAS", 1, 1);
        if (minInsync > replicationFactor)
            throw new ConfigException("MIN_INSYNC_REPLICAS: must not exceed REPLICATION_FACTOR ("
                + replicationFactor + "), got " + minInsync);
        double maxSendRate = e.decimal("MAX_SEND_RATE", 1000);
        if (maxSendRate <= 0) throw new ConfigException("MAX_SEND_RATE: must be positive, got " + maxSendRate);
        double fastReserve = e.decimal("FAST_RESERVE", 0.3);
        if (fastReserve < 0 || fastReserve >= 1)
            throw new ConfigException("FAST_RESERVE: must be in [0, 1), got " + fastReserve);
        double sendFailureRate = e.decimal("SEND_FAILURE_RATE", 0);
        if (sendFailureRate < 0 || sendFailureRate > 1)
            throw new ConfigException("SEND_FAILURE_RATE: must be in [0, 1], got " + sendFailureRate);
        int healthPort = e.integer("HEALTH_PORT", 8081, 1);
        if (healthPort > 65535) throw new ConfigException("HEALTH_PORT: must be at most 65535, got " + healthPort);

        return new InfraConfig(recovery, dispatch,
            e.text("KAFKA_BOOTSTRAP", "localhost:9092"),
            e.text("REDIS_URL", "redis://localhost:6379"),
            e.text("DYNAMO_ENDPOINT", null),
            e.integer("SHARDS", 8, 1),
            e.integer("PARTITIONS", 8, 1),
            replicationFactor, minInsync,
            maxSendRate, fastReserve, sendFailureRate,
            e.duration("RECONCILE_INTERVAL", "PT5M"),
            e.duration("RETRY_POLL", "PT1S"),
            e.integer("MAX_IN_FLIGHT", 256, 1),
            healthPort);
    }

    /** First 12 hex chars of SHA-256 over the effective config; every role logs it at startup. */
    public String hash() {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 6);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private record Env(Map<String, String> vars) {
        String text(String name, String fallback) {
            String v = vars.get(name);
            return v == null || v.isBlank() ? fallback : v.trim();
        }

        Duration duration(String name, String fallback) {
            Duration d = parse(name, text(name, fallback));
            if (!d.isPositive()) throw new ConfigException(name + ": must be a positive duration, got " + d);
            return d;
        }

        List<Duration> durations(String name, String fallback, boolean zeroAllowed) {
            List<Duration> out = new ArrayList<>();
            for (String part : text(name, fallback).split(",", -1)) {
                Duration d = parse(name, part.trim());
                if (d.isNegative() || (!zeroAllowed && d.isZero()))
                    throw new ConfigException(name + ": " + (zeroAllowed ? "must not be negative" : "must be positive")
                        + ", got " + d);
                out.add(d);
            }
            return out;
        }

        int integer(String name, int fallback, int min) {
            String v = text(name, null);
            int n;
            if (v == null) {
                n = fallback;
            } else {
                try {
                    n = Integer.parseInt(v);
                } catch (NumberFormatException ex) {
                    throw new ConfigException(name + ": expected an integer, got '" + v + "'");
                }
            }
            if (n < min) throw new ConfigException(name + ": must be at least " + min + ", got " + n);
            return n;
        }

        double decimal(String name, double fallback) {
            String v = text(name, null);
            if (v == null) return fallback;
            try {
                double d = Double.parseDouble(v);
                if (Double.isFinite(d)) return d;
            } catch (NumberFormatException ignored) {
                // reported below
            }
            throw new ConfigException(name + ": expected a number, got '" + v + "'");
        }

        private static Duration parse(String name, String v) {
            try {
                return Duration.parse(v);
            } catch (DateTimeParseException ex) {
                throw new ConfigException(name + ": expected an ISO-8601 duration such as PT30M, got '" + v + "'");
            }
        }
    }
}
