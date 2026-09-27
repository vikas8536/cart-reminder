package com.quince.cartrecovery.app;

import com.quince.cartrecovery.loadgen.LoadgenRole;
import java.util.List;
import java.util.Optional;

/** Maps --role names to roles. */
public final class RoleRegistry {
    public static final List<String> NAMES =
        List.of("init", "detector", "scheduler", "dispatcher", "reconciler", "replay", "loadgen");

    private RoleRegistry() {}

    /** Every role has a no-arg constructor; loadgen reads RATE, DURATION and RUN_PREFIX itself (controller ruling R9). */
    public static Optional<Role> create(String name) {
        Role role = switch (name) {
            case "init" -> new InitRole();
            case "detector" -> new DetectorRole();
            case "scheduler" -> new SchedulerRole();
            case "dispatcher" -> new DispatcherRole();
            case "reconciler" -> new ReconcilerRole();
            case "replay" -> new ReplayRole();
            case "loadgen" -> new LoadgenRole();
            default -> null;
        };
        return Optional.ofNullable(role);
    }
}
