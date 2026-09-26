package com.quince.cartrecovery.legacy.model;

import java.time.Instant;

public record DeadLetter(NotificationIntent intent, String reason, Instant at) {}
