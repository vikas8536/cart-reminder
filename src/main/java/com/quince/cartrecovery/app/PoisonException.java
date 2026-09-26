package com.quince.cartrecovery.app;

/** A deterministic failure for one record: dead-lettered and committed, never retried. */
public final class PoisonException extends RuntimeException {
    public PoisonException(String message, Throwable cause) {
        super(message, cause);
    }
}
