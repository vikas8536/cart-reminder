package com.quince.cartrecovery.app;

/** An invalid environment value. The message is one line and starts with the variable name. */
public final class ConfigException extends RuntimeException {
    public ConfigException(String message) {
        super(message);
    }
}
