package com.quince.cartrecovery.e2e;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.OutputStream;
import java.io.PrintStream;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Spec §6.3: integration tests fail on any virtual-thread pinning report. With -Djdk.tracePinnedThreads=full
 * the JDK prints a stack containing "onPinned" to System.out; this tees System.out and fails the test
 * during which such a report appeared. Auto-registered for every integration test class.
 */
public final class PinningGuard implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback {
    private static final Object LOCK = new Object();
    private static StringBuilder captured;

    @Override
    public void beforeAll(ExtensionContext context) {
        assertEquals("full", System.getProperty("jdk.tracePinnedThreads"),
            "integrationTest must run with -Djdk.tracePinnedThreads=full");
        synchronized (LOCK) {
            if (captured != null) return;
            captured = new StringBuilder();
            PrintStream original = System.out;
            System.setOut(new PrintStream(new OutputStream() {
                @Override public void write(int b) {
                    original.write(b);
                    synchronized (LOCK) { captured.append((char) b); }
                }
                @Override public void write(byte[] b, int off, int len) {
                    original.write(b, off, len);
                    synchronized (LOCK) { captured.append(new String(b, off, len, UTF_8)); }
                }
                @Override public void flush() { original.flush(); }
            }, true, UTF_8));
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        drain();
    }

    @Override
    public void afterEach(ExtensionContext context) {
        String out = drain();
        int at = out.indexOf("onPinned");
        if (at >= 0) fail("virtual thread pinned:\n" + out.substring(Math.max(0, at - 300), Math.min(out.length(), at + 3000)));
    }

    /** Returns and clears everything printed since the last drain. */
    static String drain() {
        synchronized (LOCK) {
            if (captured == null) return "";
            String s = captured.toString();
            captured.setLength(0);
            return s;
        }
    }
}
