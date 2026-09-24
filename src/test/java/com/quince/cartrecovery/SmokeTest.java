package com.quince.cartrecovery;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SmokeTest {
    @Test
    void junitRunsOnJava21() {
        assertEquals(21, Runtime.version().feature());
    }
}
