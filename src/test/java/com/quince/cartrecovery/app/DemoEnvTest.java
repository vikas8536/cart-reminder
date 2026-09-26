package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DemoEnvTest {
    @Test
    void demoEnvIsAValidConfigurationWithShortTimings() throws Exception {
        Map<String, String> env = new HashMap<>();
        for (String line : Files.readAllLines(Path.of("demo.env"))) {
            String l = line.strip();
            if (l.isEmpty() || l.startsWith("#")) continue;
            int eq = l.indexOf('=');
            env.put(l.substring(0, eq), l.substring(eq + 1));
        }

        InfraConfig c = InfraConfig.fromEnv(env);

        assertEquals(Duration.ofSeconds(30), c.recovery().window());
        assertEquals(List.of(Duration.ofSeconds(30), Duration.ofSeconds(60), Duration.ofSeconds(120)),
            c.recovery().offsets());
        assertEquals(3, c.recovery().latenessBounds().size());
    }
}
