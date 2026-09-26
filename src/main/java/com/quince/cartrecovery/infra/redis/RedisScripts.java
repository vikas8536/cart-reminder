package com.quince.cartrecovery.infra.redis;

import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Loads the Lua scripts from /redis/*.lua once and runs them by SHA, reloading after NOSCRIPT. */
final class RedisScripts {
    private final RedisCommands<String, String> redis;
    private final Map<String, String> bodies = new ConcurrentHashMap<>();
    private final Map<String, String> shas = new ConcurrentHashMap<>();

    RedisScripts(RedisCommands<String, String> redis, String... names) {
        this.redis = redis;
        for (String name : names) {
            String body = load(name);
            bodies.put(name, body);
            shas.put(name, redis.scriptLoad(body));
        }
    }

    <T> T run(String name, ScriptOutputType type, String[] keys, String... args) {
        try {
            return redis.evalsha(shas.get(name), type, keys, args);
        } catch (RedisNoScriptException e) {
            // Redis restarted or SCRIPT FLUSH ran: the script cache is empty. Reload and retry once.
            shas.put(name, redis.scriptLoad(bodies.get(name)));
            return redis.evalsha(shas.get(name), type, keys, args);
        }
    }

    private static String load(String name) {
        try (InputStream in = RedisScripts.class.getResourceAsStream("/redis/" + name + ".lua")) {
            if (in == null) throw new IllegalStateException("missing script /redis/" + name + ".lua");
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
