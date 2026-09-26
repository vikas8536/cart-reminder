package com.quince.cartrecovery.infra.dynamo;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

/** Attribute-value helpers shared by the DynamoDB adapters. Every attribute name goes through #name placeholders. */
final class Attrs {
    private static final Pattern NAME = Pattern.compile("#[A-Za-z]+");

    private Attrs() {}

    static AttributeValue s(String v) { return AttributeValue.fromS(v); }

    static AttributeValue n(long v) { return AttributeValue.fromN(Long.toString(v)); }

    static AttributeValue millis(Instant t) { return n(t.toEpochMilli()); }

    static AttributeValue instants(List<Instant> ts) {
        return AttributeValue.fromL(ts.stream().map(Attrs::millis).toList());
    }

    static String str(Map<String, AttributeValue> item, String name) {
        AttributeValue v = item.get(name);
        return v == null ? null : v.s();
    }

    static long num(Map<String, AttributeValue> item, String name) {
        return Long.parseLong(item.get(name).n());
    }

    static Instant instant(Map<String, AttributeValue> item, String name) {
        return Instant.ofEpochMilli(num(item, name));
    }

    static List<Instant> instantList(AttributeValue v) {
        return v == null ? List.of() : v.l().stream().map(x -> Instant.ofEpochMilli(Long.parseLong(x.n()))).toList();
    }

    /** Placeholder map for every #name used in the expressions; DynamoDB rejects unused placeholders. */
    static Map<String, String> names(String... expressions) {
        Map<String, String> names = new HashMap<>();
        Matcher m = NAME.matcher(String.join(" ", expressions));
        while (m.find()) names.put(m.group(), m.group().substring(1));
        return names;
    }

    /** Conditional UpdateItem; false when the condition failed. */
    static boolean conditionalUpdate(DynamoDbClient ddb, String table, Map<String, AttributeValue> key,
                                     String update, String condition, Map<String, AttributeValue> values) {
        try {
            ddb.updateItem(b -> b.tableName(table).key(key).updateExpression(update).conditionExpression(condition)
                    .expressionAttributeNames(names(update, condition)).expressionAttributeValues(values));
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }

    static void backoff(int round) {
        try {
            Thread.sleep(Math.min(1_000L, 25L << Math.min(round, 6)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted during DynamoDB backoff", e);
        }
    }
}
