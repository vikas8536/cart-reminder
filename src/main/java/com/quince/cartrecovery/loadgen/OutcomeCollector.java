package com.quince.cartrecovery.loadgen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.model.Lane;
import com.quince.cartrecovery.model.LedgerKey;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reads {@code sink-sends} and {@code reminder-outcomes} from the beginning on a throwaway
 * consumer group, keeping only rows whose {@code cartId} carries this run's prefix. Send latency and
 * lane come from the loadgen's own script (controller ruling R11): the ledger key gives cartId, version
 * and offsetIndex; the script gives that version's lastActivityAt.
 */
final class OutcomeCollector implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final String bootstrap;
    /** The workload keys every cart id {@code runPrefix + "-" + i} (see Workload.generateCarts); matching on the
     * bare prefix would also match another run whose prefix is a leading substring of this one. */
    private final String runPrefixWithDelimiter;
    private final Map<String, Instant> lastActivityByCartVersion;
    private final List<Duration> offsets;
    private final int fastOffsets;
    private final List<SinkSend> sends = new CopyOnWriteArrayList<>();
    private final List<OutcomeRow> outcomes = new CopyOnWriteArrayList<>();
    private final List<LatencySample> fastLatencies = new CopyOnWriteArrayList<>();
    private final List<LatencySample> slowLatencies = new CopyOnWriteArrayList<>();
    /** Fix round 1, finding 3: next-offset-to-read per partition, so a caller can tell when this
     * collector has caught up to a given end-offset snapshot before the run's results are computed. */
    private final Map<TopicPartition, Long> positions = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread pollThread;

    OutcomeCollector(String bootstrap, String runPrefix, Map<String, Instant> lastActivityByCartVersion,
                     List<Duration> offsets, int fastOffsets) {
        this.bootstrap = bootstrap;
        this.runPrefixWithDelimiter = runPrefix + "-";
        this.lastActivityByCartVersion = Map.copyOf(lastActivityByCartVersion);
        this.offsets = List.copyOf(offsets);
        this.fastOffsets = fastOffsets;
    }

    void start() {
        running.set(true);
        pollThread = new Thread(this::pollLoop, "loadgen-outcome-collector");
        pollThread.start();
    }

    void stop() throws InterruptedException {
        running.set(false);
        pollThread.join(Duration.ofSeconds(10).toMillis());
    }

    /** For tests: true once {@code start()} has run and before {@code stop()}'s join completes. */
    boolean isRunning() { return pollThread != null && pollThread.isAlive(); }

    private void pollLoop() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "loadgen-observer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(Topics.SINK_SENDS, Topics.OUTCOMES));
            while (running.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> record : records) {
                    positions.put(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
                    if (record.key() == null || !record.key().startsWith(runPrefixWithDelimiter)) continue;
                    try {
                        JsonNode node = JSON.readTree(record.value());
                        if (record.topic().equals(Topics.SINK_SENDS)) handleSinkSend(node);
                        else handleOutcome(node);
                    } catch (Exception e) {
                        System.err.println("skipping unparsable record on " + record.topic() + ": " + e.getMessage());
                    }
                }
            }
        }
    }

    private void handleSinkSend(JsonNode node) {
        SinkSend send = new SinkSend(
            node.get("key").asText(),
            node.get("cartId").asText(),
            Instant.ofEpochMilli(node.get("at").asLong()),
            node.get("hasFirstName").asBoolean(),
            node.get("itemCount").asInt());
        sends.add(send);
    }

    private void handleOutcome(JsonNode node) {
        JsonNode keyNode = node.get("key");
        OutcomeRow row = new OutcomeRow(
            keyNode == null || keyNode.isNull() ? null : keyNode.asText(),
            node.get("cartId").asText(),
            node.get("version").asLong(),
            node.get("arm").asText(),
            node.get("kind").asText(),
            Instant.ofEpochMilli(node.get("at").asLong()),
            node.get("attempts").asInt());
        outcomes.add(row);

        if ("SENT".equals(row.kind()) && row.key() != null) {
            LedgerKey k = LedgerKey.parse(row.key());
            Instant lastActivityAt = lastActivityByCartVersion.get(k.cartId() + ":" + k.version());
            if (lastActivityAt == null || k.offsetIndex() >= offsets.size()) return;   // not scripted by this run
            Instant scheduledFor = lastActivityAt.plus(offsets.get(k.offsetIndex()));
            LatencySample sample = new LatencySample(scheduledFor, Duration.between(scheduledFor, row.at()));
            (Lane.of(k.offsetIndex(), fastOffsets) == Lane.FAST ? fastLatencies : slowLatencies).add(sample);
        }
    }

    List<SinkSend> sinkSends() { return List.copyOf(sends); }
    List<OutcomeRow> outcomes() { return List.copyOf(outcomes); }
    List<LatencySample> fastLatencies() { return List.copyOf(fastLatencies); }
    List<LatencySample> slowLatencies() { return List.copyOf(slowLatencies); }
    Map<TopicPartition, Long> positions() { return Map.copyOf(positions); }

    @Override public void close() {
        running.set(false);
    }
}
