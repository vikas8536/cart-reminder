package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Metrics;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * One consumer and its poll thread (spec §6.3). Each batch is grouped by record key; groups run
 * concurrently on virtual threads, at most {@code maxInFlight} at once, records within a group in
 * order. After the batch each partition is committed up to its lowest held or unfinished offset,
 * never past it. While a batch is in flight the poll thread keeps polling with every partition
 * paused, so membership, hooks, health and rebalances continue; a revoke commits only completed
 * prefixes.
 */
public final class BatchConsumerLoop<V> implements AutoCloseable {

    public enum Verdict { DONE, HOLD }

    public interface Handler<V> {
        Verdict handle(ConsumerRecord<String, V> record) throws Exception;
    }

    /** Called on the poll thread. Hooks may read offsets and metadata but must not call poll. */
    public interface Hooks<V> {
        default void beforePoll(Consumer<String, V> consumer) {}

        /** {@code committed} maps each partition to the next offset to consume. */
        default void afterCommit(Consumer<String, V> consumer, Map<TopicPartition, Long> committed, int generation) {}
    }

    public record Settings(String groupId, List<String> topics, int maxPollRecords, Duration pollTimeout,
                           int maxInFlight, String dlqTopic /* nullable */) {
        public Settings {
            topics = List.copyOf(topics);
            if (maxPollRecords < 1 || maxInFlight < 1) throw new IllegalArgumentException("maxPollRecords and maxInFlight must be >= 1");
        }
    }

    private static final System.Logger LOG = System.getLogger(BatchConsumerLoop.class.getName());
    static final int ATTEMPTS = 3;
    static final long RETRY_BACKOFF_MS = 100;
    static final long MAX_BACKOFF_MS = 30_000;
    static final Duration DRAIN_BUDGET = Duration.ofSeconds(25);
    static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);

    private enum State { PENDING, DONE, HELD, FAILED }

    private static final class Rec {
        final ConsumerRecord<String, byte[]> raw;
        volatile State state = State.PENDING;

        Rec(ConsumerRecord<String, byte[]> raw) {
            this.raw = raw;
        }
    }

    private static final class Batch {
        final Map<TopicPartition, List<Rec>> byPartition = new HashMap<>();
        final Set<TopicPartition> revoked = new HashSet<>();   // committed by the revoke; skipped at batch end
        CountDownLatch done;
    }

    private final KafkaConsumer<String, byte[]> consumer;
    private final Consumer<String, V> view;
    private final Settings settings;
    private final Deserializer<V> valueDeserializer;
    private final Handler<V> handler;
    private final Hooks<V> hooks;
    private final Producer<String, byte[]> dlqProducer;
    private final Health health;
    private final Metrics metrics;
    private final String loopName;
    private final Semaphore permits;
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<TopicPartition, Long> notBefore = new HashMap<>();   // poll thread only; System.nanoTime
    private final Map<TopicPartition, Integer> failures = new HashMap<>(); // poll thread only
    private final AtomicBoolean started = new AtomicBoolean();
    private final CountDownLatch stopped = new CountDownLatch(1);
    private volatile Predicate<TopicPartition> shouldPause = tp -> false;
    private volatile boolean closing;
    private boolean committedThisIteration;                                 // poll thread only
    private Batch batch;                                                    // poll thread only

    public BatchConsumerLoop(Map<String, Object> consumerProps, Settings settings, Deserializer<V> valueDeserializer,
                             Handler<V> handler, Hooks<V> hooks, Producer<String, byte[]> dlqProducer,
                             Health health, Metrics metrics) {
        Map<String, Object> props = new HashMap<>(consumerProps);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, settings.groupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic");   // watermark fencing needs classic generations
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, settings.maxPollRecords());
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<>(props, new StringDeserializer(), new ByteArrayDeserializer());
        this.view = viewOf(consumer);
        this.settings = settings;
        this.valueDeserializer = valueDeserializer;
        this.handler = handler;
        this.hooks = hooks;
        this.dlqProducer = dlqProducer;
        this.health = health;
        this.metrics = metrics;
        this.loopName = "consumer:" + settings.groupId() + ":" + String.join(",", settings.topics());
        this.permits = new Semaphore(settings.maxInFlight());
        // Fetch DLQ metadata now, on this platform thread, so a first poison send from a virtual
        // thread does not block on metadata; also fails fast if the DLQ topic is missing.
        if (settings.dlqTopic() != null) dlqProducer.partitionsFor(settings.dlqTopic());
    }

    /** Evaluated for every assigned partition on every iteration; a partition resumes when it returns false. */
    public void pauseWhile(Predicate<TopicPartition> shouldPause) {
        this.shouldPause = shouldPause;
    }

    /** Runs on the calling thread until {@link #close()}. */
    public void run() {
        if (!started.compareAndSet(false, true)) throw new IllegalStateException("run() already called or loop closed");
        try {
            consumer.subscribe(settings.topics(), new Rebalance());
            while (!closing) {
                committedThisIteration = false;
                hooks.beforePoll(view);
                applyPauses();
                ConsumerRecords<String, byte[]> records;
                if (batch == null) {
                    records = consumer.poll(settings.pollTimeout());
                } else {
                    awaitBatch(settings.pollTimeout());
                    records = consumer.poll(Duration.ZERO);   // everything paused: keeps membership alive
                }
                health.beat(loopName);
                if (batch == null) {
                    if (!records.isEmpty()) start(records);
                } else {
                    rewind(records);
                    if (batch.done.getCount() == 0) finishBatch();
                }
                if (!committedThisIteration) {   // idle or in-flight iteration: hooks still run every loop
                    hooks.afterCommit(view, Map.of(), consumer.groupMetadata().generationId());
                }
            }
            if (batch != null) {
                awaitBatch(DRAIN_BUDGET);
                finishBatch();                                // commits completed prefixes only
            }
        } finally {
            release();
            stopped.countDown();
        }
    }

    /** SIGTERM path: stop polling, let in-flight groups finish, commit, close; returns within 30 s. */
    @Override
    public void close() {
        closing = true;
        if (started.compareAndSet(false, true)) {   // never ran: nothing in flight
            release();
            stopped.countDown();
            return;
        }
        try {
            if (!stopped.await(30, TimeUnit.SECONDS)) LOG.log(Level.WARNING, loopName + ": did not stop within 30 s");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void start(ConsumerRecords<String, byte[]> records) {
        Batch b = new Batch();
        Map<String, List<Rec>> groups = new LinkedHashMap<>();   // a null key forms one serial group
        for (TopicPartition tp : records.partitions()) {
            List<Rec> recs = new ArrayList<>();
            for (ConsumerRecord<String, byte[]> r : records.records(tp)) {
                Rec rec = new Rec(r);
                recs.add(rec);
                groups.computeIfAbsent(r.key(), k -> new ArrayList<>()).add(rec);
            }
            b.byPartition.put(tp, recs);
        }
        b.done = new CountDownLatch(groups.size());
        batch = b;
        for (List<Rec> group : groups.values()) workers.execute(() -> runGroup(group, b.done));
    }

    private void runGroup(List<Rec> group, CountDownLatch done) {
        try {
            permits.acquire();
            try {
                for (Rec rec : group) {
                    rec.state = process(rec);
                    if (rec.state != State.DONE) return;   // later records of this key wait for redelivery
                }
            } finally {
                permits.release();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();          // shutdown past the drain budget: rest stays PENDING
        } catch (RuntimeException e) {
            LOG.log(Level.ERROR, loopName + ": unexpected group failure", e);
        } finally {
            done.countDown();
        }
    }

    private State process(Rec rec) throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            try {
                if (handler.handle(typed(rec.raw)) == Verdict.HOLD) {
                    metrics.increment("consumer.hold");
                    return State.HELD;
                }
                return State.DONE;
            } catch (PoisonException e) {
                if (deadLetter(rec.raw, e)) return State.DONE;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                LOG.log(Level.WARNING, loopName + ": attempt " + attempt + " failed at " + position(rec.raw), e);
            }
            if (attempt >= ATTEMPTS) {
                metrics.increment("consumer.retry_exhausted");
                return State.FAILED;
            }
            metrics.increment("consumer.retry");
            Thread.sleep(RETRY_BACKOFF_MS << (attempt - 1));
        }
    }

    private ConsumerRecord<String, V> typed(ConsumerRecord<String, byte[]> r) {
        V value;
        try {
            value = valueDeserializer.deserialize(r.topic(), r.headers(), r.value());
        } catch (RuntimeException e) {
            throw new PoisonException("undeserializable value: " + e.getMessage(), e);
        }
        return new ConsumerRecord<>(r.topic(), r.partition(), r.offset(), r.timestamp(), r.timestampType(),
            r.serializedKeySize(), r.serializedValueSize(), r.key(), value, r.headers(), r.leaderEpoch());
    }

    /** True when the record is dead-lettered (or dropped for lack of a DLQ); false to retry. */
    private boolean deadLetter(ConsumerRecord<String, byte[]> r, PoisonException e) {
        if (settings.dlqTopic() == null) {
            metrics.increment("consumer.poison_dropped");
            LOG.log(Level.WARNING, loopName + ": dropped poison record at " + position(r) + ": " + e.getMessage());
            return true;
        }
        ProducerRecord<String, byte[]> out = new ProducerRecord<>(settings.dlqTopic(), r.key(), r.value());
        out.headers().add("error", String.valueOf(e.getMessage()).getBytes(StandardCharsets.UTF_8));
        out.headers().add("source-offset", position(r).getBytes(StandardCharsets.UTF_8));
        try {
            dlqProducer.send(out).get();
            metrics.increment("consumer.poison");
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException | RuntimeException ex) {
            LOG.log(Level.WARNING, loopName + ": DLQ produce failed for " + position(r), ex);
            return false;
        }
    }

    private void finishBatch() {
        Batch b = batch;
        batch = null;
        Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
        long now = System.nanoTime();
        b.byPartition.forEach((tp, recs) -> {
            if (b.revoked.contains(tp)) return;
            Rec first = firstUnfinished(recs);
            long commitAt = commitPoint(recs, first);
            commits.put(tp, new OffsetAndMetadata(commitAt));
            if (first == null) {
                failures.remove(tp);
                return;
            }
            consumer.seek(tp, commitAt);   // redeliver from the first held, failed or unstarted record
            if (first.state == State.HELD) {
                notBefore.put(tp, now + settings.pollTimeout().toNanos());
                consumer.pause(List.of(tp));
            } else if (first.state == State.FAILED) {
                int n = failures.merge(tp, 1, Integer::sum);
                long backoffMs = Math.min(MAX_BACKOFF_MS, 1000L << Math.min(n - 1, 15));
                notBefore.put(tp, now + TimeUnit.MILLISECONDS.toNanos(backoffMs));
                consumer.pause(List.of(tp));
            }
        });
        commit(commits);
    }

    private static Rec firstUnfinished(List<Rec> recs) {
        for (Rec r : recs) if (r.state != State.DONE) return r;
        return null;
    }

    private static long commitPoint(List<Rec> recs, Rec firstUnfinished) {
        return firstUnfinished == null ? recs.get(recs.size() - 1).raw.offset() + 1 : firstUnfinished.raw.offset();
    }

    private void commit(Map<TopicPartition, OffsetAndMetadata> commits) {
        if (commits.isEmpty()) return;
        try {
            consumer.commitSync(commits);
        } catch (KafkaException e) {   // lost membership, rebalance, timeout: the records are redelivered
            metrics.increment("consumer.commit_failed");
            LOG.log(Level.WARNING, loopName + ": commit failed for " + commits, e);
            return;
        }
        Map<TopicPartition, Long> offsets = new HashMap<>();
        commits.forEach((tp, o) -> offsets.put(tp, o.offset()));
        committedThisIteration = true;
        hooks.afterCommit(view, Map.copyOf(offsets), consumer.groupMetadata().generationId());
    }

    private void applyPauses() {
        Set<TopicPartition> assigned = consumer.assignment();
        if (assigned.isEmpty()) return;
        long now = System.nanoTime();
        List<TopicPartition> pause = new ArrayList<>();
        List<TopicPartition> resume = new ArrayList<>();
        for (TopicPartition tp : assigned) {
            Long until = notBefore.get(tp);
            boolean waiting = until != null && until - now > 0;
            if (until != null && !waiting) notBefore.remove(tp);
            boolean wanted = pauseWanted(tp);
            (batch != null || waiting || wanted ? pause : resume).add(tp);
        }
        consumer.pause(pause);
        consumer.resume(resume);
    }

    private boolean pauseWanted(TopicPartition tp) {
        try {
            return shouldPause.test(tp);
        } catch (RuntimeException e) {   // an unreadable gate is a closed gate
            metrics.increment("consumer.pause_check_failed");
            LOG.log(Level.WARNING, loopName + ": pause check failed for " + tp, e);
            return true;
        }
    }

    private void awaitBatch(Duration timeout) {
        try {
            batch.done.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            closing = true;   // an interrupted poll thread shuts the loop down through the normal drain
        }
    }

    /** Records polled while a batch is in flight (a partition assigned mid-batch) are fetched again later. */
    private void rewind(ConsumerRecords<String, byte[]> records) {
        for (TopicPartition tp : records.partitions()) consumer.seek(tp, records.records(tp).get(0).offset());
    }

    private void release() {
        workers.shutdownNow();
        try {
            consumer.close(CloseOptions.timeout(CLOSE_TIMEOUT));
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, loopName + ": consumer close failed", e);
        }
    }

    private void forget(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            notBefore.remove(tp);
            failures.remove(tp);
        }
    }

    private static String position(ConsumerRecord<String, ?> r) {
        return new TopicPartition(r.topic(), r.partition()) + "@" + r.offset();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <V> Consumer<String, V> viewOf(Consumer<String, byte[]> consumer) {
        return (Consumer) consumer;   // hooks never read values, so the value type is irrelevant to them
    }

    /** Runs on the poll thread, inside poll(). */
    private final class Rebalance implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            forget(partitions);
            Batch b = batch;
            if (b == null) return;   // nothing in flight: the last batch end already committed
            Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
            for (TopicPartition tp : partitions) {
                List<Rec> recs = b.byPartition.get(tp);
                if (recs == null || !b.revoked.add(tp)) continue;
                commits.put(tp, new OffsetAndMetadata(commitPoint(recs, firstUnfinished(recs))));
            }
            commit(commits);   // completed prefixes only; still-running records are redelivered to the new owner
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            forget(partitions);
            if (batch != null) batch.revoked.addAll(partitions);   // no longer ours: never commit them
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            // Runs inside poll(), which then fetches from these partitions in the same call, before
            // the next applyPauses: gate them here or a paused partition delivers one batch.
            consumer.pause(partitions.stream().filter(tp -> batch != null || pauseWanted(tp)).toList());
        }
    }
}
