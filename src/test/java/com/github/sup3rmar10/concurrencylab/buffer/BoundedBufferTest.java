package com.github.sup3rmar10.concurrencylab.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import org.junit.jupiter.api.Test;

class BoundedBufferTest {

    /** Adapter so the reference implementation runs through the same scenarios. */
    static final class QueueBuffer<T> implements Buffer<T> {
        private final BlockingQueue<T> q;
        private final int capacity;

        QueueBuffer(int capacity) {
            this.q = new LinkedBlockingQueue<>(capacity);
            this.capacity = capacity;
        }

        public void put(T item) throws InterruptedException { q.put(item); }
        public T take() throws InterruptedException { return q.take(); }
        public boolean offer(T item, long t, TimeUnit u) throws InterruptedException { return q.offer(item, t, u); }
        public T poll(long t, TimeUnit u) throws InterruptedException { return q.poll(t, u); }
        public int size() { return q.size(); }
        public int capacity() { return capacity; }
    }

    private static final long POISON = -1L;

    private static void awaitState(Thread t, Thread.State wanted) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (t.getState() != wanted && System.nanoTime() < deadline) Thread.onSpinWait();
        assertEquals(wanted, t.getState());
    }

    @Test
    void fifoOrderAndSize() throws Exception {
        BoundedBuffer<Integer> b = new BoundedBuffer<>(3);
        b.put(1);
        b.put(2);
        b.put(3);
        assertEquals(3, b.size());
        assertEquals(1, b.take());
        b.put(4); // wraps around the ring
        assertEquals(List.of(2, 3, 4), List.of(b.take(), b.take(), b.take()));
        assertEquals(0, b.size());
    }

    @Test
    void putBlocksWhenFullUntilSomeoneTakes() throws Exception {
        BoundedBuffer<Integer> b = new BoundedBuffer<>(1);
        b.put(1);
        Thread producer = new Thread(() -> {
            try {
                b.put(2);
            } catch (InterruptedException ignored) {
                // test teardown
            }
        });
        producer.setDaemon(true);
        producer.start();
        awaitState(producer, Thread.State.WAITING); // parked inside notFull.await()
        assertEquals(1, b.size());
        assertEquals(1, b.take());
        producer.join(10_000);
        assertFalse(producer.isAlive());
        assertEquals(2, b.take());
    }

    @Test
    void takeBlocksWhenEmptyUntilSomeonePuts() throws Exception {
        BoundedBuffer<Integer> b = new BoundedBuffer<>(2);
        List<Integer> got = new ArrayList<>();
        Thread consumer = new Thread(() -> {
            try {
                got.add(b.take());
            } catch (InterruptedException ignored) {
                // test teardown
            }
        });
        consumer.setDaemon(true);
        consumer.start();
        awaitState(consumer, Thread.State.WAITING);
        b.put(42);
        consumer.join(10_000);
        assertEquals(List.of(42), got);
    }

    @Test
    void offerTimesOutWhenFullAndPollTimesOutWhenEmpty() throws Exception {
        BoundedBuffer<Integer> b = new BoundedBuffer<>(1);
        assertNull(b.poll(10, TimeUnit.MILLISECONDS));
        assertTrue(b.offer(1, 10, TimeUnit.MILLISECONDS));
        assertFalse(b.offer(2, 10, TimeUnit.MILLISECONDS));
        assertEquals(1, b.size());
    }

    @Test
    void timedOfferSucceedsWhenSpaceAppearsInTime() throws Exception {
        BoundedBuffer<Integer> b = new BoundedBuffer<>(1);
        b.put(1);
        boolean[] accepted = new boolean[1];
        Thread offerer = new Thread(() -> {
            try {
                accepted[0] = b.offer(2, 20, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                // test teardown
            }
        });
        offerer.setDaemon(true);
        offerer.start();
        awaitState(offerer, Thread.State.TIMED_WAITING);
        assertEquals(1, b.take());
        offerer.join(10_000);
        assertTrue(accepted[0]);
        assertEquals(2, b.take());
    }

    @Test
    void blockedTakeIsInterruptibleAndLeavesTheBufferUsable() throws Exception {
        BoundedBuffer<Integer> b = new BoundedBuffer<>(1);
        boolean[] interrupted = new boolean[1];
        Thread consumer = new Thread(() -> {
            try {
                b.take();
            } catch (InterruptedException e) {
                interrupted[0] = true;
            }
        });
        consumer.setDaemon(true);
        consumer.start();
        awaitState(consumer, Thread.State.WAITING);
        consumer.interrupt();
        consumer.join(10_000);
        assertTrue(interrupted[0]);
        b.put(7); // lock was released
        assertEquals(7, b.take());
    }

    @Test
    void rejectsNullsAndBadCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new BoundedBuffer<Integer>(0));
        assertThrows(NullPointerException.class, () -> new BoundedBuffer<Integer>(1).put(null));
        assertThrows(NullPointerException.class, () -> new BoundedBuffer<Integer>(1).offer(null, 1, TimeUnit.MILLISECONDS));
    }

    /** Sequential differential test: random non-blocking ops give identical answers to LinkedBlockingQueue. */
    @Test
    void matchesLinkedBlockingQueueOnRandomSequentialOperations() throws Exception {
        for (int seed = 0; seed < 300; seed++) {
            Random rnd = new Random(seed);
            int cap = 1 + rnd.nextInt(6);
            Buffer<Integer> mine = new BoundedBuffer<>(cap);
            Buffer<Integer> ref = new QueueBuffer<>(cap);
            for (int step = 0; step < 200; step++) {
                String where = "seed " + seed + " step " + step;
                if (rnd.nextBoolean()) {
                    int v = rnd.nextInt(1000);
                    assertEquals(ref.offer(v, 0, TimeUnit.MILLISECONDS), mine.offer(v, 0, TimeUnit.MILLISECONDS), where);
                } else {
                    assertEquals(ref.poll(0, TimeUnit.MILLISECONDS), mine.poll(0, TimeUnit.MILLISECONDS), where);
                }
                assertEquals(ref.size(), mine.size(), where);
            }
        }
    }

    /**
     * Concurrent scenario run against an implementation: P producers, C consumers, tiny capacity.
     * Invariants: every item consumed exactly once, per-producer order preserved per consumer,
     * and the buffer never holds more than its capacity.
     */
    private static void runScenario(IntFunction<Buffer<Long>> factory, long seed) throws Exception {
        Random rnd = new Random(seed);
        int producers = 1 + rnd.nextInt(4);
        int consumers = 1 + rnd.nextInt(4);
        int capacity = 1 + rnd.nextInt(4);
        int perProducer = 150;
        Buffer<Long> buf = factory.apply(capacity);
        AtomicInteger overCapacity = new AtomicInteger();
        List<List<Long>> taken = new ArrayList<>();
        List<Thread> all = new ArrayList<>();
        List<Throwable> errors = java.util.Collections.synchronizedList(new ArrayList<>());

        List<Thread> producerThreads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            long base = (p + 1) * 1_000_000L;
            Thread t = new Thread(() -> {
                try {
                    for (int i = 0; i < perProducer; i++) {
                        buf.put(base + i);
                        if (buf.size() > capacity) overCapacity.incrementAndGet();
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
            producerThreads.add(t);
            all.add(t);
        }
        for (int c = 0; c < consumers; c++) {
            List<Long> mine = new ArrayList<>();
            taken.add(mine);
            Thread t = new Thread(() -> {
                try {
                    for (Long v = buf.take(); v != POISON; v = buf.take()) mine.add(v);
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
            all.add(t);
        }
        all.forEach(t -> t.setDaemon(true));
        all.forEach(Thread::start);
        for (Thread t : producerThreads) t.join(20_000);
        for (int c = 0; c < consumers; c++) buf.put(POISON);
        for (Thread t : all) {
            t.join(20_000);
            assertFalse(t.isAlive(), "thread stuck, seed " + seed);
        }

        assertTrue(errors.isEmpty(), "errors " + errors + " seed " + seed);
        assertEquals(0, overCapacity.get(), "buffer exceeded capacity, seed " + seed);
        Set<Long> seen = new HashSet<>();
        int count = 0;
        for (List<Long> mine : taken) {
            long[] lastSeq = new long[producers + 1];
            java.util.Arrays.fill(lastSeq, -1);
            for (long v : mine) {
                assertTrue(seen.add(v), "duplicate " + v + " seed " + seed);
                int p = (int) (v / 1_000_000L);
                long seq = v % 1_000_000L;
                assertTrue(seq > lastSeq[p], "producer " + p + " reordered, seed " + seed);
                lastSeq[p] = seq;
                count++;
            }
        }
        assertEquals(producers * perProducer, count, "lost items, seed " + seed);
    }

    @Test
    void boundedBufferHoldsInvariantsOverManyRandomInterleavings() throws Exception {
        for (long seed = 0; seed < 150; seed++) runScenario(BoundedBuffer::new, seed);
    }

    @Test
    void referenceLinkedBlockingQueueHoldsTheSameInvariants() throws Exception {
        // Guards the test itself: if the scenario were wrong, the reference would fail it too.
        for (long seed = 0; seed < 150; seed++) runScenario(QueueBuffer::new, seed);
    }
}
