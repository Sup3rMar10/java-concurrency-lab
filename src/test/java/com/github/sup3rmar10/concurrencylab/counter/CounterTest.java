package com.github.sup3rmar10.concurrencylab.counter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class CounterTest {

    static Stream<Supplier<Counter>> safeCounters() {
        return Stream.of(SynchronizedCounter::new, LockCounter::new, AtomicCounter::new, AdderCounter::new);
    }

    @ParameterizedTest
    @MethodSource("safeCounters")
    void safeCountersLoseNothingUnderContention(Supplier<Counter> factory) throws Exception {
        CounterDemo.Result r = CounterDemo.measure(factory, 4, 250_000);
        assertEquals(1_000_000, r.expected());
        assertEquals(0, r.lost(), factory.get().getClass().getSimpleName() + " lost updates");
    }

    @ParameterizedTest
    @MethodSource("safeCounters")
    void safeCountersCountSingleThreaded(Supplier<Counter> factory) {
        Counter c = factory.get();
        for (int i = 0; i < 1000; i++) c.increment();
        assertEquals(1000, c.get());
    }

    @Test
    void racyCounterIsFineWithOneThread() {
        Counter c = new RacyCounter();
        for (int i = 0; i < 1000; i++) c.increment();
        assertEquals(1000, c.get());
    }

    /** Deterministic proof of the lost update: thread t1 reads, main does a whole increment, t1 writes stale value. */
    @Test
    void racyCounterLosesAnUpdateWhenInterleavedBetweenReadAndWrite() throws Exception {
        CountDownLatch t1HasRead = new CountDownLatch(1);
        CountDownLatch mainDone = new CountDownLatch(1);
        RacyCounter c = new RacyCounter(() -> {
            if (Thread.currentThread().getName().equals("t1")) {
                t1HasRead.countDown();
                try {
                    mainDone.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        Thread t1 = new Thread(c::increment, "t1");
        t1.start();
        assertTrue(t1HasRead.await(10, TimeUnit.SECONDS));
        c.increment(); // runs completely while t1 sits between its read and its write
        mainDone.countDown();
        t1.join();
        assertEquals(1, c.get(), "two increments happened, one was lost");
    }

    @Test
    void measureNeverReportsMoreThanExpectedForRacyCounter() throws Exception {
        CounterDemo.Result r = CounterDemo.measure(RacyCounter::new, 4, 100_000);
        assertTrue(r.actual() <= r.expected());
        assertTrue(r.actual() > 0);
        assertEquals(r.expected() - r.actual(), r.lost());
    }
}
