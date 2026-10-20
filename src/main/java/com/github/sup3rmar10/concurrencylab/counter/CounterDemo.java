package com.github.sup3rmar10.concurrencylab.counter;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

/** Laptop microbenchmark (NOT JMH): lost updates and throughput per counter flavour. */
public final class CounterDemo {

    public record Result(long expected, long actual, long nanos) {
        public long lost() {
            return expected - actual;
        }
    }

    /** Runs {@code threads} threads, each calling increment() {@code perThread} times, released together. */
    public static Result measure(Supplier<Counter> factory, int threads, long perThread) throws InterruptedException {
        Counter c = factory.get();
        CountDownLatch gate = new CountDownLatch(1);
        Thread[] ts = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            ts[i] = new Thread(() -> {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    return;
                }
                for (long n = 0; n < perThread; n++) c.increment();
            });
            ts[i].start();
        }
        long t0 = System.nanoTime();
        gate.countDown();
        for (Thread t : ts) t.join();
        long nanos = System.nanoTime() - t0;
        return new Result(threads * perThread, c.get(), nanos);
    }

    public static void main(String[] args) throws InterruptedException {
        int threads = Math.min(8, Runtime.getRuntime().availableProcessors());
        long perThread = 2_000_000;
        Map<String, Supplier<Counter>> flavours = new LinkedHashMap<>();
        flavours.put("racy (plain long)", RacyCounter::new);
        flavours.put("synchronized", SynchronizedCounter::new);
        flavours.put("ReentrantLock", LockCounter::new);
        flavours.put("AtomicLong", AtomicCounter::new);
        flavours.put("LongAdder", AdderCounter::new);

        System.out.printf("Laptop microbenchmark, not JMH. cpus=%d threads=%d increments/thread=%,d runs=3 (+1 warmup)%n",
                Runtime.getRuntime().availableProcessors(), threads, perThread);
        System.out.printf("%-20s %12s %14s %16s%n", "counter", "median ms", "M incr/s", "max lost updates");
        for (Map.Entry<String, Supplier<Counter>> e : flavours.entrySet()) {
            measure(e.getValue(), threads, 200_000); // warmup
            long[] nanos = new long[3];
            long maxLost = 0;
            for (int i = 0; i < 3; i++) {
                Result r = measure(e.getValue(), threads, perThread);
                nanos[i] = r.nanos();
                maxLost = Math.max(maxLost, r.lost());
            }
            Arrays.sort(nanos);
            double ms = nanos[1] / 1e6;
            double mops = threads * perThread / (nanos[1] / 1e9) / 1e6;
            System.out.printf("%-20s %12.1f %14.1f %,16d%n", e.getKey(), ms, mops, maxLost);
        }
    }

    private CounterDemo() { }
}
