package com.github.sup3rmar10.concurrencylab;

import com.github.sup3rmar10.concurrencylab.counter.CounterDemo;
import com.github.sup3rmar10.concurrencylab.deadlock.DeadlockDemo;
import com.github.sup3rmar10.concurrencylab.pool.BoundedWorkerPool;
import com.github.sup3rmar10.concurrencylab.pool.BoundedWorkerPool.RejectPolicy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/** Runs the three demos: counters, deadlock, reject policies. Synthetic data only. */
public final class Demo {

    public static void main(String[] args) throws Exception {
        System.out.println("== 1. counters: lost updates and throughput ==");
        CounterDemo.main(args);
        System.out.println();
        System.out.println("== 2. deadlock: detect it, then the two fixes ==");
        DeadlockDemo.run(System.out);
        System.out.println();
        System.out.println("== 3. worker pool: 1 worker busy, queue capacity 2, 5 more tasks submitted ==");
        poolDemo();
    }

    private static void poolDemo() throws Exception {
        for (RejectPolicy policy : RejectPolicy.values()) {
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            List<String> ran = Collections.synchronizedList(new ArrayList<>());
            BoundedWorkerPool pool = new BoundedWorkerPool(1, 2, policy, (t, e) -> { });
            pool.execute(() -> {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                ran.add("t0");
            });
            started.await();
            int thrown = 0;
            for (int i = 1; i <= 5; i++) {
                String name = "t" + i;
                try {
                    pool.execute(() -> ran.add(Thread.currentThread().getName().startsWith("pool-") ? name : name + "(caller)"));
                } catch (RejectedExecutionException e) {
                    thrown++;
                }
            }
            release.countDown();
            pool.shutdown();
            pool.awaitTermination(5, TimeUnit.SECONDS);
            System.out.printf("%-15s ran=%s exceptions=%d discarded=%d%n", policy, ran, thrown, pool.discardedCount());
        }
    }

    private Demo() { }
}
