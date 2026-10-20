package com.github.sup3rmar10.concurrencylab.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sup3rmar10.concurrencylab.pool.BoundedWorkerPool.RejectPolicy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BoundedWorkerPoolTest {

    private static final Thread.UncaughtExceptionHandler QUIET = (t, e) -> { };
    private BoundedWorkerPool pool;

    @AfterEach
    void cleanup() {
        if (pool != null) pool.shutdownNow();
    }

    /** A task that signals when it starts and then blocks until released (or interrupted). */
    private static final class Blocker implements Runnable {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch interrupted = new CountDownLatch(1);

        @Override
        public void run() {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                interrupted.countDown();
            }
        }

        void awaitStarted() throws InterruptedException {
            assertTrue(started.await(10, TimeUnit.SECONDS), "blocker never started");
        }
    }

    private static final Runnable NOOP = () -> { };

    private static Runnable record(List<String> into, String name) {
        return () -> into.add(name);
    }

    @Test
    void runsEveryAcceptedTask() throws Exception {
        pool = new BoundedWorkerPool(4, 1000, RejectPolicy.ABORT, QUIET);
        CountDownLatch done = new CountDownLatch(1000);
        for (int i = 0; i < 1000; i++) pool.execute(done::countDown);
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(1000, pool.completedCount());
    }

    @Test
    void usesAllWorkersInParallelAndQueuesTheRest() throws Exception {
        pool = new BoundedWorkerPool(3, 5, RejectPolicy.ABORT, QUIET);
        Blocker[] blockers = {new Blocker(), new Blocker(), new Blocker()};
        for (Blocker b : blockers) pool.execute(b);
        for (Blocker b : blockers) b.awaitStarted(); // three tasks running at once
        pool.execute(NOOP);
        assertEquals(1, pool.queueSize(), "4th task waits: all 3 workers are busy");
        for (Blocker b : blockers) b.release.countDown();
    }

    @Test
    void abortThrowsWhenQueueFull() throws Exception {
        pool = new BoundedWorkerPool(1, 1, RejectPolicy.ABORT, QUIET);
        Blocker busy = new Blocker();
        pool.execute(busy);
        busy.awaitStarted();
        pool.execute(NOOP); // fills the queue
        assertThrows(RejectedExecutionException.class, () -> pool.execute(NOOP));
        assertEquals(1, pool.rejectedCount());
        assertEquals(1, pool.queueSize());
        busy.release.countDown();
    }

    @Test
    void callerRunsExecutesOnSubmittingThreadBeforeReturning() throws Exception {
        pool = new BoundedWorkerPool(1, 1, RejectPolicy.CALLER_RUNS, QUIET);
        Blocker busy = new Blocker();
        pool.execute(busy);
        busy.awaitStarted();
        pool.execute(NOOP);
        AtomicInteger ranOn = new AtomicInteger();
        Thread[] who = new Thread[1];
        pool.execute(() -> {
            who[0] = Thread.currentThread();
            ranOn.incrementAndGet();
        });
        assertEquals(1, ranOn.get(), "ran synchronously inside execute()");
        assertSame(Thread.currentThread(), who[0]);
        assertEquals(0, pool.rejectedCount());
        busy.release.countDown();
    }

    @Test
    void callerRunsFailureGoesToHandlerNotToSubmitter() throws Exception {
        List<Throwable> seen = Collections.synchronizedList(new ArrayList<>());
        pool = new BoundedWorkerPool(1, 1, RejectPolicy.CALLER_RUNS, (t, e) -> seen.add(e));
        Blocker busy = new Blocker();
        pool.execute(busy);
        busy.awaitStarted();
        pool.execute(NOOP);
        pool.execute(() -> {
            throw new IllegalStateException("boom");
        });
        assertEquals(1, seen.size());
        assertEquals(1, pool.failedCount());
        busy.release.countDown();
    }

    @Test
    void discardOldestDropsLongestWaitingTaskAndKeepsNewest() throws Exception {
        pool = new BoundedWorkerPool(1, 2, RejectPolicy.DISCARD_OLDEST, QUIET);
        List<String> ran = Collections.synchronizedList(new ArrayList<>());
        Blocker busy = new Blocker();
        pool.execute(busy);
        busy.awaitStarted();
        pool.execute(record(ran, "B"));
        pool.execute(record(ran, "C"));
        pool.execute(record(ran, "D")); // queue full: B (oldest) is dropped
        assertEquals(1, pool.discardedCount());
        assertEquals(2, pool.queueSize());
        busy.release.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(List.of("C", "D"), ran);
    }

    @Test
    void queuedTasksRunInFifoOrderOnOneWorker() throws Exception {
        pool = new BoundedWorkerPool(1, 10, RejectPolicy.ABORT, QUIET);
        List<String> ran = Collections.synchronizedList(new ArrayList<>());
        Blocker busy = new Blocker();
        pool.execute(busy);
        busy.awaitStarted();
        for (String s : List.of("a", "b", "c", "d")) pool.execute(record(ran, s));
        busy.release.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(List.of("a", "b", "c", "d"), ran);
    }

    @Test
    void shutdownRejectsNewWorkButDrainsQueue() throws Exception {
        pool = new BoundedWorkerPool(1, 5, RejectPolicy.ABORT, QUIET);
        List<String> ran = Collections.synchronizedList(new ArrayList<>());
        Blocker busy = new Blocker();
        pool.execute(busy);
        busy.awaitStarted();
        pool.execute(record(ran, "B"));
        pool.execute(record(ran, "C"));
        pool.shutdown();
        assertTrue(pool.isShutdown());
        assertThrows(RejectedExecutionException.class, () -> pool.execute(record(ran, "late")));
        busy.release.countDown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(List.of("B", "C"), ran, "queued work finished, late work never ran");
    }

    @Test
    void idleWorkersExitOnShutdown() throws Exception {
        pool = new BoundedWorkerPool(4, 4, RejectPolicy.ABORT, QUIET);
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    }

    @Test
    void awaitTerminationTimesOutWhileATaskIsStillRunning() throws Exception {
        pool = new BoundedWorkerPool(1, 1, RejectPolicy.ABORT, QUIET);
        Blocker busy = new Blocker();
        pool.execute(busy);
        busy.awaitStarted();
        pool.shutdown();
        assertFalse(pool.awaitTermination(30, TimeUnit.MILLISECONDS));
        busy.release.countDown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    }

    @Test
    void shutdownNowReturnsPendingTasksAndInterruptsRunningOnes() throws Exception {
        pool = new BoundedWorkerPool(1, 5, RejectPolicy.ABORT, QUIET);
        List<String> ran = Collections.synchronizedList(new ArrayList<>());
        Blocker busy = new Blocker();
        pool.execute(busy);
        busy.awaitStarted();
        Runnable b = record(ran, "B");
        Runnable c = record(ran, "C");
        pool.execute(b);
        pool.execute(c);
        List<Runnable> pending = pool.shutdownNow();
        assertEquals(List.of(b, c), pending);
        assertTrue(busy.interrupted.await(10, TimeUnit.SECONDS), "running task was interrupted");
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertTrue(ran.isEmpty(), "pending tasks never ran");
    }

    @Test
    void uncaughtTaskExceptionReachesHandlerAndWorkerSurvives() throws Exception {
        List<Throwable> seen = Collections.synchronizedList(new ArrayList<>());
        List<Thread> threads = Collections.synchronizedList(new ArrayList<>());
        pool = new BoundedWorkerPool(1, 4, RejectPolicy.ABORT, (t, e) -> {
            threads.add(t);
            seen.add(e);
        });
        pool.execute(() -> {
            throw new IllegalStateException("boom");
        });
        CountDownLatch next = new CountDownLatch(1);
        pool.execute(next::countDown); // same single worker must still be alive to run this
        assertTrue(next.await(10, TimeUnit.SECONDS));
        assertEquals(1, seen.size());
        assertEquals("boom", seen.get(0).getMessage());
        assertTrue(threads.get(0).getName().startsWith("pool-worker-"));
        assertEquals(1, pool.failedCount());
    }

    @Test
    void errorsAreHandledToo() throws Exception {
        List<Throwable> seen = Collections.synchronizedList(new ArrayList<>());
        pool = new BoundedWorkerPool(1, 4, RejectPolicy.ABORT, (t, e) -> seen.add(e));
        pool.execute(() -> {
            throw new AssertionError("an Error, not an Exception");
        });
        CountDownLatch next = new CountDownLatch(1);
        pool.execute(next::countDown);
        assertTrue(next.await(10, TimeUnit.SECONDS));
        assertTrue(seen.get(0) instanceof AssertionError);
    }

    @Test
    void throwingHandlerDoesNotKillTheWorker() throws Exception {
        pool = new BoundedWorkerPool(1, 4, RejectPolicy.ABORT, (t, e) -> {
            throw new RuntimeException("handler is broken too");
        });
        pool.execute(() -> {
            throw new IllegalStateException("boom");
        });
        CountDownLatch next = new CountDownLatch(1);
        pool.execute(next::countDown);
        assertTrue(next.await(10, TimeUnit.SECONDS));
    }

    @Test
    void validatesArguments() {
        assertThrows(IllegalArgumentException.class, () -> new BoundedWorkerPool(0, 1, RejectPolicy.ABORT, QUIET));
        assertThrows(IllegalArgumentException.class, () -> new BoundedWorkerPool(1, 0, RejectPolicy.ABORT, QUIET));
        assertThrows(NullPointerException.class, () -> new BoundedWorkerPool(1, 1, null, QUIET));
        pool = new BoundedWorkerPool(1, 1, RejectPolicy.ABORT, QUIET);
        assertThrows(NullPointerException.class, () -> pool.execute(null));
    }

    @Test
    void tasksRunOnPoolThreadsNotTheSubmitter() throws Exception {
        pool = new BoundedWorkerPool(2, 4, RejectPolicy.ABORT, QUIET);
        Thread[] who = new Thread[1];
        CountDownLatch done = new CountDownLatch(1);
        pool.execute(() -> {
            who[0] = Thread.currentThread();
            done.countDown();
        });
        assertTrue(done.await(10, TimeUnit.SECONDS));
        assertNotSame(Thread.currentThread(), who[0]);
    }
}
