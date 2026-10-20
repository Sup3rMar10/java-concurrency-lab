package com.github.sup3rmar10.concurrencylab.pool;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Fixed number of worker threads draining one bounded FIFO queue.
 * When the queue is full the {@link RejectPolicy} decides what happens to the new task.
 */
public final class BoundedWorkerPool implements Executor {

    public enum RejectPolicy {
        /** Throw RejectedExecutionException to the submitter. */
        ABORT,
        /** Run the task on the submitting thread (natural back-pressure). */
        CALLER_RUNS,
        /** Drop the task that has waited longest in the queue and accept the new one. */
        DISCARD_OLDEST
    }

    private final int capacity;
    private final RejectPolicy policy;
    private final Thread.UncaughtExceptionHandler onTaskFailure;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition terminated = lock.newCondition();
    private final ArrayDeque<Runnable> queue = new ArrayDeque<>(); // guarded by lock
    private final List<Thread> workers = new ArrayList<>();
    private boolean shutdown;   // guarded by lock
    private boolean stopNow;    // guarded by lock
    private int liveWorkers;    // guarded by lock

    private final AtomicLong completed = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong discarded = new AtomicLong();

    public BoundedWorkerPool(int workerCount, int queueCapacity, RejectPolicy policy,
                             Thread.UncaughtExceptionHandler onTaskFailure) {
        if (workerCount < 1) throw new IllegalArgumentException("workerCount must be >= 1");
        if (queueCapacity < 1) throw new IllegalArgumentException("queueCapacity must be >= 1");
        this.capacity = queueCapacity;
        this.policy = Objects.requireNonNull(policy);
        this.onTaskFailure = Objects.requireNonNull(onTaskFailure);
        this.liveWorkers = workerCount;
        for (int i = 0; i < workerCount; i++) {
            Thread t = new Thread(this::workerLoop, "pool-worker-" + i);
            t.setDaemon(true);
            workers.add(t);
        }
        workers.forEach(Thread::start); // start last: fields above are safely published
    }

    @Override
    public void execute(Runnable task) {
        Objects.requireNonNull(task);
        lock.lock();
        try {
            if (shutdown) {
                rejected.incrementAndGet();
                throw new RejectedExecutionException("pool is shut down");
            }
            if (queue.size() < capacity) {
                queue.addLast(task);
                notEmpty.signal();
                return;
            }
            switch (policy) {
                case ABORT -> {
                    rejected.incrementAndGet();
                    throw new RejectedExecutionException("queue full (" + capacity + ")");
                }
                case DISCARD_OLDEST -> {
                    queue.pollFirst();
                    discarded.incrementAndGet();
                    queue.addLast(task);
                    notEmpty.signal();
                    return;
                }
                case CALLER_RUNS -> { /* run below, outside the lock */ }
            }
        } finally {
            lock.unlock();
        }
        runTask(task);
    }

    /** Stop accepting tasks; already queued tasks still run. Does not block. */
    public void shutdown() {
        lock.lock();
        try {
            shutdown = true;
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Stop accepting tasks, drop the queue, interrupt workers. Returns the tasks that never started. */
    public List<Runnable> shutdownNow() {
        List<Runnable> pending;
        lock.lock();
        try {
            shutdown = true;
            stopNow = true;
            pending = new ArrayList<>(queue);
            queue.clear();
            notEmpty.signalAll();
            workers.forEach(Thread::interrupt);
        } finally {
            lock.unlock();
        }
        return pending;
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lock();
        try {
            while (liveWorkers > 0) {
                if (nanos <= 0) return false;
                nanos = terminated.awaitNanos(nanos);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    public boolean isShutdown() {
        lock.lock();
        try {
            return shutdown;
        } finally {
            lock.unlock();
        }
    }

    public int queueSize() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    public long completedCount() { return completed.get(); }
    public long failedCount() { return failed.get(); }
    public long rejectedCount() { return rejected.get(); }
    public long discardedCount() { return discarded.get(); }

    private void workerLoop() {
        try {
            Runnable task;
            while ((task = nextTask()) != null) {
                runTask(task);
            }
        } finally {
            lock.lock();
            try {
                if (--liveWorkers == 0) terminated.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    /** Blocks for the next task; null means "exit". */
    private Runnable nextTask() {
        lock.lock();
        try {
            while (queue.isEmpty()) {
                if (shutdown) return null;
                try {
                    notEmpty.await();
                } catch (InterruptedException e) {
                    if (stopNow) return null; // otherwise a stray interrupt: ignore it
                }
            }
            if (stopNow) return null;
            return queue.pollFirst();
        } finally {
            lock.unlock();
        }
    }

    private void runTask(Runnable task) {
        try {
            task.run();
            completed.incrementAndGet();
        } catch (Throwable t) {
            failed.incrementAndGet();
            try {
                onTaskFailure.uncaughtException(Thread.currentThread(), t);
            } catch (Throwable ignored) {
                // a broken handler must not kill the worker
            }
        }
    }
}
