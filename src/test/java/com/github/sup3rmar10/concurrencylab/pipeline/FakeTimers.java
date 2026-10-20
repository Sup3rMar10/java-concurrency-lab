package com.github.sup3rmar10.concurrencylab.pipeline;

import java.util.Comparator;
import java.util.PriorityQueue;

/** Manual clock: nothing fires until the test calls advance(). Single-threaded by design. */
final class FakeTimers implements Timers {

    private static final class Task {
        final long due;
        final long seq;
        final Runnable action;

        Task(long due, long seq, Runnable action) {
            this.due = due;
            this.seq = seq;
            this.action = action;
        }
    }

    private final PriorityQueue<Task> queue =
            new PriorityQueue<>(Comparator.<Task>comparingLong(t -> t.due).thenComparingLong(t -> t.seq));
    private long now;
    private long seq;

    @Override
    public Handle schedule(Runnable task, long delayMillis) {
        Task t = new Task(now + delayMillis, seq++, task);
        queue.add(t);
        return () -> queue.remove(t);
    }

    /** Moves time forward, running every task that comes due, in due-time order. */
    void advance(long millis) {
        long target = now + millis;
        while (!queue.isEmpty() && queue.peek().due <= target) {
            Task t = queue.poll();
            now = t.due;
            t.action.run();
        }
        now = target;
    }

    long now() {
        return now;
    }

    int pending() {
        return queue.size();
    }
}
