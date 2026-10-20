package com.github.sup3rmar10.concurrencylab.pipeline;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Wall-clock Timers on one daemon thread. */
public final class RealTimers implements Timers, AutoCloseable {
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "pipeline-timers");
        t.setDaemon(true);
        return t;
    });

    @Override
    public Handle schedule(Runnable task, long delayMillis) {
        ScheduledFuture<?> f = scheduler.schedule(task, delayMillis, TimeUnit.MILLISECONDS);
        return () -> f.cancel(false);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
