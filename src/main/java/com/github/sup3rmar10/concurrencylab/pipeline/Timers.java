package com.github.sup3rmar10.concurrencylab.pipeline;

/**
 * Injectable clock/scheduler. CompletableFuture.orTimeout uses a private static delayer that tests cannot
 * control, so the pipeline takes this seam instead: real in production, fake in tests.
 */
public interface Timers {

    interface Handle {
        void cancel();
    }

    Handle schedule(Runnable task, long delayMillis);
}
