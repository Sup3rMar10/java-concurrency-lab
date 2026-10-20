package com.github.sup3rmar10.concurrencylab.counter;

/**
 * Deliberately broken: read-modify-write on a plain field.
 * The optional hook runs between the read and the write so a test can force the bad interleaving.
 */
public final class RacyCounter implements Counter {
    private long value;
    private final Runnable betweenReadAndWrite;

    public RacyCounter() {
        this(() -> { });
    }

    public RacyCounter(Runnable betweenReadAndWrite) {
        this.betweenReadAndWrite = betweenReadAndWrite;
    }

    @Override
    public void increment() {
        long v = value;
        betweenReadAndWrite.run();
        value = v + 1;
    }

    @Override
    public long get() {
        return value;
    }
}
