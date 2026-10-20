package com.github.sup3rmar10.concurrencylab.counter;

public final class SynchronizedCounter implements Counter {
    private long value;

    @Override
    public synchronized void increment() {
        value++;
    }

    @Override
    public synchronized long get() {
        return value;
    }
}
