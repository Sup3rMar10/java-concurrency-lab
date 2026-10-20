package com.github.sup3rmar10.concurrencylab.counter;

import java.util.concurrent.atomic.AtomicLong;

public final class AtomicCounter implements Counter {
    private final AtomicLong value = new AtomicLong();

    @Override
    public void increment() {
        value.incrementAndGet();
    }

    @Override
    public long get() {
        return value.get();
    }
}
