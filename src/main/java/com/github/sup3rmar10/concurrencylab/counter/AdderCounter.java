package com.github.sup3rmar10.concurrencylab.counter;

import java.util.concurrent.atomic.LongAdder;

public final class AdderCounter implements Counter {
    private final LongAdder value = new LongAdder();

    @Override
    public void increment() {
        value.increment();
    }

    @Override
    public long get() {
        return value.sum();
    }
}
