package com.github.sup3rmar10.concurrencylab.buffer;

import java.util.concurrent.TimeUnit;

/** The slice of BlockingQueue behaviour the lab cares about, so tests can run one scenario against both. */
public interface Buffer<T> {
    /** Blocks while full. */
    void put(T item) throws InterruptedException;

    /** Blocks while empty. */
    T take() throws InterruptedException;

    /** False if still full after the timeout. */
    boolean offer(T item, long timeout, TimeUnit unit) throws InterruptedException;

    /** Null if still empty after the timeout. */
    T poll(long timeout, TimeUnit unit) throws InterruptedException;

    int size();

    int capacity();
}
