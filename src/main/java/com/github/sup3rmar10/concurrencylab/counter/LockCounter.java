package com.github.sup3rmar10.concurrencylab.counter;

import java.util.concurrent.locks.ReentrantLock;

public final class LockCounter implements Counter {
    private final ReentrantLock lock = new ReentrantLock();
    private long value;

    @Override
    public void increment() {
        lock.lock();
        try {
            value++;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long get() {
        lock.lock();
        try {
            return value;
        } finally {
            lock.unlock();
        }
    }
}
