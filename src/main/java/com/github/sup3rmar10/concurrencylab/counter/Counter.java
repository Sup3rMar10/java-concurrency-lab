package com.github.sup3rmar10.concurrencylab.counter;

/** A shared counter; implementations differ only in how (and whether) they stay correct. */
public interface Counter {
    void increment();

    long get();
}
