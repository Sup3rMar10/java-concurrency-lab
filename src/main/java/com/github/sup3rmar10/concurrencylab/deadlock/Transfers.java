package com.github.sup3rmar10.concurrencylab.deadlock;

public interface Transfers {
    /** Moves money; false if funds are insufficient or (timed variant) a lock could not be had in time. */
    boolean transfer(Account from, Account to, long amount) throws InterruptedException;
}
