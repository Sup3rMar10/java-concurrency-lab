package com.github.sup3rmar10.concurrencylab.deadlock;

import java.util.concurrent.locks.ReentrantLock;

/** Synthetic account; balance is guarded by its own lock. */
public final class Account {
    private final long id;
    private final ReentrantLock lock = new ReentrantLock();
    private long balance; // guarded by lock

    public Account(long id, long balance) {
        this.id = id;
        this.balance = balance;
    }

    public long id() {
        return id;
    }

    public ReentrantLock lock() {
        return lock;
    }

    public long balance() {
        lock.lock();
        try {
            return balance;
        } finally {
            lock.unlock();
        }
    }

    /** Caller must hold both locks. */
    static boolean moveLocked(Account from, Account to, long amount) {
        if (from.balance < amount) return false;
        from.balance -= amount;
        to.balance += amount;
        return true;
    }
}
