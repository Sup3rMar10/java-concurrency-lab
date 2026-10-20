package com.github.sup3rmar10.concurrencylab.deadlock;

import java.util.concurrent.TimeUnit;

/** Fix 2: keep the bad order but never wait forever. tryLock with a timeout, give up, let the caller retry. */
public final class TimedTransfers implements Transfers {
    private final long timeoutMillis;
    private final Runnable afterFirstLock;

    public TimedTransfers(long timeoutMillis, Runnable afterFirstLock) {
        this.timeoutMillis = timeoutMillis;
        this.afterFirstLock = afterFirstLock;
    }

    @Override
    public boolean transfer(Account from, Account to, long amount) throws InterruptedException {
        if (!from.lock().tryLock(timeoutMillis, TimeUnit.MILLISECONDS)) return false;
        try {
            afterFirstLock.run();
            if (!to.lock().tryLock(timeoutMillis, TimeUnit.MILLISECONDS)) return false;
            try {
                return Account.moveLocked(from, to, amount);
            } finally {
                to.lock().unlock();
            }
        } finally {
            from.lock().unlock();
        }
    }
}
