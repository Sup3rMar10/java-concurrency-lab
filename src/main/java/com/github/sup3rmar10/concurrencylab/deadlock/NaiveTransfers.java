package com.github.sup3rmar10.concurrencylab.deadlock;

/**
 * The bug: locks in argument order. transfer(a,b) racing transfer(b,a) can deadlock.
 * Uses lockInterruptibly only so a demo/test can break the deadlock afterwards.
 * The hook runs after the first lock is held and lets a test force the fatal interleaving.
 */
public final class NaiveTransfers implements Transfers {
    private final Runnable afterFirstLock;

    public NaiveTransfers(Runnable afterFirstLock) {
        this.afterFirstLock = afterFirstLock;
    }

    @Override
    public boolean transfer(Account from, Account to, long amount) throws InterruptedException {
        from.lock().lockInterruptibly();
        try {
            afterFirstLock.run();
            to.lock().lockInterruptibly();
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
