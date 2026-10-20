package com.github.sup3rmar10.concurrencylab.deadlock;

/** Fix 1: a global lock order (lower account id first). Cycles in the wait-for graph become impossible. */
public final class OrderedTransfers implements Transfers {
    private final Runnable afterFirstLock;

    public OrderedTransfers(Runnable afterFirstLock) {
        this.afterFirstLock = afterFirstLock;
    }

    @Override
    public boolean transfer(Account from, Account to, long amount) throws InterruptedException {
        Account first = from.id() < to.id() ? from : to;
        Account second = first == from ? to : from;
        first.lock().lockInterruptibly();
        try {
            afterFirstLock.run();
            second.lock().lockInterruptibly();
            try {
                return Account.moveLocked(from, to, amount);
            } finally {
                second.lock().unlock();
            }
        } finally {
            first.lock().unlock();
        }
    }
}
