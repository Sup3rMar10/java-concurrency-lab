package com.github.sup3rmar10.concurrencylab.deadlock;

import java.io.PrintStream;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/** Forces the lock-ordering deadlock, detects it with ThreadMXBean, then shows both fixes. */
public final class DeadlockDemo {

    /** Returns true if the naive version was detected as deadlocked. */
    public static boolean run(PrintStream out) throws InterruptedException {
        Account a = new Account(1, 100);
        Account b = new Account(2, 100);

        // 1. naive: both threads hold their first lock before asking for the second.
        CyclicBarrier bothHoldFirst = new CyclicBarrier(2);
        Transfers naive = new NaiveTransfers(() -> awaitQuietly(bothHoldFirst));
        Thread t1 = start("xfer-A", naive, a, b);
        Thread t2 = start("xfer-B", naive, b, a);
        Set<String> found = Set.of();
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (found.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(5); // polling for a state, not asserting on elapsed time
            found = DeadlockDetector.deadlockedThreadNames();
        }
        out.println("naive lock order : deadlocked threads = " + found);
        t1.interrupt();
        t2.interrupt();
        t1.join();
        t2.join();
        out.println("after interrupt  : deadlocked threads = " + DeadlockDetector.deadlockedThreadNames()
                + ", balances " + a.balance() + "/" + b.balance());

        // 2. global lock order, same opposite-direction traffic, many iterations.
        Account c = new Account(3, 1_000_000); // big balances: no transfer is refused for lack of funds
        Account d = new Account(4, 1_000_000);
        Transfers ordered = new OrderedTransfers(Thread::yield);
        long moved = hammer(ordered, c, d, 20_000);
        out.println("ordered locks    : " + moved + " of 40000 transfers done, total balance " + (c.balance() + d.balance())
                + ", deadlocked = " + DeadlockDetector.deadlockedThreadNames());

        // 3. tryLock with timeout, same forced interleaving as step 1.
        CyclicBarrier again = new CyclicBarrier(2);
        Transfers timed = new TimedTransfers(100, () -> awaitQuietly(again));
        boolean[] ok = new boolean[2];
        Thread u1 = new Thread(() -> ok[0] = attempt(timed, a, b), "timed-A");
        Thread u2 = new Thread(() -> ok[1] = attempt(timed, b, a), "timed-B");
        u1.start();
        u2.start();
        u1.join();
        u2.join();
        out.println("tryLock(timeout) : results A=" + ok[0] + " B=" + ok[1] + " (both true is impossible), total balance "
                + (a.balance() + b.balance()) + ", deadlocked = " + DeadlockDetector.deadlockedThreadNames());
        return !found.isEmpty();
    }

    /** Two threads, opposite directions, n transfers each. Returns how many succeeded. */
    static long hammer(Transfers t, Account a, Account b, int n) throws InterruptedException {
        long[] done = new long[2];
        Thread x = new Thread(() -> done[0] = loop(t, a, b, n), "hammer-A");
        Thread y = new Thread(() -> done[1] = loop(t, b, a, n), "hammer-B");
        x.setDaemon(true);
        y.setDaemon(true);
        x.start();
        y.start();
        x.join();
        y.join();
        return done[0] + done[1];
    }

    private static long loop(Transfers t, Account from, Account to, int n) {
        long ok = 0;
        try {
            for (int i = 0; i < n; i++) if (t.transfer(from, to, 1)) ok++;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return ok;
    }

    private static boolean attempt(Transfers t, Account from, Account to) {
        try {
            return t.transfer(from, to, 10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static Thread start(String name, Transfers t, Account from, Account to) {
        Thread th = new Thread(() -> attempt(t, from, to), name);
        th.setDaemon(true);
        th.start();
        return th;
    }

    private static void awaitQuietly(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (BrokenBarrierException e) {
            throw new IllegalStateException(e);
        }
    }

    private DeadlockDemo() { }
}
