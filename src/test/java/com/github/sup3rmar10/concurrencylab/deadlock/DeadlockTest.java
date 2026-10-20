package com.github.sup3rmar10.concurrencylab.deadlock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class DeadlockTest {

    private static void awaitBarrier(CyclicBarrier b) {
        try {
            b.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (BrokenBarrierException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Thread daemon(String name, Runnable r) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    private static boolean tryTransfer(Transfers t, Account from, Account to, long amount) {
        try {
            return t.transfer(from, to, amount);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static Set<String> awaitDeadlock() throws InterruptedException {
        long deadline = System.nanoTime() + 20_000_000_000L;
        Set<String> found = DeadlockDetector.deadlockedThreadNames();
        while (found.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(5); // polling for a state; the assertion is on the state, not on elapsed time
            found = DeadlockDetector.deadlockedThreadNames();
        }
        return found;
    }

    @Test
    void detectorReportsNothingWhenNothingIsDeadlocked() {
        assertTrue(DeadlockDetector.deadlockedThreadNames().isEmpty());
    }

    @Test
    void naiveLockOrderDeadlocksAndThreadMxBeanSeesIt() throws Exception {
        Account a = new Account(1, 100);
        Account b = new Account(2, 100);
        CyclicBarrier bothHoldFirstLock = new CyclicBarrier(2);
        Transfers naive = new NaiveTransfers(() -> awaitBarrier(bothHoldFirstLock));
        Thread t1 = daemon("xfer-A", () -> tryTransfer(naive, a, b, 10));
        Thread t2 = daemon("xfer-B", () -> tryTransfer(naive, b, a, 10));
        t1.start();
        t2.start();
        try {
            Set<String> found = awaitDeadlock();
            assertEquals(Set.of("xfer-A", "xfer-B"), found);
        } finally {
            t1.interrupt();
            t2.interrupt();
            t1.join();
            t2.join();
        }
        assertTrue(DeadlockDetector.deadlockedThreadNames().isEmpty(), "interrupt broke the cycle");
        assertEquals(200, a.balance() + b.balance());
        assertEquals(100, a.balance(), "nothing moved: neither transfer completed");
    }

    @Test
    void orderedLocksSurviveOppositeDirectionHammering() throws Exception {
        Account a = new Account(1, 1_000_000); // large enough that no transfer is refused for lack of funds
        Account b = new Account(2, 1_000_000);
        long done = DeadlockDemo.hammer(new OrderedTransfers(Thread::yield), a, b, 10_000);
        assertEquals(20_000, done);
        assertEquals(2_000_000, a.balance() + b.balance(), "money conserved");
        assertEquals(1_000_000, a.balance(), "equal traffic both ways nets out");
        assertTrue(DeadlockDetector.deadlockedThreadNames().isEmpty());
    }

    @Test
    void orderedLocksTakeLowerIdFirstRegardlessOfArgumentOrder() throws Exception {
        Account low = new Account(1, 50);
        Account high = new Account(9, 50);
        boolean[] lowWasLockedWhenHighWas = new boolean[2];
        for (int i = 0; i < 2; i++) {
            int idx = i;
            Transfers t = new OrderedTransfers(() -> lowWasLockedWhenHighWas[idx] = low.lock().isHeldByCurrentThread());
            if (i == 0) t.transfer(low, high, 1); else t.transfer(high, low, 1);
        }
        assertTrue(lowWasLockedWhenHighWas[0] && lowWasLockedWhenHighWas[1], "low id is always the first lock");
    }

    @Test
    void timedLocksNeverDeadlockInTheForcedInterleaving() throws Exception {
        Account a = new Account(1, 100);
        Account b = new Account(2, 100);
        CyclicBarrier bothHoldFirstLock = new CyclicBarrier(2);
        Transfers timed = new TimedTransfers(100, () -> awaitBarrier(bothHoldFirstLock));
        boolean[] ok = new boolean[2];
        Thread t1 = daemon("timed-A", () -> ok[0] = tryTransfer(timed, a, b, 10));
        Thread t2 = daemon("timed-B", () -> ok[1] = tryTransfer(timed, b, a, 10));
        t1.start();
        t2.start();
        t1.join(20_000);
        t2.join(20_000);
        assertFalse(t1.isAlive() || t2.isAlive(), "both threads gave up or finished instead of hanging");
        assertFalse(ok[0] && ok[1], "both succeeding is impossible: each needs the lock the other holds");
        assertEquals(200, a.balance() + b.balance());
        assertTrue(DeadlockDetector.deadlockedThreadNames().isEmpty());
    }

    @Test
    void timedLockReturnsFalseWhenAnotherThreadHoldsTheLock() throws Exception {
        Account a = new Account(1, 100);
        Account b = new Account(2, 100);
        b.lock().lock(); // held by this (test) thread
        boolean[] result = {true};
        Thread t = daemon("timed-C", () -> result[0] = tryTransfer(new TimedTransfers(20, () -> { }), a, b, 10));
        t.start();
        t.join(20_000);
        b.lock().unlock();
        assertFalse(result[0]);
        assertEquals(100, a.balance());
        assertFalse(a.lock().isLocked(), "first lock released after giving up");
    }

    static Stream<Transfers> allVariants() {
        return Stream.of(new NaiveTransfers(() -> { }), new OrderedTransfers(() -> { }), new TimedTransfers(50, () -> { }));
    }

    @ParameterizedTest
    @MethodSource("allVariants")
    void uncontendedTransferMovesMoneyAndRejectsOverdraft(Transfers t) throws Exception {
        Account a = new Account(1, 100);
        Account b = new Account(2, 5);
        assertTrue(t.transfer(a, b, 60));
        assertEquals(40, a.balance());
        assertEquals(65, b.balance());
        assertFalse(t.transfer(a, b, 41), "overdraft refused");
        assertEquals(40, a.balance());
        assertFalse(a.lock().isLocked() || b.lock().isLocked(), "locks released");
    }

    @Test
    void transferToSameAccountIsHarmlessWithGlobalOrder() throws Exception {
        Account a = new Account(1, 100);
        assertTrue(new OrderedTransfers(() -> { }).transfer(a, a, 30));
        assertEquals(100, a.balance());
    }

    @Test
    void demoDetectsTheDeadlockAndPrintsBothFixes() throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        boolean detected = DeadlockDemo.run(new PrintStream(buf, true, StandardCharsets.UTF_8));
        String text = buf.toString(StandardCharsets.UTF_8);
        assertTrue(detected);
        assertTrue(text.contains("[xfer-A, xfer-B]"), text);
        assertTrue(text.contains("ordered locks"), text);
        assertTrue(text.contains("tryLock(timeout)"), text);
    }
}
