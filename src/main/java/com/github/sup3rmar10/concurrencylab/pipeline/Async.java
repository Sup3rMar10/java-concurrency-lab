package com.github.sup3rmar10.concurrencylab.pipeline;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Small CompletableFuture toolkit: per-stage timeout, retry with exponential backoff, fan-out/fan-in.
 * Rule used everywhere: cancelling the future you were handed cancels what is behind it
 * (plain CompletableFuture does not propagate cancel upstream).
 */
public final class Async {
    private final Timers timers;

    public Async(Timers timers) {
        this.timers = Objects.requireNonNull(timers);
    }

    /** Like orTimeout, but on an injectable clock; also cancels the slow source on timeout. */
    public <T> CompletableFuture<T> withTimeout(CompletableFuture<T> source, long timeoutMs) {
        if (timeoutMs <= 0) throw new IllegalArgumentException("timeoutMs must be > 0");
        CompletableFuture<T> result = new CompletableFuture<>();
        Timers.Handle timer = timers.schedule(() -> {
            if (result.completeExceptionally(new TimeoutException("stage timed out after " + timeoutMs + " ms"))) {
                source.cancel(true);
            }
        }, timeoutMs);
        source.whenComplete((v, e) -> {
            timer.cancel();
            if (e != null) result.completeExceptionally(unwrap(e));
            else result.complete(v);
        });
        result.whenComplete((v, e) -> {
            if (result.isCancelled()) {
                timer.cancel();
                source.cancel(true);
            }
        });
        return result;
    }

    /**
     * Calls {@code attempt} up to {@code maxAttempts} times. Waits base, 2*base, 4*base ... between failures.
     * Cancelling the result cancels the in-flight attempt or the pending backoff timer, and stops retrying.
     */
    public <T> CompletableFuture<T> retry(Supplier<CompletableFuture<T>> attempt, int maxAttempts, long baseBackoffMs) {
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
        CompletableFuture<T> result = new CompletableFuture<>();
        AtomicReference<Runnable> cancelCurrent = new AtomicReference<>(() -> { });
        result.whenComplete((v, e) -> {
            if (result.isCancelled()) cancelCurrent.get().run();
        });
        runAttempt(attempt, 1, maxAttempts, baseBackoffMs, result, cancelCurrent);
        return result;
    }

    private <T> void runAttempt(Supplier<CompletableFuture<T>> attempt, int n, int max, long base,
                                CompletableFuture<T> result, AtomicReference<Runnable> cancelCurrent) {
        if (result.isDone()) return;
        CompletableFuture<T> f;
        try {
            f = attempt.get();
        } catch (Throwable t) {
            f = CompletableFuture.failedFuture(t);
        }
        CompletableFuture<T> inFlight = f;
        cancelCurrent.set(() -> inFlight.cancel(true));
        f.whenComplete((v, e) -> {
            if (e == null) {
                result.complete(v);
                return;
            }
            Throwable cause = unwrap(e);
            if (cause instanceof CancellationException || n >= max) {
                result.completeExceptionally(cause);
                return;
            }
            long delay = base * (1L << Math.min(n - 1, 20));
            Timers.Handle h = timers.schedule(() -> runAttempt(attempt, n + 1, max, base, result, cancelCurrent), delay);
            cancelCurrent.set(h::cancel);
        });
    }

    /**
     * Starts every task, completes with all results in input order. Fail-fast: the first failure fails the result
     * and cancels the tasks still running. Cancelling the result cancels all tasks.
     */
    public <T> CompletableFuture<List<T>> fanOut(List<Supplier<CompletableFuture<T>>> tasks) {
        List<CompletableFuture<T>> futures = tasks.stream().map(Supplier::get).toList();
        CompletableFuture<List<T>> result = new CompletableFuture<>();
        if (futures.isEmpty()) {
            result.complete(List.of());
            return result;
        }
        AtomicInteger remaining = new AtomicInteger(futures.size());
        for (CompletableFuture<T> f : futures) {
            f.whenComplete((v, e) -> {
                if (e != null) {
                    if (result.completeExceptionally(unwrap(e))) futures.forEach(o -> o.cancel(true));
                } else if (remaining.decrementAndGet() == 0) {
                    result.complete(futures.stream().map(CompletableFuture::join).toList());
                }
            });
        }
        result.whenComplete((v, e) -> {
            if (result.isCancelled()) futures.forEach(o -> o.cancel(true));
        });
        return result;
    }

    /** Makes cancelling {@code dependent} also cancel {@code source}. */
    public static <T> CompletableFuture<T> propagateCancel(CompletableFuture<T> dependent, CompletableFuture<?> source) {
        dependent.whenComplete((v, e) -> {
            if (dependent.isCancelled()) source.cancel(true);
        });
        return dependent;
    }

    static Throwable unwrap(Throwable t) {
        return (t instanceof CompletionException && t.getCause() != null) ? t.getCause() : t;
    }
}
