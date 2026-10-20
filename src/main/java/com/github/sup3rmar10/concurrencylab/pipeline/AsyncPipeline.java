package com.github.sup3rmar10.concurrencylab.pipeline;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * fan-out: one fetch per id, each with a per-attempt timeout and retry/backoff;
 * fan-in: sum of the results. Fails fast if any id exhausts its attempts.
 */
public final class AsyncPipeline {
    private final Async async;
    private final Function<String, CompletableFuture<Integer>> fetcher;
    private final long stageTimeoutMs;
    private final int attempts;
    private final long backoffMs;

    public AsyncPipeline(Async async, Function<String, CompletableFuture<Integer>> fetcher,
                         long stageTimeoutMs, int attempts, long backoffMs) {
        this.async = async;
        this.fetcher = fetcher;
        this.stageTimeoutMs = stageTimeoutMs;
        this.attempts = attempts;
        this.backoffMs = backoffMs;
    }

    public CompletableFuture<Integer> totalFor(List<String> ids) {
        List<Supplier<CompletableFuture<Integer>>> tasks = ids.stream()
                .<Supplier<CompletableFuture<Integer>>>map(id -> () -> async.retry(
                        () -> async.withTimeout(fetcher.apply(id), stageTimeoutMs), attempts, backoffMs))
                .toList();
        CompletableFuture<List<Integer>> fan = async.fanOut(tasks);
        CompletableFuture<Integer> total = fan.thenApply(l -> l.stream().mapToInt(Integer::intValue).sum());
        return Async.propagateCancel(total, fan);
    }
}
