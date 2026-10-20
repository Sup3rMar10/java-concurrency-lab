package com.github.sup3rmar10.concurrencylab.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/** No Thread.sleep anywhere: time only moves when the test says so. */
class AsyncTest {

    private final FakeTimers clock = new FakeTimers();
    private final Async async = new Async(clock);

    private static Throwable failureOf(CompletableFuture<?> f) {
        ExecutionException e = assertThrows(ExecutionException.class, f::get);
        return e.getCause();
    }

    // ---- withTimeout ----

    @Test
    void timeoutFiresExactlyAtTheDeadline() {
        CompletableFuture<String> slow = new CompletableFuture<>();
        CompletableFuture<String> guarded = async.withTimeout(slow, 100);
        clock.advance(99);
        assertFalse(guarded.isDone());
        clock.advance(1);
        assertInstanceOf(TimeoutException.class, failureOf(guarded));
    }

    @Test
    void timeoutCancelsTheSlowSource() {
        CompletableFuture<String> slow = new CompletableFuture<>();
        async.withTimeout(slow, 100);
        clock.advance(100);
        assertTrue(slow.isCancelled());
    }

    @Test
    void completingBeforeTheDeadlineDisarmsTheTimer() throws Exception {
        CompletableFuture<String> source = new CompletableFuture<>();
        CompletableFuture<String> guarded = async.withTimeout(source, 100);
        clock.advance(50);
        source.complete("ok");
        assertEquals("ok", guarded.get());
        assertEquals(0, clock.pending(), "timer cancelled, nothing leaks");
        clock.advance(1_000);
        assertEquals("ok", guarded.get());
    }

    @Test
    void sourceFailureIsPassedThroughUnwrapped() {
        CompletableFuture<String> source = new CompletableFuture<>();
        CompletableFuture<String> guarded = async.withTimeout(source, 100);
        source.completeExceptionally(new IllegalStateException("upstream broke"));
        assertEquals("upstream broke", failureOf(guarded).getMessage());
        assertEquals(0, clock.pending());
    }

    @Test
    void cancellingTheGuardedFutureCancelsSourceAndTimer() {
        CompletableFuture<String> source = new CompletableFuture<>();
        CompletableFuture<String> guarded = async.withTimeout(source, 100);
        guarded.cancel(true);
        assertTrue(source.isCancelled());
        assertEquals(0, clock.pending());
    }

    @Test
    void timeoutMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> async.withTimeout(new CompletableFuture<>(), 0));
    }

    // ---- retry ----

    /** Attempt factory: fails the first {@code failures} times, records the fake time of every attempt. */
    private static final class Flaky implements Supplier<CompletableFuture<String>> {
        final List<Long> attemptTimes = new ArrayList<>();
        final FakeTimers clock;
        final int failures;

        Flaky(FakeTimers clock, int failures) {
            this.clock = clock;
            this.failures = failures;
        }

        @Override
        public CompletableFuture<String> get() {
            attemptTimes.add(clock.now());
            if (attemptTimes.size() <= failures) {
                return CompletableFuture.failedFuture(new IllegalStateException("fail #" + attemptTimes.size()));
            }
            return CompletableFuture.completedFuture("ok");
        }
    }

    @Test
    void retrySucceedsOnThirdAttemptWithDoublingBackoff() throws Exception {
        Flaky op = new Flaky(clock, 2);
        CompletableFuture<String> result = async.retry(op, 5, 100);
        assertFalse(result.isDone());
        clock.advance(100);
        assertFalse(result.isDone());
        clock.advance(200);
        assertEquals("ok", result.get());
        assertEquals(List.of(0L, 100L, 300L), op.attemptTimes, "backoff 100 then 200");
    }

    @Test
    void retryDoesNotWaitBeforeTheFirstAttemptOrAfterSuccess() throws Exception {
        Flaky op = new Flaky(clock, 0);
        assertEquals("ok", async.retry(op, 3, 100).get());
        assertEquals(1, op.attemptTimes.size());
        assertEquals(0, clock.pending());
    }

    @Test
    void retryGivesUpAfterMaxAttemptsWithTheLastError() {
        Flaky op = new Flaky(clock, 99);
        CompletableFuture<String> result = async.retry(op, 3, 10);
        clock.advance(10 + 20);
        assertEquals("fail #3", failureOf(result).getMessage());
        assertEquals(3, op.attemptTimes.size());
        assertEquals(0, clock.pending());
    }

    @Test
    void retryTreatsASupplierThatThrowsAsAFailedAttempt() throws Exception {
        int[] calls = {0};
        CompletableFuture<String> result = async.retry(() -> {
            if (calls[0]++ == 0) throw new IllegalArgumentException("sync failure");
            return CompletableFuture.completedFuture("recovered");
        }, 2, 5);
        clock.advance(5);
        assertEquals("recovered", result.get());
    }

    @Test
    void cancelDuringBackoffCancelsTheTimerAndStopsRetrying() {
        Flaky op = new Flaky(clock, 99);
        CompletableFuture<String> result = async.retry(op, 5, 100);
        assertEquals(1, clock.pending(), "waiting out the backoff");
        result.cancel(true);
        assertEquals(0, clock.pending());
        clock.advance(10_000);
        assertEquals(1, op.attemptTimes.size(), "no further attempts after cancel");
    }

    @Test
    void cancelWhileAttemptInFlightCancelsThatAttemptAndDoesNotRetry() {
        List<CompletableFuture<String>> attempts = new ArrayList<>();
        CompletableFuture<String> result = async.retry(() -> {
            CompletableFuture<String> f = new CompletableFuture<>();
            attempts.add(f);
            return f;
        }, 5, 100);
        result.cancel(true);
        assertTrue(attempts.get(0).isCancelled());
        clock.advance(10_000);
        assertEquals(1, attempts.size());
    }

    @Test
    void anAttemptThatIsCancelledFromOutsideIsNotRetried() {
        List<CompletableFuture<String>> attempts = new ArrayList<>();
        CompletableFuture<String> result = async.retry(() -> {
            CompletableFuture<String> f = new CompletableFuture<>();
            attempts.add(f);
            return f;
        }, 5, 100);
        attempts.get(0).cancel(true);
        assertInstanceOf(CancellationException.class, assertThrows(CancellationException.class, result::get));
        assertEquals(0, clock.pending());
    }

    @Test
    void timeoutPlusRetryRetriesAHungAttempt() throws Exception {
        List<CompletableFuture<String>> attempts = new ArrayList<>();
        CompletableFuture<String> result = async.retry(() -> {
            CompletableFuture<String> f = new CompletableFuture<>();
            attempts.add(f);
            if (attempts.size() == 2) f.complete("second time lucky");
            return async.withTimeout(f, 50);
        }, 3, 10);
        clock.advance(50);   // attempt 1 hangs and times out
        assertTrue(attempts.get(0).isCancelled(), "hung attempt was cancelled");
        assertFalse(result.isDone());
        clock.advance(10);   // backoff elapses, attempt 2 completes immediately
        assertEquals("second time lucky", result.get());
        assertEquals(2, attempts.size());
    }

    @Test
    void retryRejectsZeroAttempts() {
        assertThrows(IllegalArgumentException.class, () -> async.retry(() -> null, 0, 1));
    }

    // ---- fanOut ----

    private static List<Supplier<CompletableFuture<Integer>>> suppliersOf(List<CompletableFuture<Integer>> fs) {
        return fs.stream().<Supplier<CompletableFuture<Integer>>>map(f -> () -> f).toList();
    }

    @Test
    void fanOutCollectsResultsInInputOrderNotCompletionOrder() throws Exception {
        List<CompletableFuture<Integer>> fs = List.of(new CompletableFuture<>(), new CompletableFuture<>(), new CompletableFuture<>());
        CompletableFuture<List<Integer>> all = async.fanOut(suppliersOf(fs));
        fs.get(2).complete(30);
        fs.get(0).complete(10);
        assertFalse(all.isDone());
        fs.get(1).complete(20);
        assertEquals(List.of(10, 20, 30), all.get());
    }

    @Test
    void fanOutFailsFastAndCancelsTheOthers() {
        List<CompletableFuture<Integer>> fs = List.of(new CompletableFuture<>(), new CompletableFuture<>(), new CompletableFuture<>());
        CompletableFuture<List<Integer>> all = async.fanOut(suppliersOf(fs));
        fs.get(1).completeExceptionally(new IllegalStateException("shard 1 down"));
        assertEquals("shard 1 down", failureOf(all).getMessage());
        assertTrue(fs.get(0).isCancelled());
        assertTrue(fs.get(2).isCancelled());
    }

    @Test
    void cancellingFanOutCancelsEveryTask() {
        List<CompletableFuture<Integer>> fs = List.of(new CompletableFuture<>(), new CompletableFuture<>());
        CompletableFuture<List<Integer>> all = async.fanOut(suppliersOf(fs));
        all.cancel(true);
        assertTrue(fs.get(0).isCancelled() && fs.get(1).isCancelled());
    }

    @Test
    void fanOutOfNothingCompletesImmediately() throws Exception {
        assertEquals(List.of(), async.<Integer>fanOut(List.of()).get());
    }

    // ---- the assembled pipeline ----

    @Test
    void pipelineSumsAllIds() throws Exception {
        AsyncPipeline p = new AsyncPipeline(async, id -> CompletableFuture.completedFuture(id.length()), 100, 3, 10);
        assertEquals(1 + 2 + 3, p.totalFor(List.of("a", "bb", "ccc")).get());
    }

    @Test
    void pipelineRecoversFromOneSlowFetchViaTimeoutAndRetry() throws Exception {
        int[] calls = {0};
        AsyncPipeline p = new AsyncPipeline(async, id -> {
            if (id.equals("slow") && calls[0]++ == 0) return new CompletableFuture<>(); // hangs the first time
            return CompletableFuture.completedFuture(5);
        }, 100, 3, 10);
        CompletableFuture<Integer> total = p.totalFor(List.of("fast", "slow"));
        clock.advance(100);
        assertFalse(total.isDone(), "slow id is backing off");
        clock.advance(10);
        assertEquals(10, total.get());
    }

    @Test
    void pipelineFailsWhenAnIdExhaustsItsAttemptsAndStopsTheOthers() {
        List<CompletableFuture<Integer>> neverEnding = new ArrayList<>();
        AsyncPipeline p = new AsyncPipeline(async, id -> {
            CompletableFuture<Integer> f = new CompletableFuture<>();
            if (id.equals("good")) neverEnding.add(f);
            else f.completeExceptionally(new IllegalStateException("bad id"));
            return f;
        }, 100, 2, 10);
        CompletableFuture<Integer> total = p.totalFor(List.of("good", "bad"));
        clock.advance(10); // bad id's single backoff
        assertEquals("bad id", failureOf(total).getMessage());
        assertTrue(neverEnding.get(0).isCancelled(), "healthy sibling was cancelled by fail-fast");
        assertEquals(0, clock.pending());
    }

    @Test
    void cancellingThePipelineResultCancelsInFlightFetches() {
        List<CompletableFuture<Integer>> fetches = new ArrayList<>();
        AsyncPipeline p = new AsyncPipeline(async, id -> {
            CompletableFuture<Integer> f = new CompletableFuture<>();
            fetches.add(f);
            return f;
        }, 100, 3, 10);
        CompletableFuture<Integer> total = p.totalFor(List.of("x", "y"));
        total.cancel(true);
        assertTrue(fetches.stream().allMatch(CompletableFuture::isCancelled));
        assertEquals(0, clock.pending(), "no timers left armed");
    }
}
