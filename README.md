# java-concurrency-lab

Five classic concurrency primitives rebuilt from `java.util.concurrent` basics in Java 17, with tests that
force the interleaving they care about (latches, barriers, a hook, a fake clock) instead of sleeping and hoping.
It exists to show how these pieces work and how to test them, not to replace `ThreadPoolExecutor`, `LinkedBlockingQueue`
or a real resilience library. The only dependency is JUnit 5 (test scope). All data is synthetic.

What it does:

- **`BoundedWorkerPool`**: N worker threads, a bounded queue (one `ReentrantLock`, one `notEmpty` condition), three
  reject policies (`ABORT`, `CALLER_RUNS`, `DISCARD_OLDEST`), `shutdown()` (drain) versus `shutdownNow()` (drop the queue,
  interrupt workers, return the tasks that never started), and an injected handler for task failures. A task that throws
  does not kill its worker. It is an `Executor`, not an `ExecutorService`.
- **Counters**: one `Counter` interface with a deliberately racy counter (plain `long`), and `synchronized`,
  `ReentrantLock`, `AtomicLong` and `LongAdder` versions. The racy one has a hook between its read and its write so a test can
  force the lost update with two latches.
- **Deadlock demo**: two `Account`s with a `ReentrantLock` each; a naive transfer (locks in argument order) that
  deadlocks on purpose, a `ThreadMXBean`-based detector, and two fixes: global lock order (lower id first) and
  `tryLock` with a timeout that reports failure to the caller.
- **Async pipeline**: `Async.withTimeout`, `retry` with exponential backoff, and `fanOut` (fail-fast, results in input
  order) over `CompletableFuture`, plus a small `AsyncPipeline` that composes them. The clock is a `Timers` seam
  (`RealTimers` in production, a manual `FakeTimers` in tests), and every combinator propagates cancellation upstream.
- **`BoundedBuffer`**: a ring buffer with one lock and separate `notFull` / `notEmpty` conditions, blocking and timed
  `put`/`take`/`offer`/`poll`, interruptible.

## How to run

Requirements: JDK 17 or newer, Maven 3.9. Python 3 is only needed for the mutation script.

```bash
mvn -q -B verify                                               # compile and run all 71 tests

java -cp target/classes com.github.sup3rmar10.concurrencylab.Demo   # counters table, deadlock demo, reject policies
mvn -q exec:java                                               # same demo via Maven

mvn -q -B test -Dtest=AsyncTest                                # one area (also BoundedBufferTest, DeadlockTest, ...)
```

Clone URL: `https://github.com/Sup3rMar10/java-concurrency-lab.git`

## Demo output (real run)

One run on an 8-CPU Windows laptop, OpenJDK 17.0.20.

```
== 1. counters: lost updates and throughput ==
Laptop microbenchmark, not JMH. cpus=8 threads=8 increments/thread=2,000,000 runs=3 (+1 warmup)
counter                 median ms       M incr/s max lost updates
racy (plain long)             1.5        10840.8       12,853,833
synchronized                357.4           44.8                0
ReentrantLock              1642.0            9.7                0
AtomicLong                 1033.3           15.5                0
LongAdder                   182.9           87.5                0

== 2. deadlock: detect it, then the two fixes ==
naive lock order : deadlocked threads = [xfer-A, xfer-B]
after interrupt  : deadlocked threads = [], balances 100/100
ordered locks    : 40000 of 40000 transfers done, total balance 2000000, deadlocked = []
tryLock(timeout) : results A=true B=false (both true is impossible), total balance 200, deadlocked = []

== 3. worker pool: 1 worker busy, queue capacity 2, 5 more tasks submitted ==
ABORT           ran=[t0, t1, t2] exceptions=3 discarded=0
CALLER_RUNS     ran=[t3(caller), t4(caller), t5(caller), t0, t1, t2] exceptions=0 discarded=0
DISCARD_OLDEST  ran=[t0, t4, t5] exceptions=0 discarded=3
```

How to read it, honestly:

- **The counter table is not a benchmark result.** It is a `System.nanoTime` loop, median of 3 runs after one warm-up,
  with no JMH, no forking and no CPU pinning. Across earlier runs of the same demo the numbers moved by about 2x
  (`LongAdder` between 115 and 252 ms, `AtomicLong` between 296 and 438 ms), so I would not publish a ranking from it.
  `ReentrantLock` being slowest here is what this run printed; I did not investigate why.
- **The racy row is a broken loop.** Its 1.5 ms is not "fast": the JIT can keep a plain field in a register across the loop.
  The interesting number is the 12.9 million lost updates (out of 16 million increments).
- In the `tryLock` line, `A=true B=false` means one transfer got both locks and the other timed out and gave up. That is
  the intended outcome of the timeout fix (the caller learns the transfer did not happen), not a bug.
- In the pool demo, `CALLER_RUNS` runs `t3` to `t5` on the submitting thread before the queued ones finish, which is
  why they appear first in the list.

## Design notes

- **Never run user code under the pool lock.** `CALLER_RUNS` executes the task after the lock is released; a task
  that re-enters the pool would otherwise block everyone or deadlock.
- **`while`, not `if`, around `await()`.** Spurious wakeups, and another thread can take the slot between the signal and
  the waiter re-acquiring the lock. `signal` (not `signalAll`) is enough in the buffer because each state change frees
  exactly one slot or one item and `notFull` / `notEmpty` are separate conditions.
- **Deadlocks are made certain, not probable.** A hook after the first lock plus a `CyclicBarrier` guarantees both
  threads hold one lock before asking for the other. The detector uses `findDeadlockedThreads`, which sees
  `java.util.concurrent` locks; `findMonitorDeadlockedThreads` would only see `synchronized`. All lock calls are
  interruptible so a test can break the deadlock afterwards and leave the JVM clean.
- **A fake clock instead of `orTimeout`.** `CompletableFuture.orTimeout` uses a private static delayer you cannot
  fake. `Async.withTimeout` has the same semantics (`TimeoutException`) on an injectable clock, so a test can assert
  "not done at 99 ms, done at 100 ms". It also cancels the slow source, which `orTimeout` does not do for you.
- **Cancelling a dependent `CompletableFuture` does not cancel its source.** Each combinator wires that explicitly
  (`withTimeout`, `retry` for both the in-flight attempt and the backoff timer, `fanOut` for its siblings), and each
  has a test and a mutation below.
- **Test discipline.** `src/test/resources/junit-platform.properties` sets a 30 s default timeout so a lost wakeup or
  deadlock fails a test instead of hanging the build. The buffer is checked two ways: a sequential differential test
  against `LinkedBlockingQueue` (300 seeds of random non-blocking operations), and a concurrent scenario (random
  producers, consumers and capacity; checks exactly-once delivery, per-producer order and capacity) run 150 times on the
  buffer and 150 times on a `LinkedBlockingQueue` adapter, which also validates the test itself.

## Tests

71 tests: `AsyncTest` 23, `BoundedWorkerPoolTest` 16, `CounterTest` 11, `DeadlockTest` 11, `BoundedBufferTest` 10
(counts from the surefire reports of the last `mvn -q -B verify`). The suite takes roughly 10 seconds.

## Mutation check

Passing tests prove little if they would also pass on broken code, so I applied 21 behaviour-breaking edits one at a
time (textual replacements run by `scripts/mutation_check.py`, which restores each file afterwards and verifies that
byte-for-byte), ran `mvn test`, and recorded whether any test failed. This is a hand-rolled check, not PIT, and the
mutations are the ones I thought of, so it says nothing about bugs I did not think of.

**Result: 21 of 21 killed, 0 survived.**

| # | Mutation | Result | First failing test(s) |
|---|---|---|---|
| 1 | pool: queue-full check `<` to `<=` (capacity off by one) | killed | `BoundedWorkerPoolTest.abortThrowsWhenQueueFull`, 2+ more |
| 2 | pool: `CALLER_RUNS` behaves like `ABORT` | killed | `BoundedWorkerPoolTest.callerRunsExecutesOnSubmittingThreadBeforeReturning`, 1 more |
| 3 | pool: `DISCARD_OLDEST` drops the newest queued task | killed | `BoundedWorkerPoolTest.discardOldestDropsLongestWaitingTaskAndKeepsNewest` |
| 4 | pool: no `notEmpty` signal on enqueue (lost wakeup) | killed | `BoundedWorkerPoolTest.throwingHandlerDoesNotKillTheWorker` (only this one) |
| 5 | pool: `shutdown()` no longer wakes idle workers | killed | `BoundedWorkerPoolTest.runsEveryAcceptedTask` (only this one) |
| 6 | pool: worker dies when a task throws | killed | `BoundedWorkerPoolTest.callerRunsFailureGoesToHandlerNotToSubmitter`, 2+ more (see notes) |
| 7 | pool: `shutdownNow()` does not interrupt workers | killed | `BoundedWorkerPoolTest.shutdownNowReturnsPendingTasksAndInterruptsRunningOnes` |
| 8 | counter: `SynchronizedCounter.increment` loses `synchronized` | killed | `CounterTest.safeCountersLoseNothingUnderContention` |
| 9 | counter: `AtomicCounter` does `get()` + `set()` instead of `incrementAndGet()` | killed | `CounterTest.safeCountersLoseNothingUnderContention` |
| 10 | counter: `LockCounter` increments without the lock | killed | `CounterTest.safeCountersLoseNothingUnderContention` |
| 11 | counter: `RacyCounter` re-reads the field when writing | killed | `CounterTest.racyCounterLosesAnUpdateWhenInterleavedBetweenReadAndWrite` |
| 12 | deadlock: `OrderedTransfers` locks in argument order | killed | `DeadlockTest.demoDetectsTheDeadlockAndPrintsBothFixes`, 2+ more |
| 13 | deadlock: `TimedTransfers` second lock waits forever | killed | `DeadlockTest.demoDetectsTheDeadlockAndPrintsBothFixes`, 2+ more |
| 14 | pipeline: retry backoff stops doubling | killed | `AsyncTest.retrySucceedsOnThirdAttemptWithDoublingBackoff` |
| 15 | pipeline: timeout no longer cancels the slow source | killed | `AsyncTest.timeoutCancelsTheSlowSource`, 1 more |
| 16 | pipeline: `fanOut` fail-fast no longer cancels siblings | killed | `AsyncTest.fanOutFailsFastAndCancelsTheOthers`, 1 more |
| 17 | pipeline: cancelling the `retry()` result no longer cancels in-flight work | killed | `AsyncTest.cancelDuringBackoffCancelsTheTimerAndStopsRetrying`, 2+ more |
| 18 | pipeline: retry gives up one attempt early | killed | `AsyncTest.retryGivesUpAfterMaxAttemptsWithTheLastError`, 1 more |
| 19 | buffer: enqueue drops the `notEmpty` signal (lost wakeup) | killed | `BoundedBufferTest.boundedBufferHoldsInvariantsOverManyRandomInterleavings`, 1 more |
| 20 | buffer: dequeue drops the `notFull` signal (lost wakeup) | killed | `BoundedBufferTest.boundedBufferHoldsInvariantsOverManyRandomInterleavings`, 2 more |
| 21 | buffer: `put()` waits with `if` instead of `while` | killed | `BoundedBufferTest.boundedBufferHoldsInvariantsOverManyRandomInterleavings` (only this one) |

Notes on the results (a clean 21 of 21 deserves some suspicion, so here are the caveats):

- **Hangs count as kills.** The script runs the tests with a 10 s per-test timeout. Lost-wakeup and deadlock mutants
  (4, 5, 12, 13, 19, 20) most likely fail by timing out rather than by a failed assertion. I did not check each one, so
  I cannot say which are timeouts. A timeout is still a failing test, but it is a slower and blunter signal.
- **Mutation 21 rests on a stress test.** The `if`-instead-of-`while` bug needs a particular schedule (a competing
  consumer takes the item between the signal and the wake-up), and the only test that caught it is the random-interleaving
  scenario. The seeds fix the shape of the workload, not the OS schedule, so this kill is probabilistic. It was killed in
  the run reported here; I have not measured how often it would be missed.
- **Mutations 8 to 10 (counter locks) also rest on a stress test** (4 threads x 250,000 increments). It fails with
  overwhelming probability when a lock is missing, not with certainty.
- **Mutation 6 once reported an unrelated test as failing.** In the first full run, `BoundedBufferTest.boundedBufferHoldsInvariantsOverManyRandomInterleavings`
  was listed as failing for a pool-only mutation. The buffer does not use the pool. Re-running mutation 6 alone listed only
  pool tests. My reading is that the buffer test (about 3.6 s normally) hit the 10 s timeout under load, which means a
  loaded machine can produce a spurious "killed". That does not change this result (mutation 6 is killed by
  pool tests either way), but it is the reason to treat timing-based kills with care.
- The mutation script touches `src/main` only and was run exactly once for the full table; source hashes before and after
  were identical, and the script asserts byte-for-byte restoration at the end.

Reproduce: `python scripts/mutation_check.py` (set `MVN` if `mvn` is not on PATH; `python scripts/mutation_check.py 3 7`
runs just mutations 3 and 7). Takes roughly 15 to 20 minutes. Run one instance only; two at once mutate the same files.

## Limits / not verified

- Microbenchmark numbers are indicative at best (see above). No JMH, no thread-scaling curve, no error bars.
- No jcstress and no memory-model claims beyond what the deterministic tests show. The random-interleaving buffer test
  depends on the OS scheduler and cannot prove the absence of bugs.
- Tests use real threads and a few short real timeouts (for example "`offer` returns false after 10 ms"). The assertions
  are one-sided so they should not flake, but those tests are not clock-free. Only the `Async` tests run on the fake clock.
- The pool has no `Future`s, `invokeAll`, dynamic sizing or metrics beyond four counters. `shutdownNow` only interrupts: a
  task that ignores interrupts keeps its worker alive.
- `tryLock` with a timeout turns a deadlock into a failure the caller must handle; it does not add jittered backoff, so
  two callers retrying in lockstep could livelock. `retry` has no jitter either.
- Only tested on JDK 17 locally (OpenJDK 17.0.20, Windows). The workflow also lists JDK 21.
- CI: the workflow (`.github/workflows/ci.yml`, JDK 17 and 21 matrix, `mvn -q -B verify` plus the demo) is included. CI ran green on the first run on GitHub Actions (Java 17 and 21, ubuntu-latest, 2026-10-20).

## Layout

```
src/main/java/com/github/sup3rmar10/concurrencylab/
    pool/       BoundedWorkerPool
    counter/    Counter, RacyCounter, SynchronizedCounter, LockCounter, AtomicCounter, AdderCounter, CounterDemo
    deadlock/   Account, Transfers, NaiveTransfers, OrderedTransfers, TimedTransfers, DeadlockDetector, DeadlockDemo
    pipeline/   Timers, RealTimers, Async, AsyncPipeline
    buffer/     Buffer, BoundedBuffer
    Demo        runs the three demos
src/test/java/...                tests per area, plus FakeTimers (the manual clock)
scripts/mutation_check.py        the mutation check described above
STEPS.md                         how this was built, the bugs I hit, and how to run it yourself
```

MIT licensed.
